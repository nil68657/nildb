//! Replay of WAL records during recovery (PostgreSQL's redo routines).
//!
//! A block with a full-page image is restored from it. A block without one
//! is changed only if its LSN is below the record's end LSN; otherwise the
//! change already reached the page before the crash. Either way the page's
//! LSN becomes the record's end LSN, the value the original change set.

use nilengine_api::coding::{get_u16, get_u64};
use nilengine_api::{Error, Result};

use crate::btree;
use crate::bufmgr::{BufferPool, Mode};
use crate::heap::{self, PruneChanges, Tid};
use crate::page;
use crate::wal::{
    RM_ABORT, RM_BTREE_INSERT, RM_BTREE_NEWROOT, RM_BTREE_SPLIT, RM_BTREE_VACUUM, RM_CHECKPOINT,
    RM_COMMIT, RM_FPI_FOR_HINT, RM_HEAP_DELETE, RM_HEAP_HOT_UPDATE, RM_HEAP_INSERT, RM_HEAP_PRUNE,
    RM_HEAP_UPDATE, RM_HEAP_VACUUM, Record,
};
use crate::xact::{XID_ABORTED, XID_COMMITTED, clog_set};

fn short(what: &str) -> Error {
    Error::corruption(format!("short {what} record"))
}

/// Applies `f` to block `i` of `rec` unless an image covers it or the page
/// already has the change.
fn redo_block(
    pool: &BufferPool,
    rec: &Record,
    i: usize,
    f: impl FnOnce(&mut [u8]) -> Result<()>,
) -> Result<()> {
    let b = rec
        .blocks
        .get(i)
        .ok_or_else(|| Error::corruption(format!("record at {} lacks block {i}", rec.start)))?;
    pool.ensure_nblocks(b.tag.rel, b.tag.fork, b.tag.block + 1)?;
    match &b.image {
        Some(img) => {
            let mut l = pool.get(b.tag, Mode::Zero)?.lock();
            l.page_mut().copy_from_slice(&img[..]);
            page::set_lsn(l.page_mut(), rec.end);
            l.mark_dirty();
        }
        None => {
            let mut l = pool.get(b.tag, Mode::Read)?.lock();
            if page::lsn(l.page()) < rec.end {
                f(l.page_mut())?;
                page::set_lsn(l.page_mut(), rec.end);
                l.mark_dirty();
            }
        }
    }
    Ok(())
}

fn place_or_fail(p: &mut [u8], block: u32, t: &[u8], off: u16) -> Result<()> {
    if page::is_new(p) {
        heap::init_page(p);
    }
    heap::place(p, block, t, Some(off))
        .map(|_| ())
        .ok_or_else(|| Error::corruption(format!("heap tuple does not replay at offset {off}")))
}

pub fn apply(pool: &BufferPool, rec: &Record) -> Result<()> {
    let m = &rec.main;
    match rec.rmid {
        RM_CHECKPOINT => Ok(()),
        RM_FPI_FOR_HINT | RM_BTREE_SPLIT | RM_BTREE_NEWROOT => {
            for i in 0..rec.blocks.len() {
                redo_block(pool, rec, i, |_| Ok(()))?;
            }
            Ok(())
        }
        RM_COMMIT | RM_ABORT => {
            let status = if rec.rmid == RM_COMMIT {
                XID_COMMITTED
            } else {
                XID_ABORTED
            };
            redo_block(pool, rec, 0, |p| {
                if page::is_new(p) {
                    page::init_raw(p, page::KIND_CLOG);
                }
                clog_set(p, rec.xid, status);
                Ok(())
            })
        }
        RM_HEAP_INSERT => {
            if m.len() < 3 {
                return Err(short("heap insert"));
            }
            let off = get_u16(m, 0);
            let block = rec.blocks.first().map_or(0, |b| b.tag.block);
            redo_block(pool, rec, 0, |p| place_or_fail(p, block, &m[3..], off))
        }
        RM_HEAP_DELETE => {
            if m.len() < 10 {
                return Err(short("heap delete"));
            }
            let (off, xmax) = (get_u16(m, 0), get_u64(m, 2));
            let block = rec.blocks.first().map_or(0, |b| b.tag.block);
            redo_block(pool, rec, 0, |p| {
                heap::mark_deleted(p, block, off, xmax);
                Ok(())
            })
        }
        RM_HEAP_UPDATE | RM_HEAP_HOT_UPDATE => {
            if m.len() < 12 {
                return Err(short("heap update"));
            }
            let (old_off, new_off, xmax) = (get_u16(m, 0), get_u16(m, 2), get_u64(m, 4));
            let tuple = &m[12..];
            let hot = rec.rmid == RM_HEAP_HOT_UPDATE;
            let new_block = rec.blocks.first().map_or(0, |b| b.tag.block);
            let new = Tid {
                block: new_block,
                off: new_off,
            };
            if rec.blocks.len() == 1 {
                redo_block(pool, rec, 0, |p| {
                    place_or_fail(p, new_block, tuple, new_off)?;
                    heap::mark_updated(p, old_off, xmax, new, hot);
                    Ok(())
                })
            } else {
                redo_block(pool, rec, 0, |p| {
                    place_or_fail(p, new_block, tuple, new_off)
                })?;
                redo_block(pool, rec, 1, |p| {
                    heap::mark_updated(p, old_off, xmax, new, false);
                    Ok(())
                })
            }
        }
        RM_HEAP_PRUNE => {
            let c = PruneChanges::decode(m)?;
            redo_block(pool, rec, 0, |p| {
                heap::apply_prune(p, &c);
                Ok(())
            })
        }
        RM_HEAP_VACUUM => {
            if m.len() < 2 || m.len() != 2 + 2 * get_u16(m, 0) as usize {
                return Err(short("heap vacuum"));
            }
            let offs: Vec<u16> = (0..get_u16(m, 0) as usize)
                .map(|i| get_u16(m, 2 + 2 * i))
                .collect();
            redo_block(pool, rec, 0, |p| {
                heap::apply_vacuum(p, &offs);
                Ok(())
            })
        }
        RM_BTREE_INSERT => redo_block(pool, rec, 0, |p| btree::redo_insert(p, m)),
        RM_BTREE_VACUUM => redo_block(pool, rec, 0, |p| btree::redo_vacuum(p, m)),
        t => Err(Error::corruption(format!(
            "unknown WAL record type {t} at {}",
            rec.start
        ))),
    }
}
