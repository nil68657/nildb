//! The page layout every relation fork shares, after PostgreSQL's
//! (docs: storage-page-layout.html). A page is 8 KiB: a 32-byte header,
//! 4-byte line pointers growing forward from it, items packed backward from
//! the special space, and the special space at the end (B-tree sibling
//! links; empty on heap pages).
//!
//! Header: page LSN u64, checksum u32, flags u16, pd_lower u16, pd_upper
//! u16, pd_special u16, page kind u8, layout version u8, 2 bytes unused,
//! pd_prune_xid u64. PostgreSQL's header is 24 bytes; this one is 32
//! because pd_prune_xid holds a 64-bit xid. All integers are little-endian.
//!
//! A line pointer is PostgreSQL's ItemIdData: a u32 holding lp_off in bits
//! 0-14, lp_flags in bits 15-16 and lp_len in bits 17-31. Offsets number
//! line pointers from 1.

use nilengine_api::coding::{crc32_parts, get_u16, get_u32, get_u64, set_u16, set_u32, set_u64};

pub const BLCKSZ: usize = 8192;
pub const HEADER: usize = 32;
pub type PageBuf = [u8; BLCKSZ];

const OFF_LSN: usize = 0;
const OFF_CHECKSUM: usize = 8;
const OFF_FLAGS: usize = 12;
const OFF_LOWER: usize = 14;
const OFF_UPPER: usize = 16;
const OFF_SPECIAL: usize = 18;
const OFF_KIND: usize = 20;
const OFF_VERSION: usize = 21;
const OFF_PRUNE_XID: usize = 24;

pub const KIND_HEAP: u8 = 1;
pub const KIND_BTREE: u8 = 2;
pub const KIND_BTREE_META: u8 = 3;
pub const KIND_CLOG: u8 = 4;
pub const KIND_FSM: u8 = 5;
pub const KIND_VM: u8 = 6;
const LAYOUT_VERSION: u8 = 1;

/// Some line pointer is unused (a hint, as in PostgreSQL).
pub const PD_HAS_FREE_LINES: u16 = 0x1;
/// Every tuple on the page is visible to every snapshot (set with the
/// visibility-map bit).
pub const PD_ALL_VISIBLE: u16 = 0x4;

pub const LP_UNUSED: u8 = 0;
pub const LP_NORMAL: u8 = 1;
pub const LP_REDIRECT: u8 = 2;
pub const LP_DEAD: u8 = 3;
pub const ITEM_ID: usize = 4;

pub fn lsn(p: &[u8]) -> u64 {
    get_u64(p, OFF_LSN)
}

pub fn set_lsn(p: &mut [u8], v: u64) {
    set_u64(p, OFF_LSN, v)
}

pub fn flags(p: &[u8]) -> u16 {
    get_u16(p, OFF_FLAGS)
}

pub fn set_flags(p: &mut [u8], v: u16) {
    set_u16(p, OFF_FLAGS, v)
}

pub fn lower(p: &[u8]) -> usize {
    get_u16(p, OFF_LOWER) as usize
}

fn set_lower(p: &mut [u8], v: usize) {
    set_u16(p, OFF_LOWER, v as u16)
}

pub fn upper(p: &[u8]) -> usize {
    get_u16(p, OFF_UPPER) as usize
}

fn set_upper(p: &mut [u8], v: usize) {
    set_u16(p, OFF_UPPER, v as u16)
}

pub fn special(p: &[u8]) -> usize {
    let s = get_u16(p, OFF_SPECIAL) as usize;
    if s == 0 { BLCKSZ } else { s }
}

pub fn kind(p: &[u8]) -> u8 {
    p[OFF_KIND]
}

pub fn prune_xid(p: &[u8]) -> u64 {
    get_u64(p, OFF_PRUNE_XID)
}

pub fn set_prune_xid(p: &mut [u8], v: u64) {
    set_u64(p, OFF_PRUNE_XID, v)
}

/// A page that was never initialized (PostgreSQL's PageIsNew).
pub fn is_new(p: &[u8]) -> bool {
    get_u16(p, OFF_UPPER) == 0
}

/// Initializes a slotted page with `special_size` bytes of special space.
pub fn init(p: &mut [u8], kind: u8, special_size: usize) {
    p.fill(0);
    set_lower(p, HEADER);
    set_upper(p, BLCKSZ - special_size);
    // pd_special is stored as 0 when it equals the page size, which does
    // not fit in a u16.
    set_u16(p, OFF_SPECIAL, (BLCKSZ - special_size) as u16);
    p[OFF_KIND] = kind;
    p[OFF_VERSION] = LAYOUT_VERSION;
}

/// Initializes a page whose bytes after the header are one array (commit
/// log, free-space map, visibility map); it has no line pointers and no
/// hole for full-page images to skip.
pub fn init_raw(p: &mut [u8], kind: u8) {
    p.fill(0);
    set_lower(p, BLCKSZ);
    set_upper(p, BLCKSZ);
    p[OFF_KIND] = kind;
    p[OFF_VERSION] = LAYOUT_VERSION;
}

/// The unused gap between the line pointers and the items, which a
/// full-page image leaves out (PostgreSQL's "hole").
pub fn hole(p: &[u8]) -> (usize, usize) {
    let (lo, up) = (lower(p), upper(p));
    if (HEADER..=BLCKSZ).contains(&lo) && lo <= up && up <= BLCKSZ {
        (lo, up - lo)
    } else {
        (0, 0)
    }
}

/// CRC-32 of the page with the checksum field zeroed, seeded with the block
/// number so a page written at the wrong offset fails too (PostgreSQL mixes
/// the block number into its checksum for the same reason).
pub fn checksum(p: &[u8], block: u32) -> u32 {
    crc32_parts(&[
        &block.to_le_bytes(),
        &p[..OFF_CHECKSUM],
        &[0u8; 4],
        &p[OFF_CHECKSUM + 4..],
    ])
}

pub fn set_checksum(p: &mut [u8], block: u32) {
    let c = checksum(p, block);
    set_u32(p, OFF_CHECKSUM, c);
}

/// True for an intact page or an all-zero (never written) one.
pub fn verify(p: &[u8], block: u32) -> bool {
    if is_new(p) && p.iter().all(|&b| b == 0) {
        return true;
    }
    get_u32(p, OFF_CHECKSUM) == checksum(p, block)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ItemId {
    pub off: u16,
    pub flags: u8,
    pub len: u16,
}

fn lp_pos(off: u16) -> usize {
    HEADER + ITEM_ID * (off as usize - 1)
}

pub fn max_offset(p: &[u8]) -> u16 {
    (lower(p).saturating_sub(HEADER) / ITEM_ID) as u16
}

pub fn item_id(p: &[u8], off: u16) -> ItemId {
    let v = get_u32(p, lp_pos(off));
    ItemId {
        off: (v & 0x7fff) as u16,
        flags: ((v >> 15) & 3) as u8,
        len: (v >> 17) as u16,
    }
}

pub fn set_item_id(p: &mut [u8], off: u16, id: ItemId) {
    let v = (id.off as u32 & 0x7fff) | ((id.flags as u32 & 3) << 15) | ((id.len as u32) << 17);
    set_u32(p, lp_pos(off), v)
}

/// The bytes of a normal item. Corrupt line pointers yield an empty slice
/// rather than a panic.
pub fn item(p: &[u8], off: u16) -> &[u8] {
    let id = item_id(p, off);
    let (s, e) = (id.off as usize, id.off as usize + id.len as usize);
    if s < HEADER || e > BLCKSZ {
        return &[];
    }
    &p[s..e]
}

pub fn item_mut(p: &mut [u8], off: u16) -> &mut [u8] {
    let id = item_id(p, off);
    let (s, e) = (id.off as usize, id.off as usize + id.len as usize);
    if s < HEADER || e > BLCKSZ {
        return &mut [];
    }
    &mut p[s..e]
}

/// Adds `data` as a normal item and returns its offset (PostgreSQL's
/// PageAddItem for heap pages). `at` forces the offset, as redo does;
/// otherwise the first unused line pointer is reused, or a new one added.
/// Returns `None` when the item does not fit.
pub fn add_item(p: &mut [u8], data: &[u8], at: Option<u16>, max_items: usize) -> Option<u16> {
    let max = max_offset(p);
    let off = match at {
        Some(o) => {
            if o == 0 || o > max + 1 || (o <= max && item_id(p, o).flags != LP_UNUSED) {
                return None;
            }
            o
        }
        None => {
            let mut found = None;
            if flags(p) & PD_HAS_FREE_LINES != 0 {
                found = (1..=max).find(|&o| item_id(p, o).flags == LP_UNUSED);
                if found.is_none() {
                    set_flags(p, flags(p) & !PD_HAS_FREE_LINES);
                }
            }
            found.unwrap_or(max + 1)
        }
    };
    let new_lp = off > max;
    if new_lp && off as usize > max_items {
        return None;
    }
    let (lo, up) = (lower(p), upper(p));
    let need_lower = lo + if new_lp { ITEM_ID } else { 0 };
    if need_lower > up || up - need_lower < data.len() {
        return None;
    }
    let start = up - data.len();
    p[start..up].copy_from_slice(data);
    set_upper(p, start);
    if new_lp {
        set_lower(p, need_lower);
    }
    set_item_id(
        p,
        off,
        ItemId {
            off: start as u16,
            flags: LP_NORMAL,
            len: data.len() as u16,
        },
    );
    Some(off)
}

/// Free bytes for one more heap tuple, allowing for its line pointer
/// (PostgreSQL's PageGetHeapFreeSpace): zero when the page already has
/// `max_items` line pointers and none is unused.
pub fn heap_free_space(p: &[u8], max_items: usize) -> usize {
    let space = (upper(p).saturating_sub(lower(p))).saturating_sub(ITEM_ID);
    let max = max_offset(p);
    if max as usize >= max_items {
        if (1..=max).any(|o| item_id(p, o).flags == LP_UNUSED) {
            return space + ITEM_ID;
        }
        return 0;
    }
    space
}

/// Moves the normal items of a heap page together at the end of the page so
/// the free space is one gap (PostgreSQL's PageRepairFragmentation). Line
/// pointer numbers do not change.
pub fn repair_fragmentation(p: &mut [u8]) {
    let max = max_offset(p);
    let mut items: Vec<(u16, usize, usize)> = Vec::with_capacity(max as usize);
    let mut has_unused = false;
    for o in 1..=max {
        let id = item_id(p, o);
        match id.flags {
            LP_NORMAL if id.len > 0 => items.push((o, id.off as usize, id.len as usize)),
            LP_UNUSED => has_unused = true,
            _ => {}
        }
    }
    // With the items sorted by position, highest first, each move lands at
    // or above its old position, so copy_within never overwrites an item
    // that has not moved yet.
    items.sort_unstable_by_key(|it| std::cmp::Reverse(it.1));
    let mut up = special(p);
    for (o, start, len) in items {
        up -= len;
        if up != start {
            p.copy_within(start..start + len, up);
        }
        let mut id = item_id(p, o);
        id.off = up as u16;
        set_item_id(p, o, id);
    }
    let lo = lower(p);
    p[lo..up].fill(0);
    set_upper(p, up);
    let f = flags(p);
    set_flags(
        p,
        if has_unused {
            f | PD_HAS_FREE_LINES
        } else {
            f & !PD_HAS_FREE_LINES
        },
    );
}

/// Drops trailing unused line pointers (PostgreSQL's
/// PageTruncateLinePointerArray, run by VACUUM).
pub fn truncate_line_pointers(p: &mut [u8]) {
    let mut max = max_offset(p);
    while max > 0 && item_id(p, max).flags == LP_UNUSED {
        set_u32(p, lp_pos(max), 0);
        max -= 1;
    }
    set_lower(p, HEADER + ITEM_ID * max as usize);
}

/// Inserts `data` so that it becomes item `off` of an index page, shifting
/// the later line pointers up by one. Returns false when it does not fit.
pub fn insert_item_at(p: &mut [u8], data: &[u8], off: u16) -> bool {
    let max = max_offset(p);
    if off == 0 || off > max + 1 {
        return false;
    }
    let (lo, up) = (lower(p), upper(p));
    if lo + ITEM_ID > up || up - lo - ITEM_ID < data.len() {
        return false;
    }
    let from = lp_pos(off);
    p.copy_within(from..lo, from + ITEM_ID);
    set_lower(p, lo + ITEM_ID);
    let start = up - data.len();
    p[start..up].copy_from_slice(data);
    set_upper(p, start);
    set_item_id(
        p,
        off,
        ItemId {
            off: start as u16,
            flags: LP_NORMAL,
            len: data.len() as u16,
        },
    );
    true
}

/// Replaces every item of an index page with `items`, in order, keeping the
/// header and the special space. Returns false when they do not fit.
pub fn set_index_items(p: &mut [u8], items: &[&[u8]]) -> bool {
    let sp = special(p);
    p[HEADER..sp].fill(0);
    set_lower(p, HEADER);
    set_upper(p, sp);
    items
        .iter()
        .enumerate()
        .all(|(i, it)| insert_item_at(p, it, i as u16 + 1))
}

/// Deletes the items at `offs` from an index page and closes the gaps in
/// both the line pointer array and the item space.
pub fn delete_index_items(p: &mut [u8], offs: &[u16]) {
    let max = max_offset(p);
    let mut gone = vec![false; max as usize + 1];
    for &o in offs {
        if (1..=max).contains(&o) {
            gone[o as usize] = true;
        }
    }
    let keep: Vec<Vec<u8>> = (1..=max)
        .filter(|&o| !gone[o as usize])
        .map(|o| item(p, o).to_vec())
        .collect();
    let refs: Vec<&[u8]> = keep.iter().map(Vec::as_slice).collect();
    let ok = set_index_items(p, &refs);
    debug_assert!(ok, "the remaining items always fit");
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_pointers_round_trip() {
        let mut p = vec![0u8; BLCKSZ];
        init(&mut p, KIND_HEAP, 0);
        let a = add_item(&mut p, b"hello", None, 300).unwrap();
        let b = add_item(&mut p, b"world!", None, 300).unwrap();
        assert_eq!((a, b), (1, 2));
        assert_eq!(item(&p, 1), b"hello");
        assert_eq!(item(&p, 2), b"world!");
        let mut id = item_id(&p, 1);
        id.flags = LP_UNUSED;
        id.len = 0;
        set_item_id(&mut p, 1, id);
        repair_fragmentation(&mut p);
        assert_eq!(item(&p, 2), b"world!");
        assert_eq!(upper(&p), BLCKSZ - 6);
        assert_eq!(add_item(&mut p, b"again", None, 300), Some(1));
        set_checksum(&mut p, 7);
        assert!(verify(&p, 7));
        assert!(!verify(&p, 8));
        assert!(verify(&[0u8; BLCKSZ], 3));
    }

    #[test]
    fn index_items_shift_and_delete() {
        let mut p = vec![0u8; BLCKSZ];
        init(&mut p, KIND_BTREE, 16);
        assert!(insert_item_at(&mut p, b"b", 1));
        assert!(insert_item_at(&mut p, b"d", 2));
        assert!(insert_item_at(&mut p, b"a", 1));
        assert!(insert_item_at(&mut p, b"c", 3));
        let all: Vec<&[u8]> = (1..=max_offset(&p)).map(|o| item(&p, o)).collect();
        assert_eq!(all, vec![&b"a"[..], b"b", b"c", b"d"]);
        delete_index_items(&mut p, &[2, 4]);
        let all: Vec<&[u8]> = (1..=max_offset(&p)).map(|o| item(&p, o)).collect();
        assert_eq!(all, vec![&b"a"[..], b"c"]);
        assert_eq!(special(&p), BLCKSZ - 16);
    }
}
