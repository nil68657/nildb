//! Sorted string table files. Layout, after LevelDB's `doc/table_format.md`
//! without compression or a metaindex:
//!
//! ```text
//! [data block][crc32] ... [data block][crc32]
//! [filter block][crc32]
//! [index block][crc32]
//! [footer: 56 bytes]
//! ```
//!
//! The index block maps the last internal key of each data block to its
//! `(offset varint64, length varint64)`. The footer holds the filter and
//! index handles, the entry count (five u64 LE), the format version (u32),
//! a CRC-32 of the preceding 44 footer bytes and the magic number.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use nilengine_api::cache::LruCache;
use nilengine_api::coding::{crc32, get_varint64, put_varint64};
use nilengine_api::{Error, IoContext, Options, Result};

use crate::block::{Block, BlockBuilder, BlockIter};
use crate::filter;
use crate::iter::InternalIterator;
use crate::key::{KIND_DELETE, parse_trailer, user_key};

const FOOTER_LEN: usize = 56;
const FORMAT_VERSION: u32 = 1;
/// "NILSST" followed by format version 1.
const MAGIC: u64 = 0x4e49_4c53_5354_0001;

/// Block cache keyed by (file number, block offset).
pub type BlockCache = LruCache<(u64, u64), Block>;

/// What a finished table file holds; the manifest records it.
#[derive(Clone, Debug, Default)]
pub struct TableProps {
    pub size: u64,
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
    pub smallest_seq: u64,
    pub largest_seq: u64,
    pub entries: u64,
    pub deletions: u64,
}

pub struct TableBuilder {
    w: BufWriter<File>,
    path: PathBuf,
    offset: u64,
    data: BlockBuilder,
    index: BlockBuilder,
    block_size: usize,
    bits_per_key: usize,
    hashes: Vec<u64>,
    last_user: Vec<u8>,
    props: TableProps,
}

impl TableBuilder {
    pub fn create(path: &Path, opts: &Options) -> Result<TableBuilder> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .ctx(|| format!("create {}", path.display()))?;
        Ok(TableBuilder {
            w: BufWriter::with_capacity(256 << 10, file),
            path: path.to_path_buf(),
            offset: 0,
            data: BlockBuilder::new(opts.block_restart_interval),
            index: BlockBuilder::new(1),
            block_size: opts.block_size,
            bits_per_key: opts.bloom_bits_per_key,
            hashes: Vec::new(),
            last_user: Vec::new(),
            props: TableProps {
                smallest_seq: u64::MAX,
                ..TableProps::default()
            },
        })
    }

    /// Adds an entry; internal keys must arrive in increasing order.
    pub fn add(&mut self, ikey: &[u8], value: &[u8]) -> Result<()> {
        if self.props.entries == 0 {
            self.props.smallest = ikey.to_vec();
        }
        let (seq, kind) = parse_trailer(ikey);
        self.props.smallest_seq = self.props.smallest_seq.min(seq);
        self.props.largest_seq = self.props.largest_seq.max(seq);
        if kind == KIND_DELETE {
            self.props.deletions += 1;
        }
        let u = user_key(ikey);
        if self.props.entries == 0 || u != self.last_user.as_slice() {
            self.hashes.push(filter::key_hash(u));
            self.last_user.clear();
            self.last_user.extend_from_slice(u);
        }
        self.props.entries += 1;
        self.data.add(ikey, value);
        if self.data.size_estimate() >= self.block_size {
            self.flush_block()?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.data.is_empty() {
            return Ok(());
        }
        let last = self.data.last_key().to_vec();
        let contents = self.data.finish();
        let (off, len) = write_block(&mut self.w, &mut self.offset, contents, &self.path)?;
        self.data.reset();
        let mut handle = Vec::with_capacity(12);
        put_varint64(&mut handle, off);
        put_varint64(&mut handle, len);
        self.index.add(&last, &handle);
        self.props.largest = last;
        Ok(())
    }

    /// Bytes written so far plus the pending block.
    pub fn estimated_size(&self) -> u64 {
        self.offset + self.data.size_estimate() as u64
    }

    /// Writes the filter, index and footer and syncs the file.
    pub fn finish(mut self) -> Result<TableProps> {
        self.flush_block()?;
        let filt = filter::build(&self.hashes, self.bits_per_key);
        let (foff, flen) = write_block(&mut self.w, &mut self.offset, &filt, &self.path)?;
        let index = self.index.finish();
        let (ioff, ilen) = write_block(&mut self.w, &mut self.offset, index, &self.path)?;
        let mut footer = Vec::with_capacity(FOOTER_LEN);
        for v in [foff, flen, ioff, ilen, self.props.entries] {
            footer.extend_from_slice(&v.to_le_bytes());
        }
        footer.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        let crc = crc32(&footer);
        footer.extend_from_slice(&crc.to_le_bytes());
        footer.extend_from_slice(&MAGIC.to_le_bytes());
        self.w
            .write_all(&footer)
            .ctx(|| format!("write {}", self.path.display()))?;
        self.offset += FOOTER_LEN as u64;
        self.w
            .flush()
            .ctx(|| format!("write {}", self.path.display()))?;
        self.w
            .get_ref()
            .sync_data()
            .ctx(|| format!("sync {}", self.path.display()))?;
        self.props.size = self.offset;
        if self.props.entries == 0 {
            self.props.smallest_seq = 0;
        }
        Ok(self.props)
    }

    /// Deletes the partial file.
    pub fn abandon(self) {
        let path = self.path.clone();
        drop(self);
        let _ = std::fs::remove_file(path);
    }
}

fn write_block(
    w: &mut BufWriter<File>,
    offset: &mut u64,
    contents: &[u8],
    path: &Path,
) -> Result<(u64, u64)> {
    let off = *offset;
    w.write_all(contents)
        .and_then(|_| w.write_all(&crc32(contents).to_le_bytes()))
        .ctx(|| format!("write {}", path.display()))?;
    *offset += contents.len() as u64 + 4;
    Ok((off, contents.len() as u64))
}

fn decode_handle(v: &[u8]) -> Result<(u64, u64)> {
    let (off, n) = get_varint64(v)?;
    let (len, _) = get_varint64(&v[n..])?;
    Ok((off, len))
}

pub struct Table {
    file: File,
    path: PathBuf,
    number: u64,
    size: u64,
    index: Arc<Block>,
    filter: Vec<u8>,
    cache: Arc<BlockCache>,
}

impl Table {
    pub fn open(path: &Path, number: u64, size: u64, cache: Arc<BlockCache>) -> Result<Table> {
        let file = File::open(path).ctx(|| format!("open {}", path.display()))?;
        if size < FOOTER_LEN as u64 {
            return Err(Error::corruption(format!(
                "{}: shorter than a footer",
                path.display()
            )));
        }
        let mut footer = [0u8; FOOTER_LEN];
        file.read_exact_at(&mut footer, size - FOOTER_LEN as u64)
            .ctx(|| format!("read footer of {}", path.display()))?;
        let u64_at = |i: usize| u64::from_le_bytes(footer[i..i + 8].try_into().unwrap());
        if u64_at(48) != MAGIC {
            return Err(Error::corruption(format!(
                "{}: bad table magic",
                path.display()
            )));
        }
        let crc = u32::from_le_bytes(footer[44..48].try_into().unwrap());
        if crc32(&footer[..44]) != crc {
            return Err(Error::corruption(format!(
                "{}: footer checksum mismatch",
                path.display()
            )));
        }
        let version = u32::from_le_bytes(footer[40..44].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err(Error::corruption(format!(
                "{}: table format version {version}",
                path.display()
            )));
        }
        let (foff, flen, ioff, ilen) = (u64_at(0), u64_at(8), u64_at(16), u64_at(24));
        let body = size - FOOTER_LEN as u64;
        if foff.checked_add(flen + 4).is_none_or(|e| e > body)
            || ioff.checked_add(ilen + 4).is_none_or(|e| e > body)
        {
            return Err(Error::corruption(format!(
                "{}: block handle past the end",
                path.display()
            )));
        }
        let index = Arc::new(Block::new(read_raw_block(&file, ioff, ilen, path)?)?);
        let filter = read_raw_block(&file, foff, flen, path)?;
        Ok(Table {
            file,
            path: path.to_path_buf(),
            number,
            size,
            index,
            filter,
            cache,
        })
    }

    fn read_block(&self, off: u64, len: u64, fill: bool) -> Result<Arc<Block>> {
        if let Some(b) = self.cache.get(&(self.number, off)) {
            return Ok(b);
        }
        if off.checked_add(len + 4).is_none_or(|e| e > self.size) {
            return Err(Error::corruption(format!(
                "{}: data block handle past the end",
                self.path.display()
            )));
        }
        let b = Arc::new(Block::new(read_raw_block(
            &self.file, off, len, &self.path,
        )?)?);
        if fill {
            self.cache
                .insert((self.number, off), b.clone(), b.size() + 64);
        }
        Ok(b)
    }

    /// Newest entry of `user` with sequence <= the one in `target`:
    /// `(sequence, kind, value)`.
    pub fn get(&self, target: &[u8], user: &[u8], hash: u64) -> Result<Option<(u64, u8, Vec<u8>)>> {
        if !filter::may_contain(&self.filter, hash) {
            return Ok(None);
        }
        let mut idx = self.index.clone().iter();
        idx.seek(target);
        idx.status()?;
        if !idx.valid() {
            return Ok(None);
        }
        let (off, len) = decode_handle(idx.value())?;
        let block = self.read_block(off, len, true)?;
        let mut it = block.iter();
        it.seek(target);
        it.status()?;
        if it.valid() && user_key(it.key()) == user {
            let (seq, kind) = parse_trailer(it.key());
            return Ok(Some((seq, kind, it.value().to_vec())));
        }
        Ok(None)
    }
}

fn read_raw_block(file: &File, off: u64, len: u64, path: &Path) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len as usize + 4];
    file.read_exact_at(&mut buf, off)
        .ctx(|| format!("read block at {off} of {}", path.display()))?;
    let crc = u32::from_le_bytes(buf[len as usize..].try_into().unwrap());
    buf.truncate(len as usize);
    if crc32(&buf) != crc {
        return Err(Error::corruption(format!(
            "{}: block at offset {off} fails its checksum",
            path.display()
        )));
    }
    Ok(buf)
}

/// Two-level iterator: the index block picks a data block, a block iterator
/// walks it.
pub struct TableIter {
    table: Arc<Table>,
    index: BlockIter,
    data: Option<BlockIter>,
    data_off: u64,
    fill: bool,
    err: Option<Error>,
}

impl TableIter {
    pub fn new(table: Arc<Table>, fill: bool) -> TableIter {
        let index = table.index.clone().iter();
        TableIter {
            table,
            index,
            data: None,
            data_off: u64::MAX,
            fill,
            err: None,
        }
    }

    /// Opens the data block the index points at, keeping the current one if
    /// it is the same block.
    fn load_block(&mut self) {
        if !self.index.valid() {
            if let Err(e) = self.index.status() {
                self.err = Some(e);
            }
            self.data = None;
            return;
        }
        let handle = decode_handle(self.index.value());
        match handle
            .and_then(|(off, len)| self.table.read_block(off, len, self.fill).map(|b| (off, b)))
        {
            Ok((off, b)) => {
                if self.data.is_none() || self.data_off != off {
                    self.data = Some(b.iter());
                    self.data_off = off;
                }
            }
            Err(e) => {
                self.err = Some(e);
                self.data = None;
            }
        }
    }

    fn data_valid(&self) -> bool {
        self.data.as_ref().is_some_and(BlockIter::valid)
    }

    fn data_failed(&mut self) -> bool {
        if let Some(d) = &self.data
            && let Err(e) = d.status()
        {
            self.err = Some(e);
            return true;
        }
        self.err.is_some()
    }

    fn skip_forward(&mut self) {
        while !self.data_valid() {
            if self.data_failed() || !self.index.valid() {
                self.data = None;
                return;
            }
            self.index.next();
            self.load_block();
            match &mut self.data {
                Some(d) => d.seek_to_first(),
                None => return,
            }
        }
    }

    fn skip_backward(&mut self) {
        while !self.data_valid() {
            if self.data_failed() || !self.index.valid() {
                self.data = None;
                return;
            }
            self.index.prev();
            self.load_block();
            match &mut self.data {
                Some(d) => d.seek_to_last(),
                None => return,
            }
        }
    }
}

impl InternalIterator for TableIter {
    fn valid(&self) -> bool {
        self.err.is_none() && self.data_valid()
    }

    fn seek_to_first(&mut self) {
        self.err = None;
        self.index.seek_to_first();
        self.load_block();
        if let Some(d) = &mut self.data {
            d.seek_to_first();
        }
        self.skip_forward();
    }

    fn seek_to_last(&mut self) {
        self.err = None;
        self.index.seek_to_last();
        self.load_block();
        if let Some(d) = &mut self.data {
            d.seek_to_last();
        }
        self.skip_backward();
    }

    fn seek(&mut self, target: &[u8]) {
        self.err = None;
        self.index.seek(target);
        self.load_block();
        if let Some(d) = &mut self.data {
            d.seek(target);
        }
        self.skip_forward();
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        self.err = None;
        self.index.seek(target);
        if !self.index.valid() {
            if let Err(e) = self.index.status() {
                self.err = Some(e);
                self.data = None;
                return;
            }
            self.index.seek_to_last();
        }
        self.load_block();
        if let Some(d) = &mut self.data {
            d.seek_for_prev(target);
        }
        self.skip_backward();
    }

    fn next(&mut self) {
        if let Some(d) = &mut self.data {
            d.next();
        }
        self.skip_forward();
    }

    fn prev(&mut self) {
        if let Some(d) = &mut self.data {
            d.prev();
        }
        self.skip_backward();
    }

    fn key(&self) -> &[u8] {
        self.data.as_ref().expect("valid iterator").key()
    }

    fn value(&self) -> &[u8] {
        self.data.as_ref().expect("valid iterator").value()
    }

    fn status(&self) -> Result<()> {
        match &self.err {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }
}
