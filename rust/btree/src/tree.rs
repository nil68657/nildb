//! Committed trees and the copy-on-write write transaction.
//!
//! A commit never overwrites a page that a committed tree uses: the write
//! transaction copies every page it changes to a free page (LMDB's scheme,
//! Howard Chu, "MDB: A Memory-Mapped Database and Backend for OpenLDAP",
//! 2011), and the old pages become free only once no reader can see them.
//! Nodes are parsed into `Node` values while a transaction edits them and
//! serialized when it commits.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use nilengine_api::{Error, Result};

use crate::page::{
    ChildRef, HEADER, LeafCell, LeafVal, Node, TYPE_BRANCH, TYPE_LEAF, TYPE_OVERFLOW, ValRef, aux1,
    aux2, branch_cell_size, branch_child, branch_find, checksum_ok, count, init_header,
    inline_cell_size, leaf_key, leaf_search, leaf_val, max_cell, overflow_capacity, page_no,
    page_txn, page_type, seal, validate,
};
use crate::pager::Pager;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Root {
    pub page: u64,
    pub txn: u64,
    pub keys: u64,
}

impl Root {
    pub fn child(&self) -> ChildRef {
        ChildRef {
            page: self.page,
            txn: self.txn,
        }
    }
}

/// Counts live `TreeState`s per transaction id; the oldest decides which
/// freed pages may be reused.
#[derive(Default)]
pub struct ReaderTable {
    m: Mutex<BTreeMap<u64, usize>>,
}

impl ReaderTable {
    fn register(&self, txn: u64) {
        *self.m.lock().unwrap().entry(txn).or_insert(0) += 1;
    }

    fn release(&self, txn: u64) {
        let mut m = self.m.lock().unwrap();
        if let Some(n) = m.get_mut(&txn) {
            *n -= 1;
            if *n == 0 {
                m.remove(&txn);
            }
        }
    }

    pub fn oldest(&self) -> Option<u64> {
        self.m.lock().unwrap().keys().next().copied()
    }
}

/// A committed tree: the roots of every column family after transaction
/// `txn`. The engine keeps the latest one; snapshots, iterators and reads
/// hold clones of the `Arc`, which keeps their pages from being reused.
pub struct TreeState {
    pub txn: u64,
    pub roots: Vec<Root>,
    pub num_pages: u64,
    readers: Arc<ReaderTable>,
}

impl TreeState {
    pub fn new(
        txn: u64,
        roots: Vec<Root>,
        num_pages: u64,
        readers: &Arc<ReaderTable>,
    ) -> Arc<TreeState> {
        readers.register(txn);
        Arc::new(TreeState {
            txn,
            roots,
            num_pages,
            readers: readers.clone(),
        })
    }
}

impl Drop for TreeState {
    fn drop(&mut self) {
        self.readers.release(self.txn);
    }
}

pub fn read_overflow(pager: &Pager, mut page: u64, txn: u64, len: u32) -> Result<Vec<u8>> {
    let len = len as usize;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        if page == 0 {
            return Err(Error::corruption("overflow chain ends early"));
        }
        let p = pager.read(page, txn, false)?;
        if page_type(&p) != TYPE_OVERFLOW {
            return Err(Error::corruption(format!(
                "page {page} in an overflow chain is not an overflow page"
            )));
        }
        let n = aux2(&p) as usize;
        if n == 0 || n > overflow_capacity(p.len()) || out.len() + n > len {
            return Err(Error::corruption(format!(
                "overflow page {page} holds {n} bytes"
            )));
        }
        out.extend_from_slice(&p[HEADER..HEADER + n]);
        page = aux1(&p);
    }
    Ok(out)
}

pub fn read_value(pager: &Pager, v: ValRef<'_>) -> Result<Vec<u8>> {
    match v {
        ValRef::Inline(b) => Ok(b.to_vec()),
        ValRef::Overflow { page, txn, len } => read_overflow(pager, page, txn, len),
    }
}

/// Point read from a committed tree.
pub fn get(pager: &Pager, root: &Root, key: &[u8]) -> Result<Option<Vec<u8>>> {
    if root.page == 0 {
        return Ok(None);
    }
    let mut r = root.child();
    loop {
        let p = pager.read(r.page, r.txn, true)?;
        match page_type(&p) {
            TYPE_BRANCH => r = branch_child(&p, branch_find(&p, key)),
            TYPE_LEAF => {
                let i = leaf_search(&p, key, false);
                if i < count(&p) && leaf_key(&p, i) == key {
                    return read_value(pager, leaf_val(&p, i)).map(Some);
                }
                return Ok(None);
            }
            t => {
                return Err(Error::corruption(format!(
                    "page {} of type {t} inside a tree",
                    r.page
                )));
            }
        }
    }
}

/// Walks a committed tree read straight from the file, checking every page
/// (checksum, number, writer transaction, structure) and marking the pages
/// it reaches in `seen`. Recovery uses it to choose a meta page and to find
/// the free pages. Returns the number of keys.
pub fn verify_tree(pager: &Pager, root: &Root, num_pages: u64, seen: &mut [bool]) -> Result<u64> {
    if root.page == 0 {
        return Ok(0);
    }
    let cap = overflow_capacity(pager.page_size());
    let mut mark = |n: u64| -> Result<()> {
        if n < 2 || n >= num_pages {
            return Err(Error::corruption(format!(
                "tree references page {n} outside 2..{num_pages}"
            )));
        }
        if std::mem::replace(&mut seen[n as usize], true) {
            return Err(Error::corruption(format!("page {n} is reachable twice")));
        }
        Ok(())
    };
    let mut keys = 0u64;
    let mut stack = vec![root.child()];
    while let Some(r) = stack.pop() {
        mark(r.page)?;
        let p = pager.read_raw(r.page)?;
        validate(&p, r.page)?;
        if page_txn(&p) != r.txn {
            return Err(Error::corruption(format!(
                "page {} holds transaction {}, expected {}",
                r.page,
                page_txn(&p),
                r.txn
            )));
        }
        match page_type(&p) {
            TYPE_BRANCH => {
                for i in 0..=count(&p) {
                    let c = branch_child(&p, i);
                    if c.txn > r.txn {
                        return Err(Error::corruption(format!(
                            "page {} has a child newer than itself",
                            r.page
                        )));
                    }
                    stack.push(c);
                }
            }
            TYPE_LEAF => {
                keys += count(&p) as u64;
                for i in 0..count(&p) {
                    if let ValRef::Overflow { page, txn, len } = leaf_val(&p, i) {
                        let (mut pg, mut left) = (page, len as usize);
                        while left > 0 {
                            mark(pg)?;
                            let o = pager.read_raw(pg)?;
                            if !checksum_ok(&o)
                                || page_type(&o) != TYPE_OVERFLOW
                                || page_no(&o) != pg
                                || page_txn(&o) != txn
                            {
                                return Err(Error::corruption(format!(
                                    "overflow page {pg} did not reach the disk intact"
                                )));
                            }
                            let n = aux2(&o) as usize;
                            if n == 0 || n > cap || n > left {
                                return Err(Error::corruption(format!(
                                    "overflow page {pg} holds {n} bytes"
                                )));
                            }
                            left -= n;
                            pg = aux1(&o);
                        }
                    }
                }
            }
            t => {
                return Err(Error::corruption(format!(
                    "page {} of type {t} inside a tree",
                    r.page
                )));
            }
        }
    }
    Ok(keys)
}

/// Free pages. A page freed by transaction `F` was last visible in tree
/// `F - 1`; it becomes reusable once every live reader and the last durable
/// commit are at `F` or later.
#[derive(Default)]
pub struct Allocator {
    ready: Vec<u64>,
    pending: BTreeMap<u64, Vec<u64>>,
    pub num_pages: u64,
}

impl Allocator {
    pub fn new(mut ready: Vec<u64>, num_pages: u64) -> Allocator {
        ready.sort_unstable_by(|a, b| b.cmp(a));
        Allocator {
            ready,
            pending: BTreeMap::new(),
            num_pages,
        }
    }

    /// Makes pages freed by transactions `<= limit` reusable.
    pub fn release_up_to(&mut self, limit: u64) {
        while let Some((&f, _)) = self.pending.first_key_value() {
            if f > limit {
                break;
            }
            let pages = self.pending.pop_first().unwrap().1;
            self.ready.extend(pages);
        }
    }

    /// Records pages freed by transaction `txn`.
    pub fn free_at(&mut self, txn: u64, pages: Vec<u64>) {
        self.pending.entry(txn).or_default().extend(pages);
    }

    pub fn ready_count(&self) -> usize {
        self.ready.len()
    }

    pub fn pending_count(&self) -> usize {
        self.pending.values().map(Vec::len).sum()
    }
}

/// What a finished transaction hands to the commit.
pub struct TxnOutput {
    pub pages: Vec<(u64, Vec<u8>)>,
    pub overflow: Vec<(u64, Vec<u8>)>,
    pub freed: Vec<u64>,
    pub roots: Vec<Root>,
}

/// One copy-on-write transaction. Pages written by this transaction have
/// `txn == self.txn` and live in `dirty` (tree nodes) or `overflow`.
pub struct WriteTxn<'a> {
    pager: &'a Pager,
    page_size: usize,
    txn: u64,
    pub roots: Vec<Root>,
    dirty: HashMap<u64, Node>,
    overflow: HashMap<u64, Vec<u8>>,
    alloc: &'a mut Allocator,
    taken: Vec<u64>,
    start_num_pages: u64,
    local_free: Vec<u64>,
    freed: Vec<u64>,
}

enum RangeRes {
    Unchanged,
    Changed(ChildRef),
    Gone,
}

impl<'a> WriteTxn<'a> {
    pub fn new(
        pager: &'a Pager,
        txn: u64,
        roots: Vec<Root>,
        alloc: &'a mut Allocator,
    ) -> WriteTxn<'a> {
        let start_num_pages = alloc.num_pages;
        WriteTxn {
            pager,
            page_size: pager.page_size(),
            txn,
            roots,
            dirty: HashMap::new(),
            overflow: HashMap::new(),
            alloc,
            taken: Vec::new(),
            start_num_pages,
            local_free: Vec::new(),
            freed: Vec::new(),
        }
    }

    fn me(&self, page: u64) -> ChildRef {
        ChildRef {
            page,
            txn: self.txn,
        }
    }

    fn alloc_page(&mut self) -> u64 {
        if let Some(p) = self.local_free.pop() {
            return p;
        }
        if let Some(p) = self.alloc.ready.pop() {
            self.taken.push(p);
            return p;
        }
        let p = self.alloc.num_pages;
        self.alloc.num_pages += 1;
        p
    }

    /// Releases a tree page this transaction no longer references.
    fn drop_page(&mut self, r: ChildRef) {
        if r.txn == self.txn {
            self.dirty.remove(&r.page);
            self.local_free.push(r.page);
        } else {
            self.freed.push(r.page);
        }
    }

    fn parse_committed(&self, r: ChildRef) -> Result<Node> {
        let p = self.pager.read(r.page, r.txn, true)?;
        match page_type(&p) {
            TYPE_LEAF | TYPE_BRANCH => Ok(Node::parse(&p)),
            t => Err(Error::corruption(format!(
                "page {} of type {t} inside a tree",
                r.page
            ))),
        }
    }

    /// Returns the page number under which this transaction may modify the
    /// node `r`, copying a committed node to a new page first.
    fn make_dirty(&mut self, r: ChildRef) -> Result<u64> {
        if r.txn == self.txn {
            debug_assert!(self.dirty.contains_key(&r.page));
            return Ok(r.page);
        }
        let node = self.parse_committed(r)?;
        let p = self.alloc_page();
        self.dirty.insert(p, node);
        self.freed.push(r.page);
        Ok(p)
    }

    fn node(&self, p: u64) -> &Node {
        &self.dirty[&p]
    }

    fn node_mut(&mut self, p: u64) -> &mut Node {
        self.dirty.get_mut(&p).expect("dirty page")
    }

    fn node_size(&self, r: ChildRef) -> Result<usize> {
        if r.txn == self.txn {
            return Ok(self.node(r.page).size());
        }
        Ok(self.parse_committed(r)?.size())
    }

    fn child_count(&self, p: u64) -> usize {
        match self.node(p) {
            Node::Branch { cells, .. } => cells.len() + 1,
            Node::Leaf(_) => 0,
        }
    }

    fn child_at(&self, p: u64, i: usize) -> ChildRef {
        match self.node(p) {
            Node::Branch { first, cells } => {
                if i == 0 {
                    *first
                } else {
                    cells[i - 1].1
                }
            }
            Node::Leaf(_) => unreachable!("leaf has no children"),
        }
    }

    fn set_child(&mut self, p: u64, i: usize, c: ChildRef) {
        if let Node::Branch { first, cells } = self.node_mut(p) {
            if i == 0 {
                *first = c;
            } else {
                cells[i - 1].1 = c;
            }
        }
    }

    /// Removes child `i` of branch `p` with the separator that bounds it.
    fn remove_child(&mut self, p: u64, i: usize) {
        if let Node::Branch { first, cells } = self.node_mut(p) {
            if i == 0 {
                if cells.is_empty() {
                    *first = ChildRef { page: 0, txn: 0 };
                } else {
                    *first = cells.remove(0).1;
                }
            } else {
                cells.remove(i - 1);
            }
        }
    }

    fn write_overflow(&mut self, value: &[u8]) -> LeafVal {
        let cap = overflow_capacity(self.page_size);
        let chunks: Vec<&[u8]> = value.chunks(cap).collect();
        let pages: Vec<u64> = chunks.iter().map(|_| self.alloc_page()).collect();
        for (i, chunk) in chunks.iter().enumerate() {
            let mut p = vec![0u8; self.page_size];
            let next = pages.get(i + 1).copied().unwrap_or(0);
            init_header(
                &mut p,
                TYPE_OVERFLOW,
                0,
                self.txn,
                pages[i],
                next,
                chunk.len() as u64,
            );
            p[HEADER..HEADER + chunk.len()].copy_from_slice(chunk);
            seal(&mut p);
            self.overflow.insert(pages[i], p);
        }
        LeafVal::Overflow {
            page: pages[0],
            txn: self.txn,
            len: value.len() as u32,
        }
    }

    fn make_val(&mut self, key_len: usize, value: &[u8]) -> LeafVal {
        if inline_cell_size(key_len, value.len()) <= max_cell(self.page_size) {
            LeafVal::Inline(value.to_vec())
        } else {
            self.write_overflow(value)
        }
    }

    fn free_value(&mut self, v: LeafVal) -> Result<()> {
        let LeafVal::Overflow { mut page, txn, .. } = v else {
            return Ok(());
        };
        while page != 0 {
            let next = if txn == self.txn {
                let p = self.overflow.remove(&page).ok_or_else(|| {
                    Error::corruption("overflow page of this transaction is missing")
                })?;
                self.local_free.push(page);
                aux1(&p)
            } else {
                let p = self.pager.read(page, txn, false)?;
                if page_type(&p) != TYPE_OVERFLOW {
                    return Err(Error::corruption(format!(
                        "page {page} in an overflow chain is not an overflow page"
                    )));
                }
                self.freed.push(page);
                aux1(&p)
            };
            page = next;
        }
        Ok(())
    }

    fn value_of(&self, v: &LeafVal) -> Result<Vec<u8>> {
        match v {
            LeafVal::Inline(b) => Ok(b.clone()),
            LeafVal::Overflow { page, txn, len } if *txn == self.txn => {
                let mut out = Vec::with_capacity(*len as usize);
                let mut pg = *page;
                while pg != 0 {
                    let p = &self.overflow[&pg];
                    out.extend_from_slice(&p[HEADER..HEADER + aux2(p) as usize]);
                    pg = aux1(p);
                }
                Ok(out)
            }
            LeafVal::Overflow { page, txn, len } => read_overflow(self.pager, *page, *txn, *len),
        }
    }

    /// Reads `key` from the tree as this transaction has changed it. With
    /// `want_value` false a found key returns an empty vector.
    pub fn get(&self, cf: usize, key: &[u8], want_value: bool) -> Result<Option<Vec<u8>>> {
        let root = self.roots[cf];
        if root.page == 0 {
            return Ok(None);
        }
        let mut r = root.child();
        loop {
            if r.txn == self.txn {
                match self.node(r.page) {
                    Node::Branch { first, cells } => {
                        let i = cells.partition_point(|(k, _)| k.as_slice() <= key);
                        r = if i == 0 { *first } else { cells[i - 1].1 };
                    }
                    Node::Leaf(cells) => {
                        return match cells.binary_search_by(|c| c.key.as_slice().cmp(key)) {
                            Ok(i) if want_value => self.value_of(&cells[i].val).map(Some),
                            Ok(_) => Ok(Some(Vec::new())),
                            Err(_) => Ok(None),
                        };
                    }
                }
            } else {
                let p = self.pager.read(r.page, r.txn, true)?;
                match page_type(&p) {
                    TYPE_BRANCH => r = branch_child(&p, branch_find(&p, key)),
                    TYPE_LEAF => {
                        let i = leaf_search(&p, key, false);
                        if i < count(&p) && leaf_key(&p, i) == key {
                            return if want_value {
                                read_value(self.pager, leaf_val(&p, i)).map(Some)
                            } else {
                                Ok(Some(Vec::new()))
                            };
                        }
                        return Ok(None);
                    }
                    t => {
                        return Err(Error::corruption(format!(
                            "page {} of type {t} inside a tree",
                            r.page
                        )));
                    }
                }
            }
        }
    }

    pub fn put(&mut self, cf: usize, key: &[u8], value: &[u8]) -> Result<()> {
        let val = self.make_val(key.len(), value);
        let root = self.roots[cf];
        if root.page == 0 {
            let p = self.alloc_page();
            self.dirty.insert(
                p,
                Node::Leaf(vec![LeafCell {
                    key: key.to_vec(),
                    val,
                }]),
            );
            self.roots[cf] = Root {
                page: p,
                txn: self.txn,
                keys: 1,
            };
            return Ok(());
        }
        let (new_root, split, inserted) = self.insert_rec(root.child(), key, val)?;
        let top = match split {
            None => new_root,
            Some((sep, right)) => {
                let p = self.alloc_page();
                self.dirty.insert(
                    p,
                    Node::Branch {
                        first: new_root,
                        cells: vec![(sep, right)],
                    },
                );
                self.me(p)
            }
        };
        let r = &mut self.roots[cf];
        r.page = top.page;
        r.txn = top.txn;
        if inserted {
            r.keys += 1;
        }
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    fn insert_rec(
        &mut self,
        r: ChildRef,
        key: &[u8],
        val: LeafVal,
    ) -> Result<(ChildRef, Option<(Vec<u8>, ChildRef)>, bool)> {
        let p = self.make_dirty(r)?;
        let (child, idx) = match self.node_mut(p) {
            Node::Leaf(cells) => {
                let (inserted, old) = match cells.binary_search_by(|c| c.key.as_slice().cmp(key)) {
                    Ok(i) => (false, Some(std::mem::replace(&mut cells[i].val, val))),
                    Err(i) => {
                        cells.insert(
                            i,
                            LeafCell {
                                key: key.to_vec(),
                                val,
                            },
                        );
                        (true, None)
                    }
                };
                if let Some(old) = old {
                    self.free_value(old)?;
                }
                let split = self.split_if_needed(p);
                return Ok((self.me(p), split, inserted));
            }
            Node::Branch { first, cells } => {
                let i = cells.partition_point(|(k, _)| k.as_slice() <= key);
                (if i == 0 { *first } else { cells[i - 1].1 }, i)
            }
        };
        let (new_child, split, inserted) = self.insert_rec(child, key, val)?;
        self.set_child(p, idx, new_child);
        if let Some((sep, right)) = split
            && let Node::Branch { cells, .. } = self.node_mut(p)
        {
            cells.insert(idx, (sep, right));
        }
        let split = self.split_if_needed(p);
        Ok((self.me(p), split, inserted))
    }

    /// Splits dirty node `p` in two when it no longer fits in a page,
    /// returning the separator and the new right node.
    fn split_if_needed(&mut self, p: u64) -> Option<(Vec<u8>, ChildRef)> {
        if self.node(p).size() <= self.page_size {
            return None;
        }
        let (sep, right) = match self.node_mut(p) {
            Node::Leaf(cells) => {
                let i = balanced_split(cells.iter().map(LeafCell::size));
                let right = cells.split_off(i);
                let sep = shortest_separator(&cells[cells.len() - 1].key, &right[0].key);
                (sep, Node::Leaf(right))
            }
            Node::Branch { cells, .. } => {
                let i = balanced_split(cells.iter().map(|(k, _)| branch_cell_size(k.len())));
                let mut right = cells.split_off(i);
                let (sep, first) = right.remove(0);
                (
                    sep,
                    Node::Branch {
                        first,
                        cells: right,
                    },
                )
            }
        };
        let q = self.alloc_page();
        self.dirty.insert(q, right);
        Some((sep, self.me(q)))
    }

    /// Deletes `key`; returns whether it existed.
    pub fn delete(&mut self, cf: usize, key: &[u8]) -> Result<bool> {
        if self.get(cf, key, false)?.is_none() {
            return Ok(false);
        }
        let root = self.roots[cf];
        let new_root = self.delete_rec(root.child(), key)?;
        self.roots[cf].keys -= 1;
        self.set_root(cf, Some(new_root));
        Ok(true)
    }

    fn delete_rec(&mut self, r: ChildRef, key: &[u8]) -> Result<ChildRef> {
        let p = self.make_dirty(r)?;
        let (child, idx) = match self.node_mut(p) {
            Node::Leaf(cells) => {
                let i = cells
                    .binary_search_by(|c| c.key.as_slice().cmp(key))
                    .map_err(|_| Error::corruption("deleted key vanished during the delete"))?;
                let old = cells.remove(i);
                self.free_value(old.val)?;
                return Ok(self.me(p));
            }
            Node::Branch { first, cells } => {
                let i = cells.partition_point(|(k, _)| k.as_slice() <= key);
                (if i == 0 { *first } else { cells[i - 1].1 }, i)
            }
        };
        let new_child = self.delete_rec(child, key)?;
        self.set_child(p, idx, new_child);
        self.fix_child(p, idx)?;
        Ok(self.me(p))
    }

    /// After child `i` of branch `p` changed: removes it if empty, or merges
    /// it with a neighbour if it fell below a quarter page and the two fit
    /// in one page.
    fn fix_child(&mut self, p: u64, i: usize) -> Result<bool> {
        let c = self.child_at(p, i);
        if c.page == 0 || (c.txn == self.txn && self.node(c.page).is_empty()) {
            self.remove_child(p, i);
            if c.page != 0 {
                self.drop_page(c);
            }
            return Ok(true);
        }
        let n = self.child_count(p);
        if n < 2 || self.node_size(c)? >= HEADER + max_cell(self.page_size) {
            return Ok(false);
        }
        let li = if i > 0 { i - 1 } else { 0 };
        self.try_merge(p, li)
    }

    /// Merges children `li` and `li + 1` of branch `p` when they fit in one
    /// page.
    fn try_merge(&mut self, p: u64, li: usize) -> Result<bool> {
        let (left, right) = (self.child_at(p, li), self.child_at(p, li + 1));
        let sep_len = match self.node(p) {
            Node::Branch { cells, .. } => cells[li].0.len(),
            Node::Leaf(_) => unreachable!(),
        };
        let rnode = if right.txn == self.txn {
            self.node(right.page).clone()
        } else {
            self.parse_committed(right)?
        };
        let lsize = self.node_size(left)?;
        let extra = match rnode {
            Node::Branch { .. } => branch_cell_size(sep_len),
            Node::Leaf(_) => 0,
        };
        if lsize + rnode.size() - HEADER + extra > self.page_size {
            return Ok(false);
        }
        let lp = self.make_dirty(left)?;
        self.drop_page(right);
        let sep = match self.node_mut(p) {
            Node::Branch { cells, .. } => cells.remove(li).0,
            Node::Leaf(_) => unreachable!(),
        };
        match (self.node_mut(lp), rnode) {
            (Node::Leaf(lc), Node::Leaf(rc)) => lc.extend(rc),
            (Node::Branch { cells: lc, .. }, Node::Branch { first, cells: rc }) => {
                lc.push((sep, first));
                lc.extend(rc);
            }
            _ => return Err(Error::corruption("siblings at different depths")),
        }
        self.set_child(p, li, self.me(lp));
        Ok(true)
    }

    /// Installs a new root for `cf`, collapsing branches left with one child.
    fn set_root(&mut self, cf: usize, mut root: Option<ChildRef>) {
        while let Some(r) = root {
            if r.txn != self.txn {
                break;
            }
            match self.node(r.page) {
                Node::Leaf(cells) if cells.is_empty() => {
                    self.drop_page(r);
                    root = None;
                }
                Node::Branch { first, cells } if cells.is_empty() => {
                    let child = *first;
                    self.drop_page(r);
                    root = (child.page != 0).then_some(child);
                }
                _ => break,
            }
        }
        let keys = self.roots[cf].keys;
        self.roots[cf] = match root {
            Some(r) => Root {
                page: r.page,
                txn: r.txn,
                keys,
            },
            None => Root::default(),
        };
    }

    /// Deletes every key in `[start, end)`. Subtrees entirely inside the
    /// range are freed without being rewritten.
    pub fn delete_range(&mut self, cf: usize, start: &[u8], end: &[u8]) -> Result<()> {
        let root = self.roots[cf];
        if start >= end || root.page == 0 {
            return Ok(());
        }
        let mut removed = 0;
        let res = self.range_rec(root.child(), None, None, start, end, &mut removed)?;
        self.roots[cf].keys -= removed;
        match res {
            RangeRes::Unchanged => {}
            RangeRes::Changed(r) => self.set_root(cf, Some(r)),
            RangeRes::Gone => self.set_root(cf, None),
        }
        Ok(())
    }

    fn range_rec(
        &mut self,
        r: ChildRef,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        start: &[u8],
        end: &[u8],
        removed: &mut u64,
    ) -> Result<RangeRes> {
        if hi.is_some_and(|h| h <= start) || lo.is_some_and(|l| l >= end) {
            return Ok(RangeRes::Unchanged);
        }
        if lo.map_or(start.is_empty(), |l| l >= start) && hi.is_some_and(|h| h <= end) {
            *removed += self.free_subtree(r)?;
            return Ok(RangeRes::Gone);
        }
        let node = if r.txn == self.txn {
            self.node(r.page).clone()
        } else {
            self.parse_committed(r)?
        };
        match node {
            Node::Leaf(cells) => {
                let a = cells.partition_point(|c| c.key.as_slice() < start);
                let b = cells.partition_point(|c| c.key.as_slice() < end);
                if a == b {
                    return Ok(RangeRes::Unchanged);
                }
                let p = self.make_dirty(r)?;
                let gone: Vec<LeafCell> = match self.node_mut(p) {
                    Node::Leaf(cells) => cells.drain(a..b).collect(),
                    Node::Branch { .. } => unreachable!(),
                };
                *removed += gone.len() as u64;
                for c in gone {
                    self.free_value(c.val)?;
                }
                if self.node(p).is_empty() {
                    self.drop_page(self.me(p));
                    return Ok(RangeRes::Gone);
                }
                Ok(RangeRes::Changed(self.me(p)))
            }
            Node::Branch { first, cells } => {
                let n = cells.len() + 1;
                let mut results = Vec::new();
                for i in 0..n {
                    let child = if i == 0 { first } else { cells[i - 1].1 };
                    let clo = if i == 0 {
                        lo
                    } else {
                        Some(cells[i - 1].0.as_slice())
                    };
                    let chi = if i + 1 < n {
                        Some(cells[i].0.as_slice())
                    } else {
                        hi
                    };
                    let res = self.range_rec(child, clo, chi, start, end, removed)?;
                    if !matches!(res, RangeRes::Unchanged) {
                        results.push((i, res));
                    }
                }
                if results.is_empty() {
                    return Ok(RangeRes::Unchanged);
                }
                let p = self.make_dirty(r)?;
                let mut changed = Vec::new();
                for (i, res) in results.iter().rev() {
                    match res {
                        RangeRes::Changed(c) => {
                            self.set_child(p, *i, *c);
                            changed.push(*c);
                        }
                        RangeRes::Gone => self.remove_child(p, *i),
                        RangeRes::Unchanged => {}
                    }
                }
                if self.node(p).is_empty() {
                    self.drop_page(self.me(p));
                    return Ok(RangeRes::Gone);
                }
                // Merge shrunken children with their neighbours.
                let mut i = 0;
                while i < self.child_count(p) {
                    let c = self.child_at(p, i);
                    let small = changed.contains(&c)
                        && self.node_size(c)? < HEADER + max_cell(self.page_size);
                    if small && self.child_count(p) >= 2 {
                        let li = if i > 0 { i - 1 } else { 0 };
                        if self.try_merge(p, li)? {
                            let merged = self.child_at(p, li);
                            changed.push(merged);
                            i = li;
                            continue;
                        }
                    }
                    i += 1;
                }
                Ok(RangeRes::Changed(self.me(p)))
            }
        }
    }

    /// Frees every page of a subtree and its overflow chains; returns the
    /// number of keys it held.
    fn free_subtree(&mut self, r: ChildRef) -> Result<u64> {
        let node = if r.txn == self.txn {
            self.node(r.page).clone()
        } else {
            self.parse_committed(r)?
        };
        let mut keys = 0;
        match node {
            Node::Leaf(cells) => {
                keys += cells.len() as u64;
                for c in cells {
                    self.free_value(c.val)?;
                }
            }
            Node::Branch { first, cells } => {
                keys += self.free_subtree(first)?;
                for (_, c) in cells {
                    keys += self.free_subtree(c)?;
                }
            }
        }
        self.drop_page(r);
        Ok(keys)
    }

    /// Serializes the transaction's pages. Pages it allocated and then
    /// dropped go back to the allocator.
    pub fn finish(self) -> TxnOutput {
        let WriteTxn {
            page_size,
            txn,
            roots,
            dirty,
            overflow,
            alloc,
            local_free,
            freed,
            ..
        } = self;
        alloc.ready.extend(local_free);
        let pages = dirty
            .into_iter()
            .map(|(p, n)| (p, n.write(page_size, txn, p)))
            .collect();
        TxnOutput {
            pages,
            overflow: overflow.into_iter().collect(),
            freed,
            roots,
        }
    }

    /// Returns every page the transaction took.
    pub fn abort(self) {
        self.alloc.ready.extend(self.taken);
        self.alloc.num_pages = self.start_num_pages;
    }
}

/// The split index that best balances the two halves' byte sizes.
fn balanced_split(sizes: impl Iterator<Item = usize>) -> usize {
    let sizes: Vec<usize> = sizes.collect();
    let total: usize = sizes.iter().sum();
    let mut acc = 0;
    let mut best = (usize::MAX, 1);
    for (i, s) in sizes.iter().enumerate().take(sizes.len() - 1) {
        acc += s;
        let worst = acc.max(total - acc);
        if worst < best.0 {
            best = (worst, i + 1);
        }
    }
    best.1
}

/// The shortest key `s` with `left < s <= right`, as a branch separator
/// (LevelDB's `FindShortestSeparator` idea).
fn shortest_separator(left: &[u8], right: &[u8]) -> Vec<u8> {
    let p = left.iter().zip(right).take_while(|(a, b)| a == b).count();
    right[..(p + 1).min(right.len())].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn separators() {
        assert_eq!(shortest_separator(b"abc", b"abd"), b"abd");
        assert_eq!(shortest_separator(b"ab", b"abc"), b"abc");
        assert_eq!(shortest_separator(b"apple", b"banana"), b"b");
        assert_eq!(shortest_separator(b"", b"a"), b"a");
    }

    #[test]
    fn split_balances() {
        assert_eq!(balanced_split([10, 10, 10, 10].into_iter()), 2);
        assert_eq!(balanced_split([30, 5, 5, 5, 5].into_iter()), 1);
    }
}
