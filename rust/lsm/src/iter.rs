//! Iterators over internal keys (memtables, table files, levels, the merge of
//! all of them) and `DbIter`, which turns the merged stream into the user
//! view: the newest version of each key visible at the snapshot, minus point
//! and range deletions, inside the iterator's bounds. `DbIter` follows the
//! forward and reverse scans of LevelDB's `db/db_iter.cc`.

use std::cmp::Ordering;
use std::sync::Arc;

use nilengine_api::{DbIterator, Error, Result};

use crate::key::{
    KIND_DELETE, KIND_SEEK, MAX_SEQ, cmp_internal, make_internal, parse_trailer, user_key,
};
use crate::rangedel::Fragments;
use crate::table::TableIter;
use crate::version::FileMeta;

pub trait InternalIterator: Send {
    fn valid(&self) -> bool;
    fn seek_to_first(&mut self);
    fn seek_to_last(&mut self);
    /// First entry >= `target`.
    fn seek(&mut self, target: &[u8]);
    /// Last entry <= `target`.
    fn seek_for_prev(&mut self, target: &[u8]);
    fn next(&mut self);
    fn prev(&mut self);
    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
    fn status(&self) -> Result<()>;
}

/// Merges children by scanning them for the smallest (or largest) key. The
/// children are few (memtables, level-0 files, one per deeper level), so a
/// scan beats a heap and handles both directions. Callers call `next` only
/// after a forward positioning and `prev` only after a reverse one.
pub struct MergingIterator {
    children: Vec<Box<dyn InternalIterator>>,
    current: Option<usize>,
}

impl MergingIterator {
    pub fn new(children: Vec<Box<dyn InternalIterator>>) -> Self {
        MergingIterator {
            children,
            current: None,
        }
    }

    fn pick(&mut self, want: Ordering) {
        let mut best: Option<usize> = None;
        for (i, c) in self.children.iter().enumerate() {
            if !c.valid() {
                continue;
            }
            best = match best {
                Some(b) if cmp_internal(c.key(), self.children[b].key()) != want => Some(b),
                _ => Some(i),
            };
        }
        self.current = best;
    }
}

impl InternalIterator for MergingIterator {
    fn valid(&self) -> bool {
        self.current.is_some()
    }

    fn seek_to_first(&mut self) {
        for c in &mut self.children {
            c.seek_to_first();
        }
        self.pick(Ordering::Less);
    }

    fn seek_to_last(&mut self) {
        for c in &mut self.children {
            c.seek_to_last();
        }
        self.pick(Ordering::Greater);
    }

    fn seek(&mut self, target: &[u8]) {
        for c in &mut self.children {
            c.seek(target);
        }
        self.pick(Ordering::Less);
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        for c in &mut self.children {
            c.seek_for_prev(target);
        }
        self.pick(Ordering::Greater);
    }

    fn next(&mut self) {
        if let Some(i) = self.current {
            self.children[i].next();
        }
        self.pick(Ordering::Less);
    }

    fn prev(&mut self) {
        if let Some(i) = self.current {
            self.children[i].prev();
        }
        self.pick(Ordering::Greater);
    }

    fn key(&self) -> &[u8] {
        self.children[self.current.expect("valid iterator")].key()
    }

    fn value(&self) -> &[u8] {
        self.children[self.current.expect("valid iterator")].value()
    }

    fn status(&self) -> Result<()> {
        for c in &self.children {
            c.status()?;
        }
        Ok(())
    }
}

/// Iterates a level of non-overlapping files sorted by key, opening one file
/// at a time.
pub struct LevelIter {
    files: Vec<Arc<FileMeta>>,
    idx: usize,
    cur: Option<TableIter>,
    fill: bool,
    err: Option<Error>,
}

impl LevelIter {
    pub fn new(files: Vec<Arc<FileMeta>>, fill: bool) -> LevelIter {
        let idx = files.len();
        LevelIter {
            files,
            idx,
            cur: None,
            fill,
            err: None,
        }
    }

    fn open(&mut self, i: usize) -> bool {
        if i >= self.files.len() {
            self.cur = None;
            self.idx = self.files.len();
            return false;
        }
        if self.idx == i && self.cur.is_some() {
            return true;
        }
        match self.files[i].table() {
            Ok(t) => {
                self.cur = Some(TableIter::new(t, self.fill));
                self.idx = i;
                true
            }
            Err(e) => {
                self.err = Some(e);
                self.cur = None;
                false
            }
        }
    }

    fn cur_stuck(&self) -> bool {
        self.cur
            .as_ref()
            .is_some_and(|c| !c.valid() && c.status().is_ok())
    }

    fn skip_forward(&mut self) {
        while self.cur_stuck() {
            if !self.open(self.idx + 1) {
                return;
            }
            self.cur.as_mut().unwrap().seek_to_first();
        }
    }

    fn skip_backward(&mut self) {
        while self.cur_stuck() {
            if self.idx == 0 {
                self.cur = None;
                return;
            }
            if !self.open(self.idx - 1) {
                return;
            }
            self.cur.as_mut().unwrap().seek_to_last();
        }
    }

    /// Index of the first file whose largest key is >= `target`.
    fn find(&self, target: &[u8]) -> usize {
        self.files
            .partition_point(|f| cmp_internal(&f.d.largest, target) == Ordering::Less)
    }
}

impl InternalIterator for LevelIter {
    fn valid(&self) -> bool {
        self.err.is_none() && self.cur.as_ref().is_some_and(|c| c.valid())
    }

    fn seek_to_first(&mut self) {
        self.err = None;
        if self.open(0) {
            self.cur.as_mut().unwrap().seek_to_first();
            self.skip_forward();
        }
    }

    fn seek_to_last(&mut self) {
        self.err = None;
        if let Some(last) = self.files.len().checked_sub(1)
            && self.open(last)
        {
            self.cur.as_mut().unwrap().seek_to_last();
            self.skip_backward();
        }
    }

    fn seek(&mut self, target: &[u8]) {
        self.err = None;
        let i = self.find(target);
        if self.open(i) {
            self.cur.as_mut().unwrap().seek(target);
            self.skip_forward();
        }
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        self.err = None;
        if self.files.is_empty() {
            return;
        }
        let i = self.find(target).min(self.files.len() - 1);
        if self.open(i) {
            self.cur.as_mut().unwrap().seek_for_prev(target);
            self.skip_backward();
        }
    }

    fn next(&mut self) {
        if let Some(c) = &mut self.cur {
            c.next();
        }
        self.skip_forward();
    }

    fn prev(&mut self) {
        if let Some(c) = &mut self.cur {
            c.prev();
        }
        self.skip_backward();
    }

    fn key(&self) -> &[u8] {
        self.cur.as_ref().expect("valid iterator").key()
    }

    fn value(&self) -> &[u8] {
        self.cur.as_ref().expect("valid iterator").value()
    }

    fn status(&self) -> Result<()> {
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        match &self.cur {
            Some(c) => c.status(),
            None => Ok(()),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Direction {
    Forward,
    Reverse,
}

/// The user-facing iterator. In the forward direction the merged iterator
/// sits on the entry being returned. In the reverse direction it sits just
/// before all entries of the returned key, whose key and value are copied
/// into `saved_key` and `saved_value`.
pub struct DbIter {
    merged: MergingIterator,
    seq: u64,
    frags: Option<Fragments>,
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    dir: Direction,
    valid: bool,
    saved_key: Vec<u8>,
    saved_value: Vec<u8>,
    err: Option<Error>,
    /// Keeps the memtables and files the children read alive.
    _pin: Box<dyn std::any::Any + Send + Sync>,
}

impl DbIter {
    pub fn new(
        merged: MergingIterator,
        seq: u64,
        frags: Option<Fragments>,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        pin: Box<dyn std::any::Any + Send + Sync>,
    ) -> DbIter {
        DbIter {
            merged,
            seq,
            frags: frags.filter(|f| !f.is_empty()),
            lower,
            upper,
            dir: Direction::Forward,
            valid: false,
            saved_key: Vec::new(),
            saved_value: Vec::new(),
            err: None,
            _pin: pin,
        }
    }

    fn covered(&self, ukey: &[u8], seq: u64) -> bool {
        self.frags
            .as_ref()
            .is_some_and(|f| f.max_covering(ukey, self.seq) > seq)
    }

    fn check_status(&mut self) {
        if let Err(e) = self.merged.status() {
            self.err = Some(e);
            self.valid = false;
        }
    }

    /// Moves forward to the next visible entry. With `skipping`, entries of
    /// user keys <= `saved_key` are hidden.
    fn find_next_user_entry(&mut self, mut skipping: bool) {
        loop {
            if !self.merged.valid() {
                self.valid = false;
                return;
            }
            let ikey = self.merged.key();
            let (seq, kind) = parse_trailer(ikey);
            let ukey = user_key(ikey);
            if let Some(up) = &self.upper
                && ukey >= up.as_slice()
            {
                self.valid = false;
                return;
            }
            if seq <= self.seq && !(skipping && ukey <= self.saved_key.as_slice()) {
                if kind == KIND_DELETE || self.covered(ukey, seq) {
                    self.saved_key.clear();
                    self.saved_key.extend_from_slice(ukey);
                    skipping = true;
                } else {
                    self.valid = true;
                    return;
                }
            }
            self.merged.next();
        }
    }

    /// Moves backward over the entries of the previous user key and keeps it
    /// if its newest visible version is a live value; otherwise continues.
    fn find_prev_user_entry(&mut self) {
        let mut found = false;
        self.saved_key.clear();
        self.saved_value.clear();
        while self.merged.valid() {
            let ikey = self.merged.key();
            let (seq, kind) = parse_trailer(ikey);
            let ukey = user_key(ikey);
            if let Some(lo) = &self.lower
                && ukey < lo.as_slice()
            {
                break;
            }
            if seq <= self.seq {
                if found && ukey < self.saved_key.as_slice() {
                    break;
                }
                if kind == KIND_DELETE || self.covered(ukey, seq) {
                    found = false;
                } else {
                    found = true;
                    self.saved_key.clear();
                    self.saved_key.extend_from_slice(ukey);
                    let v = self.merged.value();
                    self.saved_value.clear();
                    self.saved_value.extend_from_slice(v);
                }
            }
            self.merged.prev();
        }
        if found {
            self.valid = true;
            self.dir = Direction::Reverse;
        } else {
            self.valid = false;
            self.dir = Direction::Forward;
            self.saved_key.clear();
            self.saved_value.clear();
        }
    }

    fn seek_forward(&mut self, target: &[u8]) {
        self.dir = Direction::Forward;
        self.saved_key.clear();
        self.merged
            .seek(&make_internal(target, self.seq, KIND_SEEK));
        self.find_next_user_entry(false);
        self.check_status();
    }

    /// Positions before all entries of `key` and scans backward.
    fn seek_backward_before(&mut self, key: &[u8]) {
        self.merged
            .seek_for_prev(&make_internal(key, MAX_SEQ, KIND_SEEK));
        self.find_prev_user_entry();
        self.check_status();
    }
}

impl DbIterator for DbIter {
    fn valid(&self) -> bool {
        self.valid && self.err.is_none()
    }

    fn seek_to_first(&mut self) {
        self.err = None;
        match self.lower.clone() {
            Some(lo) => self.seek_forward(&lo),
            None => {
                self.dir = Direction::Forward;
                self.saved_key.clear();
                self.merged.seek_to_first();
                self.find_next_user_entry(false);
                self.check_status();
            }
        }
    }

    fn seek_to_last(&mut self) {
        self.err = None;
        match self.upper.clone() {
            Some(up) => self.seek_backward_before(&up),
            None => {
                self.merged.seek_to_last();
                self.find_prev_user_entry();
                self.check_status();
            }
        }
    }

    fn seek(&mut self, target: &[u8]) {
        self.err = None;
        match self.lower.clone() {
            Some(lo) if target < lo.as_slice() => self.seek_forward(&lo),
            _ => self.seek_forward(target),
        }
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        self.err = None;
        match self.upper.clone() {
            Some(up) if target >= up.as_slice() => self.seek_backward_before(&up),
            _ => {
                // (target, 0, DELETE) is the last internal key of `target`, so
                // the scan starts with the oldest entry of `target` itself.
                self.merged
                    .seek_for_prev(&make_internal(target, 0, KIND_DELETE));
                self.find_prev_user_entry();
                self.check_status();
            }
        }
    }

    fn next(&mut self) {
        if !self.valid() {
            return;
        }
        if self.dir == Direction::Reverse {
            self.dir = Direction::Forward;
            let t = make_internal(&self.saved_key, 0, KIND_DELETE);
            self.merged.seek(&t);
        } else {
            let k = user_key(self.merged.key()).to_vec();
            self.saved_key = k;
            self.merged.next();
        }
        self.find_next_user_entry(true);
        self.check_status();
    }

    fn prev(&mut self) {
        if !self.valid() {
            return;
        }
        if self.dir == Direction::Forward {
            let cur = user_key(self.merged.key()).to_vec();
            self.seek_backward_before(&cur);
            return;
        }
        self.find_prev_user_entry();
        self.check_status();
    }

    fn key(&self) -> &[u8] {
        match self.dir {
            Direction::Forward => user_key(self.merged.key()),
            Direction::Reverse => &self.saved_key,
        }
    }

    fn value(&self) -> &[u8] {
        match self.dir {
            Direction::Forward => self.merged.value(),
            Direction::Reverse => &self.saved_value,
        }
    }

    fn status(&self) -> Result<()> {
        match &self.err {
            Some(e) => Err(e.clone()),
            None => self.merged.status(),
        }
    }
}
