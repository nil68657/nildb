//! Cursors over a committed tree. A cursor keeps the path from the root to
//! its leaf as a stack of `(page, index)` pairs and moves between leaves
//! through the parents, as LMDB's cursors do, so leaves need no sibling
//! pointers (a copy-on-write tree could not keep them current).

use std::cell::OnceCell;
use std::sync::Arc;

use nilengine_api::{DbIterator, Error, Result};

use crate::page::{
    ChildRef, TYPE_BRANCH, TYPE_LEAF, ValRef, branch_child, branch_find, count, leaf_key,
    leaf_search, leaf_val, page_type,
};
use crate::pager::{Page, Pager};
use crate::tree::{Root, TreeState, read_overflow};

pub struct Cursor {
    pager: Arc<Pager>,
    _state: Arc<TreeState>,
    root: Root,
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    stack: Vec<(Page, usize)>,
    valid: bool,
    fill: bool,
    value: OnceCell<Vec<u8>>,
    err: Option<Error>,
    lazy_err: OnceCell<Error>,
}

impl Cursor {
    pub fn new(
        pager: Arc<Pager>,
        state: Arc<TreeState>,
        cf: usize,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        fill: bool,
    ) -> Cursor {
        let root = state.roots[cf];
        Cursor {
            pager,
            _state: state,
            root,
            lower,
            upper,
            stack: Vec::new(),
            valid: false,
            fill,
            value: OnceCell::new(),
            err: None,
            lazy_err: OnceCell::new(),
        }
    }

    fn reset(&mut self) {
        self.value = OnceCell::new();
        self.lazy_err = OnceCell::new();
        self.err = None;
        self.valid = false;
    }

    fn fail(&mut self, e: Error) {
        self.err = Some(e);
        self.valid = false;
        self.stack.clear();
    }

    fn load(&self, r: ChildRef) -> Result<Page> {
        let p = self.pager.read(r.page, r.txn, self.fill)?;
        match page_type(&p) {
            TYPE_BRANCH | TYPE_LEAF => Ok(p),
            t => Err(Error::corruption(format!(
                "page {} of type {t} inside a tree",
                r.page
            ))),
        }
    }

    fn leaf(&self) -> (&Page, usize) {
        let (p, i) = self.stack.last().expect("cursor on a leaf");
        (p, *i)
    }

    /// Descends from `r` to a leaf through the leftmost (or rightmost)
    /// children.
    fn push_edge(&mut self, mut r: ChildRef, leftmost: bool) -> Result<()> {
        loop {
            let p = self.load(r)?;
            if page_type(&p) == TYPE_BRANCH {
                let i = if leftmost { 0 } else { count(&p) };
                r = branch_child(&p, i);
                self.stack.push((p, i));
            } else {
                let n = count(&p);
                let i = if leftmost { 0 } else { n.saturating_sub(1) };
                self.stack.push((p, i));
                return Ok(());
            }
        }
    }

    /// Pushes the path to the leaf whose key range holds `key`.
    fn descend_to(&mut self, key: &[u8]) -> Result<bool> {
        self.stack.clear();
        if self.root.page == 0 {
            return Ok(false);
        }
        let mut r = self.root.child();
        loop {
            let p = self.load(r)?;
            if page_type(&p) == TYPE_BRANCH {
                let i = branch_find(&p, key);
                r = branch_child(&p, i);
                self.stack.push((p, i));
            } else {
                self.stack.push((p, 0));
                return Ok(true);
            }
        }
    }

    fn next_leaf(&mut self) -> Result<bool> {
        self.stack.pop();
        loop {
            let Some((p, i)) = self.stack.last_mut() else {
                return Ok(false);
            };
            if *i < count(p) {
                *i += 1;
                let c = branch_child(p, *i);
                self.push_edge(c, true)?;
                return Ok(true);
            }
            self.stack.pop();
        }
    }

    fn prev_leaf(&mut self) -> Result<bool> {
        self.stack.pop();
        loop {
            let Some((p, i)) = self.stack.last_mut() else {
                return Ok(false);
            };
            if *i > 0 {
                *i -= 1;
                let c = branch_child(p, *i);
                self.push_edge(c, false)?;
                return Ok(true);
            }
            self.stack.pop();
        }
    }

    /// From a leaf index that may be past the leaf's end, moves forward to
    /// the next cell and applies the upper bound.
    fn settle_forward(&mut self) -> Result<()> {
        loop {
            if self.stack.is_empty() {
                return Ok(());
            }
            let (p, i) = self.leaf();
            if i < count(p) {
                break;
            }
            if !self.next_leaf()? {
                return Ok(());
            }
        }
        self.valid = self
            .upper
            .as_deref()
            .is_none_or(|up| self.current_key() < up);
        Ok(())
    }

    /// Positions on leaf cell `idx`, or when `idx` is `None` on the last cell
    /// of an earlier leaf, and applies the lower bound.
    fn settle_backward(&mut self, mut idx: Option<usize>) -> Result<()> {
        loop {
            if self.stack.is_empty() {
                return Ok(());
            }
            let n = count(self.leaf().0);
            match idx {
                Some(i) if i < n => {
                    self.stack.last_mut().unwrap().1 = i;
                    break;
                }
                _ => {
                    if !self.prev_leaf()? {
                        return Ok(());
                    }
                    idx = count(self.leaf().0).checked_sub(1);
                }
            }
        }
        self.valid = self
            .lower
            .as_deref()
            .is_none_or(|lo| self.current_key() >= lo);
        Ok(())
    }

    fn seek_ge(&mut self, key: &[u8]) -> Result<()> {
        if !self.descend_to(key)? {
            return Ok(());
        }
        let i = leaf_search(self.leaf().0, key, false);
        self.stack.last_mut().unwrap().1 = i;
        self.settle_forward()
    }

    /// Last key `<= key`, or `< key` when `strict`.
    fn seek_le(&mut self, key: &[u8], strict: bool) -> Result<()> {
        if !self.descend_to(key)? {
            return Ok(());
        }
        let past = leaf_search(self.leaf().0, key, !strict);
        self.settle_backward(past.checked_sub(1))
    }

    fn current_key(&self) -> &[u8] {
        let (p, i) = self.leaf();
        leaf_key(p, i)
    }

    fn run(&mut self, f: impl FnOnce(&mut Self) -> Result<()>) {
        if let Err(e) = f(self) {
            self.fail(e);
        }
    }
}

impl DbIterator for Cursor {
    fn valid(&self) -> bool {
        self.valid && self.err.is_none()
    }

    fn seek_to_first(&mut self) {
        self.reset();
        self.run(|c| match c.lower.clone() {
            Some(lo) => c.seek_ge(&lo),
            None => {
                c.stack.clear();
                if c.root.page == 0 {
                    return Ok(());
                }
                c.push_edge(c.root.child(), true)?;
                c.settle_forward()
            }
        });
    }

    fn seek_to_last(&mut self) {
        self.reset();
        self.run(|c| match c.upper.clone() {
            Some(up) => c.seek_le(&up, true),
            None => {
                c.stack.clear();
                if c.root.page == 0 {
                    return Ok(());
                }
                c.push_edge(c.root.child(), false)?;
                let last = count(c.leaf().0).checked_sub(1);
                c.settle_backward(last)
            }
        });
    }

    fn seek(&mut self, target: &[u8]) {
        self.reset();
        let t = match &self.lower {
            Some(lo) if target < lo.as_slice() => lo.clone(),
            _ => target.to_vec(),
        };
        self.run(|c| c.seek_ge(&t));
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        self.reset();
        self.run(|c| match c.upper.clone() {
            Some(up) if target >= up.as_slice() => c.seek_le(&up, true),
            _ => c.seek_le(target, false),
        });
    }

    fn next(&mut self) {
        if !self.valid() {
            return;
        }
        self.reset();
        self.stack.last_mut().unwrap().1 += 1;
        self.run(|c| c.settle_forward());
    }

    fn prev(&mut self) {
        if !self.valid() {
            return;
        }
        let i = self.leaf().1;
        self.reset();
        self.run(|c| c.settle_backward(i.checked_sub(1)));
    }

    fn key(&self) -> &[u8] {
        self.current_key()
    }

    fn value(&self) -> &[u8] {
        let (p, i) = self.leaf();
        match leaf_val(p, i) {
            ValRef::Inline(v) => v,
            ValRef::Overflow { page, txn, len } => self.value.get_or_init(|| {
                read_overflow(&self.pager, page, txn, len).unwrap_or_else(|e| {
                    let _ = self.lazy_err.set(e);
                    Vec::new()
                })
            }),
        }
    }

    fn status(&self) -> Result<()> {
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        match self.lazy_err.get() {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }
}
