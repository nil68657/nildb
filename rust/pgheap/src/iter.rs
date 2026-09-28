//! Iterators. An iterator holds a registered snapshot and fills a window of
//! visible (key, value) pairs from the index: one scan of index leaves in
//! the direction of travel, then a HOT-chain walk per key. Moving past
//! either end of the window re-descends the index from the last key seen,
//! so concurrent page splits never disturb it, and it holds no page pin
//! between calls.

use std::sync::Arc;

use nilengine_api::{DbIterator, Error, Result, Snapshot};

use crate::db::{Inner, PgSnapshot};
use crate::xact::View;

/// Keys and value bytes gathered per window.
const WINDOW_KEYS: usize = 32;
const WINDOW_BYTES: usize = 1 << 20;

pub struct PgIter {
    inner: Arc<Inner>,
    icf: usize,
    snap: Snapshot,
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    buf: Vec<(Vec<u8>, Vec<u8>)>,
    pos: usize,
    valid: bool,
    err: Option<Error>,
}

impl PgIter {
    pub fn new(
        inner: Arc<Inner>,
        icf: usize,
        snap: Snapshot,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
    ) -> PgIter {
        PgIter {
            inner,
            icf,
            snap,
            lower,
            upper,
            buf: Vec::new(),
            pos: 0,
            valid: false,
            err: None,
        }
    }

    /// Fills the window with the first visible keys at or after `start`
    /// (after, when not `inclusive`) and below the upper bound.
    fn fill_forward(&mut self, start: &[u8], inclusive: bool) -> Result<()> {
        self.buf.clear();
        let e = &*self.inner;
        let data = &self
            .snap
            .downcast::<PgSnapshot>()
            .expect("the engine checked the snapshot")
            .data;
        let index = &e.rels[self.icf].index;
        let mut from = start.to_vec();
        let mut incl = inclusive;
        let mut bytes = 0;
        loop {
            let (groups, done) = index.scan_forward(&e.pool, &from, incl, WINDOW_KEYS)?;
            let vis = e.vis_ctx();
            for g in &groups {
                if self.upper.as_deref().is_some_and(|u| g.key.as_slice() >= u) {
                    return Ok(());
                }
                if let Some(v) = e.resolve(self.icf, &g.key, &g.tids, &vis, &View::Snap(data))? {
                    bytes += v.len();
                    self.buf.push((g.key.clone(), v));
                    if self.buf.len() >= WINDOW_KEYS || bytes >= WINDOW_BYTES {
                        return Ok(());
                    }
                }
            }
            if done || !self.buf.is_empty() {
                return Ok(());
            }
            match groups.last() {
                Some(g) => {
                    from = g.key.clone();
                    incl = false;
                }
                None => return Ok(()),
            }
        }
    }

    /// Fills the window with the last visible keys at or before `start`
    /// (before, when not `inclusive`; `None` is past every key) and at or
    /// above the lower bound, in ascending order.
    fn fill_backward(&mut self, start: Option<&[u8]>, inclusive: bool) -> Result<()> {
        self.buf.clear();
        let e = &*self.inner;
        let data = &self
            .snap
            .downcast::<PgSnapshot>()
            .expect("the engine checked the snapshot")
            .data;
        let index = &e.rels[self.icf].index;
        let lower = self.lower.as_deref();
        let buf = &mut self.buf;
        let mut from = start.map(<[u8]>::to_vec);
        let mut incl = inclusive;
        let mut bytes = 0;
        let res = (|| {
            loop {
                let (groups, done) =
                    index.scan_backward(&e.pool, from.as_deref(), incl, WINDOW_KEYS)?;
                let vis = e.vis_ctx();
                for g in &groups {
                    if lower.is_some_and(|l| g.key.as_slice() < l) {
                        return Ok(());
                    }
                    if let Some(v) =
                        e.resolve(self.icf, &g.key, &g.tids, &vis, &View::Snap(data))?
                    {
                        bytes += v.len();
                        buf.push((g.key.clone(), v));
                        if buf.len() >= WINDOW_KEYS || bytes >= WINDOW_BYTES {
                            return Ok(());
                        }
                    }
                }
                if done || !buf.is_empty() {
                    return Ok(());
                }
                match groups.last() {
                    Some(g) => {
                        from = Some(g.key.clone());
                        incl = false;
                    }
                    None => return Ok(()),
                }
            }
        })();
        self.buf.reverse();
        res
    }

    fn settle(&mut self, res: Result<()>, at_end: bool) {
        match res {
            Ok(()) => {
                self.valid = !self.buf.is_empty();
                self.pos = if at_end {
                    self.buf.len().saturating_sub(1)
                } else {
                    0
                };
            }
            Err(e) => {
                self.buf.clear();
                self.valid = false;
                self.err = Some(e);
            }
        }
    }

    fn seek_forward(&mut self, start: &[u8], inclusive: bool) {
        self.err = None;
        let r = self.fill_forward(start, inclusive);
        self.settle(r, false);
    }

    fn seek_backward(&mut self, start: Option<&[u8]>, inclusive: bool) {
        self.err = None;
        let r = self.fill_backward(start, inclusive);
        self.settle(r, true);
    }
}

impl DbIterator for PgIter {
    fn valid(&self) -> bool {
        self.valid
    }

    fn seek_to_first(&mut self) {
        let start = self.lower.clone().unwrap_or_default();
        self.seek_forward(&start, true);
    }

    fn seek_to_last(&mut self) {
        let upper = self.upper.clone();
        self.seek_backward(upper.as_deref(), false);
    }

    fn seek(&mut self, target: &[u8]) {
        let start = match &self.lower {
            Some(l) if l.as_slice() > target => l.clone(),
            _ => target.to_vec(),
        };
        self.seek_forward(&start, true);
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        match self.upper.clone() {
            Some(u) if target >= u.as_slice() => self.seek_backward(Some(&u), false),
            _ => self.seek_backward(Some(target), true),
        }
    }

    fn next(&mut self) {
        if !self.valid {
            return;
        }
        self.pos += 1;
        if self.pos < self.buf.len() {
            return;
        }
        let last = self
            .buf
            .last()
            .expect("a valid iterator has a window")
            .0
            .clone();
        let r = self.fill_forward(&last, false);
        self.settle(r, false);
    }

    fn prev(&mut self) {
        if !self.valid {
            return;
        }
        if self.pos > 0 {
            self.pos -= 1;
            return;
        }
        let first = self.buf[0].0.clone();
        let r = self.fill_backward(Some(&first), false);
        self.settle(r, true);
    }

    fn key(&self) -> &[u8] {
        if self.valid {
            &self.buf[self.pos].0
        } else {
            &[]
        }
    }

    fn value(&self) -> &[u8] {
        if self.valid {
            &self.buf[self.pos].1
        } else {
            &[]
        }
    }

    fn status(&self) -> Result<()> {
        match &self.err {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }
}
