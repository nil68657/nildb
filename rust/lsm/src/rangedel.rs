//! Range tombstones. A `DeleteRange(start, end)` at sequence `s` hides every
//! entry of `[start, end)` with a sequence below `s`. Tombstones live in the
//! memtable until a flush, then in the manifest (per column family) until no
//! table file can hold an entry they hide. Reads split the tombstones of a
//! source into disjoint fragments, each with the descending list of
//! sequences that cover it, the structure RocksDB's
//! `FragmentedRangeTombstoneList` uses.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeTombstone {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub seq: u64,
}

#[derive(Debug)]
struct Frag {
    start: Vec<u8>,
    end: Vec<u8>,
    /// Sequences of the tombstones covering this fragment, descending.
    seqs: Vec<u64>,
}

#[derive(Debug, Default)]
pub struct Fragments {
    frags: Vec<Frag>,
}

impl Fragments {
    pub fn build(tombstones: &[RangeTombstone]) -> Fragments {
        let ts: Vec<&RangeTombstone> = tombstones.iter().filter(|t| t.start < t.end).collect();
        if ts.is_empty() {
            return Fragments::default();
        }
        let mut points: Vec<&[u8]> = ts
            .iter()
            .flat_map(|t| [t.start.as_slice(), t.end.as_slice()])
            .collect();
        points.sort_unstable();
        points.dedup();
        let mut frags: Vec<Frag> = Vec::new();
        for w in points.windows(2) {
            let (a, b) = (w[0], w[1]);
            let mut seqs: Vec<u64> = ts
                .iter()
                .filter(|t| t.start.as_slice() <= a && t.end.as_slice() >= b)
                .map(|t| t.seq)
                .collect();
            if seqs.is_empty() {
                continue;
            }
            seqs.sort_unstable_by(|x, y| y.cmp(x));
            seqs.dedup();
            if let Some(last) = frags.last_mut()
                && last.end == a
                && last.seqs == seqs
            {
                last.end = b.to_vec();
                continue;
            }
            frags.push(Frag {
                start: a.to_vec(),
                end: b.to_vec(),
                seqs,
            });
        }
        Fragments { frags }
    }

    pub fn is_empty(&self) -> bool {
        self.frags.is_empty()
    }

    fn find(&self, key: &[u8]) -> Option<&Frag> {
        let i = self.frags.partition_point(|f| f.end.as_slice() <= key);
        self.frags.get(i).filter(|f| f.start.as_slice() <= key)
    }

    /// Largest sequence `<= snapshot` among tombstones covering `key`, or 0.
    /// An entry with sequence `e` is deleted at `snapshot` when this is
    /// greater than `e`.
    pub fn max_covering(&self, key: &[u8], snapshot: u64) -> u64 {
        self.find(key)
            .and_then(|f| f.seqs.iter().find(|&&s| s <= snapshot).copied())
            .unwrap_or(0)
    }

    /// Smallest sequence above `seq` among tombstones covering `key`.
    pub fn next_newer(&self, key: &[u8], seq: u64) -> Option<u64> {
        self.find(key)
            .and_then(|f| f.seqs.iter().rev().find(|&&s| s > seq).copied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(start: &str, end: &str, seq: u64) -> RangeTombstone {
        RangeTombstone {
            start: start.as_bytes().to_vec(),
            end: end.as_bytes().to_vec(),
            seq,
        }
    }

    #[test]
    fn overlapping_tombstones_fragment() {
        let f = Fragments::build(&[t("b", "f", 10), t("d", "h", 20), t("x", "x", 30)]);
        assert_eq!(f.max_covering(b"a", 100), 0);
        assert_eq!(f.max_covering(b"b", 100), 10);
        assert_eq!(f.max_covering(b"e", 100), 20);
        assert_eq!(f.max_covering(b"e", 15), 10);
        assert_eq!(f.max_covering(b"e", 5), 0);
        assert_eq!(f.max_covering(b"g", 100), 20);
        assert_eq!(f.max_covering(b"h", 100), 0);
        assert_eq!(f.max_covering(b"x", 100), 0);
        assert_eq!(f.next_newer(b"e", 5), Some(10));
        assert_eq!(f.next_newer(b"e", 10), Some(20));
        assert_eq!(f.next_newer(b"e", 20), None);
    }
}
