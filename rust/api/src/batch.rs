//! Write batches. A batch is an ordered list of operations across column
//! families that an engine applies atomically: readers see all of a committed
//! batch or none of it, and after a crash the batch survives whole or not at
//! all.

use crate::coding::{get_length_prefixed, get_varint32, put_length_prefixed, put_varint32};
use crate::error::{Error, Result};

/// A column family, numbered by its position in the name list given to
/// `open`.
pub type CfId = u32;

const TAG_PUT: u8 = 1;
const TAG_DELETE: u8 = 2;
const TAG_DELETE_RANGE: u8 = 3;
const TAG_MERGE: u8 = 4;

/// One operation of a batch, borrowing its bytes from the batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchOp<'a> {
    Put {
        cf: CfId,
        key: &'a [u8],
        value: &'a [u8],
    },
    Delete {
        cf: CfId,
        key: &'a [u8],
    },
    /// Deletes every key in `[start, end)`.
    DeleteRange {
        cf: CfId,
        start: &'a [u8],
        end: &'a [u8],
    },
    /// Adds `operand` to the stored value with the `i64add` operator.
    Merge {
        cf: CfId,
        key: &'a [u8],
        operand: &'a [u8],
    },
}

impl BatchOp<'_> {
    pub fn cf(&self) -> CfId {
        match *self {
            BatchOp::Put { cf, .. }
            | BatchOp::Delete { cf, .. }
            | BatchOp::DeleteRange { cf, .. }
            | BatchOp::Merge { cf, .. } => cf,
        }
    }
}

/// An encoded batch: `[tag u8][cf varint32][key][value]` per operation, where
/// key and value are varint32-length-prefixed and a delete has no value. The
/// LSM engine writes this encoding into its log unchanged.
#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct WriteBatch {
    rep: Vec<u8>,
    count: u32,
    merges: u32,
    ranges: u32,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(bytes: usize) -> Self {
        WriteBatch {
            rep: Vec::with_capacity(bytes),
            ..Self::default()
        }
    }

    pub fn put(&mut self, cf: CfId, key: &[u8], value: &[u8]) {
        self.rep.push(TAG_PUT);
        put_varint32(&mut self.rep, cf);
        put_length_prefixed(&mut self.rep, key);
        put_length_prefixed(&mut self.rep, value);
        self.count += 1;
    }

    pub fn delete(&mut self, cf: CfId, key: &[u8]) {
        self.rep.push(TAG_DELETE);
        put_varint32(&mut self.rep, cf);
        put_length_prefixed(&mut self.rep, key);
        self.count += 1;
    }

    /// Deletes `[start, end)`. `start == end` deletes nothing; `start > end`
    /// makes the write fail with `InvalidArgument`.
    pub fn delete_range(&mut self, cf: CfId, start: &[u8], end: &[u8]) {
        self.rep.push(TAG_DELETE_RANGE);
        put_varint32(&mut self.rep, cf);
        put_length_prefixed(&mut self.rep, start);
        put_length_prefixed(&mut self.rep, end);
        self.count += 1;
        self.ranges += 1;
    }

    /// Merges an 8-byte little-endian signed operand into `key` with the
    /// `i64add` operator (see [`i64add`]); the store's `nildb.i64add` on
    /// the `default` column family is the one user.
    pub fn merge(&mut self, cf: CfId, key: &[u8], operand: &[u8]) {
        self.rep.push(TAG_MERGE);
        put_varint32(&mut self.rep, cf);
        put_length_prefixed(&mut self.rep, key);
        put_length_prefixed(&mut self.rep, operand);
        self.count += 1;
        self.merges += 1;
    }

    pub fn len(&self) -> usize {
        self.count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn clear(&mut self) {
        self.rep.clear();
        self.count = 0;
        self.merges = 0;
        self.ranges = 0;
    }

    pub fn has_merges(&self) -> bool {
        self.merges > 0
    }

    pub fn has_ranges(&self) -> bool {
        self.ranges > 0
    }

    /// The encoded operations.
    pub fn data(&self) -> &[u8] {
        &self.rep
    }

    /// Size of the encoding in bytes.
    pub fn size(&self) -> usize {
        self.rep.len()
    }

    /// Rebuilds a batch from `data()` output, checking every operation.
    pub fn from_data(data: &[u8]) -> Result<WriteBatch> {
        let mut b = WriteBatch {
            rep: data.to_vec(),
            ..Self::default()
        };
        let mut pos = 0;
        while pos < data.len() {
            let (op, n) = decode_op(&data[pos..])?;
            pos += n;
            b.count += 1;
            match op {
                BatchOp::Merge { .. } => b.merges += 1,
                BatchOp::DeleteRange { .. } => b.ranges += 1,
                _ => {}
            }
        }
        Ok(b)
    }

    pub fn iter(&self) -> BatchIter<'_> {
        BatchIter { rest: &self.rep }
    }

    /// Appends one decoded operation, for engines that rewrite a batch.
    pub fn push(&mut self, op: BatchOp<'_>) {
        match op {
            BatchOp::Put { cf, key, value } => self.put(cf, key, value),
            BatchOp::Delete { cf, key } => self.delete(cf, key),
            BatchOp::DeleteRange { cf, start, end } => self.delete_range(cf, start, end),
            BatchOp::Merge { cf, key, operand } => self.merge(cf, key, operand),
        }
    }

    /// Checks column family ids, key and value lengths, range order and
    /// merge operand sizes before an engine applies the batch.
    pub fn validate(&self, num_cfs: usize, max_key_len: usize, max_value_len: usize) -> Result<()> {
        for op in self.iter() {
            let cf = op.cf() as usize;
            if cf >= num_cfs {
                return Err(Error::invalid(format!(
                    "column family {cf} does not exist ({num_cfs} are open)"
                )));
            }
            let key_ok = |k: &[u8]| {
                if k.len() > max_key_len {
                    Err(Error::invalid(format!(
                        "key of {} bytes exceeds this engine's limit of {max_key_len}",
                        k.len()
                    )))
                } else {
                    Ok(())
                }
            };
            match op {
                BatchOp::Put { key, value, .. } => {
                    key_ok(key)?;
                    if value.len() > max_value_len {
                        return Err(Error::invalid(format!(
                            "value of {} bytes exceeds this engine's limit of {max_value_len}",
                            value.len()
                        )));
                    }
                }
                BatchOp::Delete { key, .. } => key_ok(key)?,
                BatchOp::DeleteRange { start, end, .. } => {
                    key_ok(start)?;
                    key_ok(end)?;
                    if start > end {
                        return Err(Error::invalid(
                            "delete-range start key sorts after its end key",
                        ));
                    }
                }
                BatchOp::Merge { key, operand, .. } => {
                    key_ok(key)?;
                    if operand.len() != 8 {
                        return Err(Error::invalid(format!(
                            "i64add operand of {} bytes, want 8",
                            operand.len()
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

pub struct BatchIter<'a> {
    rest: &'a [u8],
}

impl<'a> Iterator for BatchIter<'a> {
    type Item = BatchOp<'a>;

    fn next(&mut self) -> Option<BatchOp<'a>> {
        if self.rest.is_empty() {
            return None;
        }
        // The encoding was produced by WriteBatch's own methods or checked
        // by from_data, so decoding cannot fail here.
        let (op, n) = decode_op(self.rest).expect("WriteBatch holds a valid encoding");
        self.rest = &self.rest[n..];
        Some(op)
    }
}

fn decode_op(src: &[u8]) -> Result<(BatchOp<'_>, usize)> {
    let tag = *src
        .first()
        .ok_or_else(|| Error::corruption("empty batch operation"))?;
    let mut pos = 1;
    let (cf, n) = get_varint32(&src[pos..])?;
    pos += n;
    let (a, n) = get_length_prefixed(&src[pos..])?;
    pos += n;
    if tag == TAG_DELETE {
        return Ok((BatchOp::Delete { cf, key: a }, pos));
    }
    let (b, n) = get_length_prefixed(&src[pos..])?;
    pos += n;
    let op = match tag {
        TAG_PUT => BatchOp::Put {
            cf,
            key: a,
            value: b,
        },
        TAG_DELETE_RANGE => BatchOp::DeleteRange {
            cf,
            start: a,
            end: b,
        },
        TAG_MERGE => BatchOp::Merge {
            cf,
            key: a,
            operand: b,
        },
        t => {
            return Err(Error::corruption(format!(
                "unknown batch operation tag {t}"
            )));
        }
    };
    Ok((op, pos))
}

/// The `i64add` merge operator of `internal/store/merge.go`: the stored value
/// and the operand are 8-byte little-endian signed integers and the result is
/// their sum with two's-complement wraparound. A missing or empty stored value
/// counts as zero. Both engines apply it at commit time, under the writer
/// lock, and store the sum as a plain put.
pub fn i64add(existing: Option<&[u8]>, operand: &[u8]) -> Result<Vec<u8>> {
    let op: [u8; 8] = operand.try_into().map_err(|_| {
        Error::invalid(format!("i64add operand of {} bytes, want 8", operand.len()))
    })?;
    let base = match existing {
        None | Some([]) => 0i64,
        Some(v) => {
            let b: [u8; 8] = (*v).try_into().map_err(|_| {
                Error::corruption(format!("i64add stored value of {} bytes, want 8", v.len()))
            })?;
            i64::from_le_bytes(b)
        }
    };
    Ok(base
        .wrapping_add(i64::from_le_bytes(op))
        .to_le_bytes()
        .to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trip() {
        let mut b = WriteBatch::new();
        b.put(0, b"k1", b"v1");
        b.delete(3, b"");
        b.delete_range(1, b"a", b"z");
        b.merge(0, b"n", &5i64.to_le_bytes());
        let c = WriteBatch::from_data(b.data()).unwrap();
        assert_eq!(b, c);
        assert_eq!(c.len(), 4);
        assert!(c.has_merges() && c.has_ranges());
        let ops: Vec<_> = c.iter().collect();
        assert_eq!(
            ops[1],
            BatchOp::Delete {
                cf: 3,
                key: b"".as_slice()
            }
        );
        assert!(WriteBatch::from_data(&b.data()[..b.size() - 1]).is_err());
    }

    #[test]
    fn validate_rejects_bad_input() {
        let mut b = WriteBatch::new();
        b.put(2, b"k", b"v");
        assert!(b.validate(2, 100, 100).is_err());
        assert!(b.validate(3, 100, 100).is_ok());
        let mut r = WriteBatch::new();
        r.delete_range(0, b"b", b"a");
        assert!(r.validate(1, 100, 100).is_err());
        let mut m = WriteBatch::new();
        m.merge(0, b"k", b"123");
        assert!(m.validate(1, 100, 100).is_err());
    }

    #[test]
    fn i64add_matches_store_semantics() {
        let seven = 7i64.to_le_bytes();
        assert_eq!(i64add(None, &seven).unwrap(), seven.to_vec());
        assert_eq!(i64add(Some(&[]), &seven).unwrap(), seven.to_vec());
        let max = i64::MAX.to_le_bytes();
        assert_eq!(
            i64add(Some(&max), &1i64.to_le_bytes()).unwrap(),
            i64::MIN.to_le_bytes()
        );
        assert!(i64add(Some(b"abc"), &seven).is_err());
        assert!(i64add(None, b"abc").is_err());
    }
}
