//! Internal keys: the user key followed by an 8-byte little-endian trailer
//! `(sequence << 8) | kind`, the layout of LevelDB's `dbformat.h`. Internal
//! keys sort by user key ascending, then by trailer descending, so the
//! newest version of a user key comes first.

use std::cmp::Ordering;

pub const KIND_DELETE: u8 = 0;
pub const KIND_PUT: u8 = 1;
/// Kind of a seek target: the largest kind, so `(k, s, KIND_SEEK)` sorts
/// before every entry of `k` with sequence `<= s`.
pub const KIND_SEEK: u8 = KIND_PUT;
/// Sequence numbers use the upper 56 bits of the trailer.
pub const MAX_SEQ: u64 = (1 << 56) - 1;

pub fn trailer(seq: u64, kind: u8) -> u64 {
    (seq << 8) | kind as u64
}

pub fn append_internal(dst: &mut Vec<u8>, user: &[u8], seq: u64, kind: u8) {
    dst.extend_from_slice(user);
    dst.extend_from_slice(&trailer(seq, kind).to_le_bytes());
}

pub fn make_internal(user: &[u8], seq: u64, kind: u8) -> Vec<u8> {
    let mut v = Vec::with_capacity(user.len() + 8);
    append_internal(&mut v, user, seq, kind);
    v
}

/// The user key of an internal key. Callers check `len() >= 8` on bytes read
/// from disk before calling.
pub fn user_key(ikey: &[u8]) -> &[u8] {
    &ikey[..ikey.len() - 8]
}

fn trailer_of(ikey: &[u8]) -> u64 {
    u64::from_le_bytes(ikey[ikey.len() - 8..].try_into().unwrap())
}

/// Sequence number and kind of an internal key.
pub fn parse_trailer(ikey: &[u8]) -> (u64, u8) {
    let t = trailer_of(ikey);
    (t >> 8, t as u8)
}

pub fn cmp_internal(a: &[u8], b: &[u8]) -> Ordering {
    match user_key(a).cmp(user_key(b)) {
        Ordering::Equal => trailer_of(b).cmp(&trailer_of(a)),
        o => o,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newest_version_sorts_first() {
        let a5 = make_internal(b"a", 5, KIND_PUT);
        let a9 = make_internal(b"a", 9, KIND_DELETE);
        let b1 = make_internal(b"b", 1, KIND_PUT);
        let ab1 = make_internal(b"ab", 1, KIND_PUT);
        assert_eq!(cmp_internal(&a9, &a5), Ordering::Less);
        assert_eq!(cmp_internal(&a5, &b1), Ordering::Less);
        assert_eq!(cmp_internal(&a5, &ab1), Ordering::Less);
        assert_eq!(cmp_internal(&ab1, &b1), Ordering::Less);
        let seek = make_internal(b"a", 7, KIND_SEEK);
        assert_eq!(cmp_internal(&a9, &seek), Ordering::Less);
        assert_eq!(cmp_internal(&seek, &a5), Ordering::Less);
        assert_eq!(parse_trailer(&a9), (9, KIND_DELETE));
        assert_eq!(user_key(&ab1), b"ab");
    }
}
