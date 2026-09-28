//! Bloom filters (Bloom, "Space/Time Trade-offs in Hash Coding with Allowable
//! Errors", CACM 1970): one per table file over its distinct user keys,
//! probed with double hashing as LevelDB's `util/bloom.cc` does. The last
//! byte of a filter holds the number of probes.

use nilengine_api::coding::hash64;

pub fn key_hash(user_key: &[u8]) -> u64 {
    hash64(user_key, 0xbc9f_1d34)
}

pub fn build(hashes: &[u64], bits_per_key: usize) -> Vec<u8> {
    if bits_per_key == 0 {
        return Vec::new();
    }
    let k = ((bits_per_key as f64 * 0.69) as usize).clamp(1, 30);
    let bytes = (hashes.len() * bits_per_key).max(64).div_ceil(8);
    let bits = (bytes * 8) as u64;
    let mut f = vec![0u8; bytes + 1];
    f[bytes] = k as u8;
    for &h in hashes {
        let delta = h.rotate_right(33) | 1;
        let mut x = h;
        for _ in 0..k {
            let b = (x % bits) as usize;
            f[b >> 3] |= 1 << (b & 7);
            x = x.wrapping_add(delta);
        }
    }
    f
}

/// False means the key is certainly absent from the file.
pub fn may_contain(filter: &[u8], h: u64) -> bool {
    if filter.len() < 2 {
        return true;
    }
    let bytes = filter.len() - 1;
    let k = filter[bytes] as usize;
    if k == 0 || k > 30 {
        return true;
    }
    let bits = (bytes * 8) as u64;
    let delta = h.rotate_right(33) | 1;
    let mut x = h;
    for _ in 0..k {
        let b = (x % bits) as usize;
        if filter[b >> 3] & (1 << (b & 7)) == 0 {
            return false;
        }
        x = x.wrapping_add(delta);
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives_and_few_false_positives() {
        let keys: Vec<Vec<u8>> = (0..10_000)
            .map(|i| format!("key{i}").into_bytes())
            .collect();
        let hashes: Vec<u64> = keys.iter().map(|k| key_hash(k)).collect();
        let f = build(&hashes, 10);
        assert!(hashes.iter().all(|&h| may_contain(&f, h)));
        let fp = (0..10_000)
            .filter(|i| may_contain(&f, key_hash(format!("other{i}").as_bytes())))
            .count();
        assert!(fp < 300, "{fp} false positives in 10000 probes");
        assert!(may_contain(&build(&hashes, 0), 1));
    }
}
