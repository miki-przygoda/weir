//! The opaque payload byte buffer carried end-to-end through weir.

use std::ops::Deref;

use bytes::Bytes;

/// Opaque payload bytes — a newtype over ref-counted [`bytes::Bytes`] so clones
/// through the drain / sink path are O(1) instead of heap copies.
///
/// `Payload` *wraps* `Bytes` rather than aliasing it so weir's 1.0 public API
/// does not leak the `bytes` crate's semver: a future `bytes 2.0` would
/// otherwise be a breaking change to weir's own 1.0. It derefs to `[u8]`, so
/// slicing, indexing, iteration, `len()`, and `&payload` → `&[u8]` coercion all
/// work transparently. `Debug` prints only the length — never the bytes — so a
/// stray `debug!(?payload)` cannot leak (possibly sensitive) record contents.
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct Payload(Bytes);

impl Payload {
    /// An empty payload.
    pub fn new() -> Self {
        Payload(Bytes::new())
    }

    /// Wraps a `'static` byte slice without copying (O(1)).
    pub fn from_static(bytes: &'static [u8]) -> Self {
        Payload(Bytes::from_static(bytes))
    }

    /// Copies a borrowed slice into a new ref-counted buffer.
    pub fn copy_from_slice(data: &[u8]) -> Self {
        Payload(Bytes::copy_from_slice(data))
    }

    /// Borrows the payload as a byte slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the payload, returning the inner [`Bytes`] (the escape hatch for
    /// code that genuinely needs the `bytes` type).
    pub fn into_bytes(self) -> Bytes {
        self.0
    }
}

impl Deref for Payload {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::borrow::Borrow<[u8]> for Payload {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Length only — the bytes may be sensitive record data and must never
        // land in a log line. This is the F2 footgun fix.
        write!(f, "Payload({} bytes)", self.0.len())
    }
}

impl From<Bytes> for Payload {
    fn from(b: Bytes) -> Self {
        Payload(b)
    }
}

impl From<Payload> for Bytes {
    fn from(p: Payload) -> Self {
        p.0
    }
}

impl From<Vec<u8>> for Payload {
    fn from(v: Vec<u8>) -> Self {
        Payload(Bytes::from(v))
    }
}

impl From<&[u8]> for Payload {
    fn from(s: &[u8]) -> Self {
        Payload(Bytes::copy_from_slice(s))
    }
}

impl PartialEq<[u8]> for Payload {
    fn eq(&self, other: &[u8]) -> bool {
        self.0.as_ref() == other
    }
}

impl PartialEq<Vec<u8>> for Payload {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.0.as_ref() == other.as_slice()
    }
}

impl PartialEq<&[u8]> for Payload {
    fn eq(&self, other: &&[u8]) -> bool {
        self.0.as_ref() == *other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_static_is_byte_identical() {
        let p = Payload::from_static(b"hello");
        assert_eq!(&p[..], b"hello");
        assert_eq!(p.len(), 5);
        assert!(!p.is_empty());
    }

    #[test]
    fn from_vec_and_slice_round_trip() {
        assert_eq!(&Payload::from(vec![1u8, 2, 3])[..], &[1, 2, 3]);
        assert_eq!(&Payload::from(&[4u8, 5][..])[..], &[4, 5]);
    }

    #[test]
    fn new_and_default_are_empty() {
        assert!(Payload::new().is_empty());
        assert!(Payload::default().is_empty());
    }

    #[test]
    fn clone_shares_buffer_and_compares_equal() {
        let a = Payload::from(vec![9u8; 32]);
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn debug_prints_length_not_contents() {
        let p = Payload::from_static(b"super-secret-token");
        let dbg = format!("{p:?}");
        assert!(!dbg.contains("secret"), "Debug leaked payload bytes: {dbg}");
        assert_eq!(dbg, "Payload(18 bytes)");
    }

    #[test]
    fn bytes_round_trip_is_zero_copy_identity() {
        let b = Bytes::from_static(b"abc");
        let p = Payload::from(b.clone());
        assert_eq!(Bytes::from(p), b);
    }

    // ── F51: PartialEq impls + Borrow/Hash contract ───────────────────────────

    #[test]
    fn partial_eq_against_slice_vec_and_ref_slice() {
        let p = Payload::from_static(b"abc");
        // PartialEq<[u8]>
        assert_eq!(p, *b"abc".as_slice());
        assert_ne!(p, *b"abd".as_slice());
        // PartialEq<Vec<u8>>
        assert_eq!(p, b"abc".to_vec());
        assert_ne!(p, b"ab".to_vec());
        // PartialEq<&[u8]>
        let s: &[u8] = b"abc";
        assert_eq!(p, s);
        let s2: &[u8] = b"abcd";
        assert_ne!(p, s2);
    }

    /// The `Borrow<[u8]>` + derived `Hash` (over the inner `Bytes`) must agree
    /// with hashing the borrowed `&[u8]`, or `Payload` would be unusable as a
    /// `HashMap`/`HashSet` key looked up by slice. This pins the Borrow/Hash
    /// contract that the derive + Borrow impl quietly rely on.
    #[test]
    fn borrow_and_hash_agree_with_slice() {
        use std::borrow::Borrow;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_of<T: Hash + ?Sized>(t: &T) -> u64 {
            let mut h = DefaultHasher::new();
            t.hash(&mut h);
            h.finish()
        }

        let p = Payload::from_static(b"key-bytes");
        let slice: &[u8] = b"key-bytes";

        // Borrow exposes the same bytes…
        let borrowed: &[u8] = p.borrow();
        assert_eq!(borrowed, slice);
        // …and Hash(Payload) must equal Hash(&[u8]) for the Borrow contract.
        assert_eq!(
            hash_of(&p),
            hash_of(slice),
            "Hash(Payload) must match Hash(&[u8]) for Borrow<[u8]> lookups"
        );
    }

    /// End-to-end Borrow demonstration: a HashSet<Payload> can be queried by a
    /// plain &[u8] (the whole point of the Borrow<[u8]> impl).
    #[test]
    fn hashset_keyed_by_payload_is_queryable_by_slice() {
        use std::collections::HashSet;
        let mut set: HashSet<Payload> = HashSet::new();
        set.insert(Payload::from_static(b"present"));
        assert!(set.contains(b"present".as_slice()));
        assert!(!set.contains(b"absent".as_slice()));
    }
}
