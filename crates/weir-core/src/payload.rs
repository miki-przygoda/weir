/// Opaque payload bytes. Ref-counted `Bytes` so clones through the drain /
/// sink path are O(1) instead of heap copies. All weir-core and weir-server
/// APIs use `Payload`, never `Vec<u8>` directly for payload data.
pub type Payload = bytes::Bytes;
