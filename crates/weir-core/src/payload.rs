/// Opaque payload bytes. Type alias so the backing type is a one-line change
/// (e.g. to `bytes::Bytes` if mmap segment reads are added) without breaking callers.
/// All weir-core and weir-server APIs use `Payload`, never `Vec<u8>` directly for payload data.
pub type Payload = Vec<u8>;
