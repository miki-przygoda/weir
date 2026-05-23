pub mod format;
pub mod recovery;
pub mod segment;

use std::{
    fs::File,
    io::{self, BufReader, Read},
    path::Path,
};

use format::{FORMAT_VERSION, SEGMENT_HEADER_LEN};
use weir_core::{Payload, MAX_PAYLOAD_HARD_CAP};

/// An iterator over records in a sealed WAB segment file.
///
/// Streams records without materialising the whole segment. Applies
/// `MAX_PAYLOAD_HARD_CAP` before every heap allocation to bound memory usage
/// during recovery. Stops at the end-of-records sentinel or on the first error.
pub struct SegmentReader {
    reader: BufReader<File>,
    done: bool,
}

impl SegmentReader {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut header = [0u8; SEGMENT_HEADER_LEN];
        reader.read_exact(&mut header)?;

        if &header[0..4] != b"WEIR" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad segment magic: {:?}", &header[0..4]),
            ));
        }
        if header[4] != FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown segment format version: {}", header[4]),
            ));
        }

        Ok(SegmentReader { reader, done: false })
    }
}

impl Iterator for SegmentReader {
    type Item = io::Result<Payload>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return None,
            Err(e) => { self.done = true; return Some(Err(e)); }
        }

        let payload_len = u32::from_le_bytes(len_buf) as usize;
        if payload_len == 0 {
            self.done = true;
            return None; // sentinel
        }

        // Cap check before allocation — MAX_PAYLOAD_HARD_CAP from weir-core.
        if payload_len > MAX_PAYLOAD_HARD_CAP {
            self.done = true;
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("record payload_len {payload_len} exceeds MAX_PAYLOAD_HARD_CAP {MAX_PAYLOAD_HARD_CAP}"),
            )));
        }

        let mut crc_buf = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut crc_buf) {
            self.done = true;
            return Some(Err(e));
        }
        let expected_crc = u32::from_le_bytes(crc_buf);

        let mut payload = vec![0u8; payload_len];
        if let Err(e) = self.reader.read_exact(&mut payload) {
            self.done = true;
            return Some(Err(e));
        }

        let computed_crc = crc32fast::hash(&payload);
        if expected_crc != computed_crc {
            self.done = true;
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "record CRC mismatch: expected {expected_crc:#010x}, computed {computed_crc:#010x}"
                ),
            )));
        }

        Some(Ok(payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wab::segment::{segment_path, WabSegment};
    use std::fs;

    fn tmp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("weir_wab_{label}_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    use std::path::PathBuf;

    #[test]
    fn segment_reader_round_trip() {
        let dir = tmp_dir("rdroundtrip");
        let path = segment_path(&dir, 1);
        let payloads: Vec<&[u8]> = vec![b"alpha", b"beta", b"gamma delta"];
        let mut seg = WabSegment::create(&path, 0).unwrap();
        for p in &payloads {
            seg.write_record(p).unwrap();
        }
        let sealed = seg.seal().unwrap();

        let got: Vec<Vec<u8>> = SegmentReader::open(&sealed)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(got, payloads);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn segment_reader_detects_crc_mismatch() {
        let dir = tmp_dir("rdcrc");
        let path = segment_path(&dir, 1);
        let mut seg = WabSegment::create(&path, 0).unwrap();
        seg.write_record(b"data").unwrap();
        let sealed = seg.seal().unwrap();

        // Flip a bit in the payload bytes.
        // Layout: 24 header + 4 payload_len + 4 crc = offset 32 is start of payload.
        let mut bytes = fs::read(&sealed).unwrap();
        bytes[32] ^= 0xff;
        fs::write(&sealed, &bytes).unwrap();

        let mut reader = SegmentReader::open(&sealed).unwrap();
        assert!(reader.next().unwrap().is_err());
        fs::remove_dir_all(dir).ok();
    }
}
