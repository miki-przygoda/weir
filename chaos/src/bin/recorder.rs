//! The recording sink — the oracle's delivery log.
//!
//! weir posts committed batches here as NDJSON; this appends each record's
//! identity to a log and **fsyncs before returning 200**.
//!
//! The fsync is not incidental — it is the `Sink` contract. weir treats a 200
//! as "durably committed downstream" and becomes free to reclaim the segment.
//! A recorder that buffered in memory and lost records on its own crash would
//! manufacture false durability violations. The oracle must be at least as
//! durable as the thing it judges.
//!
//! Hand-rolled HTTP over `TcpListener`: the surface is one POST route, and a
//! framework dependency would buy nothing while adding a supply chain to a
//! component whose whole job is to be trustworthy.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;

/// Append-only log of delivered record identities, fsynced before each ack.
struct DeliveryLog {
    file: File,
}

impl DeliveryLog {
    fn create(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { file })
    }

    /// Appends every decodable record in an NDJSON body. Returns the count
    /// written. Undecodable lines are skipped rather than failing the batch:
    /// weir would retry the whole segment, and a permanently-undecodable
    /// record would wedge the drain forever.
    fn append_ndjson(&mut self, body: &[u8]) -> std::io::Result<usize> {
        let mut out = String::new();
        let mut count = 0usize;
        for line in body.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let Ok(text) = std::str::from_utf8(line) else {
                continue;
            };
            let Some(rec) = weir_chaos::decode_record(text) else {
                continue;
            };
            out.push_str(&format!("{} {}\n", rec.run_id, rec.seq));
            count += 1;
        }
        if !out.is_empty() {
            self.file.write_all(out.as_bytes())?;
            // Durable BEFORE the 200. See the module docs.
            self.file.sync_data()?;
        }
        Ok(count)
    }
}

/// Extracts `Content-Length` from an HTTP request head. Case-insensitive.
fn content_length(head: &str) -> Option<usize> {
    head.lines()
        .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
        .and_then(|l| l.split(':').nth(1))
        .and_then(|v| v.trim().parse().ok())
}

fn handle(stream: &mut TcpStream, log: &mut DeliveryLog) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut head = String::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            return Ok(());
        }
        let blank = line == "\r\n" || line == "\n";
        head.push_str(&line);
        if blank {
            break;
        }
    }

    // A HEAD probe is how the HTTP sink's health check works; answer it 200.
    if head.starts_with("HEAD") {
        stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")?;
        return Ok(());
    }

    let len = content_length(&head).unwrap_or(0);
    let mut body = vec![0u8; len];
    reader.read_exact(&mut body)?;
    log.append_ndjson(&body)?;
    stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")?;
    stream.flush()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut bind = "127.0.0.1:9900".to_string();
    let mut log_path = String::new();
    let mut i = 1;
    while i + 1 < args.len() {
        match args[i].as_str() {
            "--bind" => bind = args[i + 1].clone(),
            "--log" => log_path = args[i + 1].clone(),
            _ => {}
        }
        i += 2;
    }
    if log_path.is_empty() {
        eprintln!("usage: recorder --bind <addr> --log <path>");
        std::process::exit(2);
    }

    let mut log = DeliveryLog::create(Path::new(&log_path)).expect("open delivery log");
    let listener = TcpListener::bind(&bind).expect("bind recorder");
    eprintln!("recorder listening on {bind}, log at {log_path}");

    for stream in listener.incoming() {
        match stream {
            Ok(mut s) => {
                if let Err(e) = handle(&mut s, &mut log) {
                    eprintln!("recorder: request failed: {e}");
                }
            }
            Err(e) => eprintln!("recorder: accept failed: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn appends_one_line_per_ndjson_record() {
        let dir = std::env::temp_dir().join(format!("chaos-rec-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let log_path = dir.join("delivered.log");
        let mut log = DeliveryLog::create(&log_path).unwrap();

        let body = format!(
            "{}\n{}\n",
            weir_chaos::encode_record(3, 1, 64),
            weir_chaos::encode_record(3, 2, 64)
        );
        let written = log.append_ndjson(body.as_bytes()).unwrap();
        assert_eq!(written, 2);

        let mut contents = String::new();
        std::fs::File::open(&log_path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();
        assert_eq!(contents, "3 1\n3 2\n");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn skips_undecodable_lines_without_failing_the_batch() {
        let dir = std::env::temp_dir().join(format!("chaos-rec-junk-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let log_path = dir.join("delivered.log");
        let mut log = DeliveryLog::create(&log_path).unwrap();

        let body = format!("junk\n{}\n\n", weir_chaos::encode_record(4, 9, 64));
        let written = log.append_ndjson(body.as_bytes()).unwrap();
        assert_eq!(written, 1, "only the decodable record counts");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn parses_content_length_from_a_request_head() {
        let head = "POST /ingest HTTP/1.1\r\nHost: x\r\nContent-Length: 42\r\n\r\n";
        assert_eq!(content_length(head), Some(42));
        assert_eq!(content_length("POST / HTTP/1.1\r\n\r\n"), None);
    }
}
