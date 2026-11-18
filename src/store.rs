use crate::record::Record;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::mpsc::{self, Sender};

struct WriteRequest {
    bytes: Vec<u8>,
    reply_tx: Sender<std::io::Result<u64>>,
}

pub struct LogStore {
    path: PathBuf,

    request_tx: Sender<WriteRequest>,

    index: Arc<RwLock<Vec<u64>>>,
}

impl LogStore {
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {

        let path_buf = path.as_ref().to_path_buf();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&path_buf)?;

        let mut initial_index = Vec::new();

        file.seek(SeekFrom::Start(0))?;

        loop {
            let current_pos = match file.stream_position() {
                Ok(pos) => pos,
                Err(_) => break,
            };

            let mut header_buf = [0u8; 8];
            match file.read_exact(&mut header_buf) {
                Ok(()) => {
                    initial_index.push(current_pos);
                    let length = u32::from_le_bytes(header_buf[4..8].try_into().unwrap());
                    file.seek(SeekFrom::Current(length as i64))?;
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => return Err(e),
            }
        }


        let mut current_offset = file.stream_position()?;

        let index = Arc::new(RwLock::new(initial_index));
        let flusher_index = Arc::clone(&index);

        let (request_tx, request_rx) = mpsc::channel::<WriteRequest>();

        std::thread::spawn(move || {
            while let Ok(first_req) = request_rx.recv() {
                //1. start a batch with the first request that woke us up
                let mut batch = vec![first_req];

                //2. drain any other requests that arrived concurrently
                while let Ok(req) = request_rx.try_recv() {
                    batch.push(req);
                }

                //3. serialize all records in the batch into one contiguous buffer
                let mut combined_bytes = Vec::new();
                let mut offsets = Vec::with_capacity(batch.len());

                for req in &batch {
                    let rec_offset = current_offset + combined_bytes.len() as u64;
                    offsets.push(rec_offset);
                    combined_bytes.extend_from_slice(&req.bytes);
                }

                //4. perform one single write and one single fsync for the batch
                let write_result = file
                    .seek(SeekFrom::Start(current_offset))
                    .and_then(|_| file.write_all(&combined_bytes))
                    .and_then(|_| file.sync_all());

                match write_result {
                    Ok(()) => {
                        current_offset += combined_bytes.len() as u64;
                        {
                            let mut idx_guard = flusher_index.write().unwrap();
                            idx_guard.extend_from_slice(&offsets);
                        } 
                        for (req, offset) in batch.into_iter().zip(offsets) {
                            let _ = req.reply_tx.send(Ok(offset));
                        }
                    }
                    Err(e) => {
                        let kind = e.kind();
                        let msg = e.to_string();
                        for req in batch {
                            let _ = req.reply_tx.send(Err(std::io::Error::new(kind, msg.clone())));
                        }
                    }
                }
            }

        });


        Ok(LogStore {
            path: path_buf,
            request_tx,
            index,
        })
    }


    pub fn append(&self, payload: &[u8]) -> std::io::Result<u64> {
        let length = payload.len() as u32;
        let checksum = 0; //temp static crc32
        
        let record = Record {
            checksum,
            length,
            payload: payload.to_vec(),
        };

        let bytes = record.serialize();

        let (reply_tx, reply_rx) = mpsc::channel();
        let req = WriteRequest { bytes, reply_tx };

        self.request_tx.send(req).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Flusher thread stopped")
        })?;

        reply_rx.recv().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Flusher dropped reply")
        })?
    }


    pub fn read_all(&self) -> std::io::Result<Vec<Record>> {
        let mut file = File::open(&self.path)?;
        let mut records = Vec::new();

        loop {
            let mut header_buf = [0u8; 8];
            match file.read_exact(&mut header_buf) {
                Ok(()) => {
                    let checksum = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
                    let length = u32::from_le_bytes(header_buf[4..8].try_into().unwrap());

                    let mut payload_buf = vec![0u8; length as usize];
                    file.read_exact(&mut payload_buf)?;

                    records.push(Record {
                        checksum,
                        length,
                        payload: payload_buf,
                    });
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break Ok(records);
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub fn read_at(&self, record_index: usize) -> std::io::Result<Record> {
        let offset = {
            let idx_guard = self.index.read().unwrap();
            *idx_guard.get(record_index).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "Record not found")
            })?
        };

        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(offset))?;

        let mut header_buf = [0u8; 8];
        file.read_exact(&mut header_buf)?;

        let checksum = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
        let length = u32::from_le_bytes(header_buf[4..8].try_into().unwrap());

        let mut payload_buf = vec![0u8; length as usize];
        file.read_exact(&mut payload_buf)?;

        Ok(Record {
            checksum,
            length,
            payload: payload_buf,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_append_and_read_roundtrip() {
        let log_path = "test_store_unit.log";

        if Path::new(log_path).exists() {
            let _ = std::fs::remove_file(log_path);
        }

        let store = LogStore::open(log_path).unwrap();
        let payload = b"Unit test payload";

        let offset = store.append(payload).unwrap();
        assert_eq!(offset, 0);

        let records = store.read_all().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].payload, payload);

        let _ = std::fs::remove_file(log_path);
    }


    #[test]
    fn test_random_access_and_recovery() {
        let log_path = "test_index.log";

        if Path::new(log_path).exists() {
            let _ = std::fs::remove_file(log_path);
        }

        let store = LogStore::open(log_path).unwrap();
        store.append(b"Zero").unwrap();
        store.append(b"One").unwrap();
        store.append(b"Two").unwrap();

        let rec = store.read_at(1).unwrap();
        assert_eq!(rec.payload, b"One");

        drop(store);

        let store_reopened = LogStore::open(log_path).unwrap();
        let rec_zero = store_reopened.read_at(0).unwrap();
        assert_eq!(rec_zero.payload, b"Zero");

        let _ = std::fs::remove_file(log_path);
    }
}

