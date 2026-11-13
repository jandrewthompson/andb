use crate::record::Record;
use std::fs::OpenOptions;
use std::fs::File;
use std::io::Read;
use std::io::Write;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::mpsc::Sender;


struct WriteRequest {
    bytes: Vec<u8>,
    reply_tx: Sender<std::io::Result<u64>>,
}

pub struct LogStore {
    path: PathBuf,

    write_tx: Sender<WriteRequest>,

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

        let (write_tx, write_tx) = mpsc::channel::<WriteRequest>();

        Ok(LogStore {
            file,
            current_offset,
            index,
            is_syncing: false,
            pending_batch: Vec::new(),
            sync_generation: 0,
        })
    }


    pub fn append(&mut self, payload: &[u8]) -> std::io::Result<u64> {
        let length = payload.len() as u32;
        let checksum = 0; //temp static crc32
        
        let record = Record {
            checksum,
            length,
            payload: payload.to_vec(),
        };

        let bytes = record.serialize();

        let offset = self.current_offset + self.pending_batch.iter().map(|b| b.len() as u64).sum::<u64>();
        self.index.push(offset);

        self.pending_batch.push(bytes);

        if self.is_syncing {
            return Ok(offset);
        }

        self.is_syncing = true; // i am the captain now

        while !self.pending_batch.is_empty() {
            let batch = std::mem::take(&mut self.pending_batch);

            self.file.seek(SeekFrom::Start(self.current_offset))?;


            let mut batch_bytes = Vec::new();
            for record_bytes in batch {
                batch_bytes.extend(record_bytes);
            }

            self.file.write_all(&batch_bytes)?;
            self.current_offset += batch_bytes.len() as u64;

            self.file.sync_all()?;
        }

        self.is_syncing = false;
        Ok(offset)
    }


    pub fn read_all(&mut self) -> std::io::Result<Vec<Record>> {
        let mut records = Vec::new();

        self.file.seek(SeekFrom::Start(0))?;

        loop {
            let mut header_buf = [0u8; 8];

            match self.file.read_exact(&mut header_buf) {
                Ok(()) => {
                    let checksum = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
                    let length = u32::from_le_bytes(header_buf[4..8].try_into().unwrap());

                    let mut payload_buf = vec![0u8; length as usize];
                    self.file.read_exact(&mut payload_buf)?;

                    records.push(Record {
                        checksum,
                        length,
                        payload: payload_buf,
                    });
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break Ok(records);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    pub fn read_at(&mut self, record_index: usize) -> std::io::Result<Record> {
        let offset = self.index.get(record_index).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "Record not found")
        })?;

        self.file.seek(SeekFrom::Start(*offset))?;
            
        let mut header_buf = [0u8; 8];
        self.file.read_exact(&mut header_buf)?;

        let checksum = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
        let length = u32::from_le_bytes(header_buf[4..8].try_into().unwrap());

        let mut payload_buf = vec![0u8; length as usize];
        self.file.read_exact(&mut payload_buf)?;

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

        let mut store = LogStore::open(log_path).unwrap();
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

        let mut store = LogStore::open(log_path).unwrap();
        store.append(b"Zero").unwrap();
        store.append(b"One").unwrap();
        store.append(b"Two").unwrap();

        let rec = store.read_at(1).unwrap();
        assert_eq!(rec.payload, b"One");

        drop(store);

        let mut store_reopened = LogStore::open(log_path).unwrap();
        let rec_zero = store_reopened.read_at(0).unwrap();
        assert_eq!(rec_zero.payload, b"Zero");

        let _ = std::fs::remove_file(log_path);
    }
}

