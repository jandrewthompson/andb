use crate::record::Record;
use std::fs::OpenOptions;
use std::fs::File;
use std::io::Read;
use std::io::Write;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;

pub struct LogStore {
    file: File,
    current_offset: u64,
}

impl LogStore {
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;

        let current_offset = file.metadata()?.len();

        Ok(LogStore {
            file,
            current_offset,
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

        let start_offset = self.current_offset;

        self.file.write_all(&bytes)?;

        self.current_offset += bytes.len() as u64;

        Ok(start_offset)
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
}

