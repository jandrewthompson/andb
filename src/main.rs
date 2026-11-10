use std::fs::OpenOptions;
use std::fs::File;
use std::io::Write;
use std::path::Path;


pub struct Record {
    pub checksum: u32,
    pub length: u32,
    pub payload: Vec<u8>,
}

impl Record {
    pub fn serialize(&self) -> Vec<u8> {
    
        let mut bytes = Vec::with_capacity(4+4+self.payload.len());

        let checksum_bytes = self.checksum.to_le_bytes();
        let length_bytes = self.length.to_le_bytes();

        bytes.extend_from_slice(&checksum_bytes);
        bytes.extend_from_slice(&length_bytes);
        bytes.extend_from_slice(&self.payload);

        bytes

    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() < 8 {
            return Err("Invalid header!");
        }

        let checksum = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let length = u32::from_le_bytes(bytes[4..8].try_into().unwrap());

        let expected_size = 4 + 4 + length as usize;
        if bytes.len() < expected_size {
            return Err("Incomplete record");
        }

        let payload = bytes[8..expected_size].to_vec();

        Ok(Record {
            checksum,
            length,
            payload,
        })
    }
}

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
}

fn main() {
    let data = "foóbar";

    let r = Record {
        checksum: 64,
        length: data.len() as u32,
        payload: "foo".as_bytes().to_vec(),
    };


    println!("{}, {}", r.checksum, r.length, );




}
