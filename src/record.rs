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
