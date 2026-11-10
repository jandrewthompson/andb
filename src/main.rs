use andb::LogStore;
use andb::Record;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data = "foóbar";

    let r = Record {
        checksum: 64,
        length: data.len() as u32,
        payload: "foo".as_bytes().to_vec(),
    };

    
    println!("{}, {}", r.checksum, r.length, );


    let mut log = LogStore::open("test.log")?;

    let offset1 = log.append(b"Hello, Rust Log!")?;
    println!("Wrote record 1 at offset: {}", offset1);
    let offset2 = log.append(b"Second record")?;
    println!("Wrote record 2 at offset: {}", offset2);
    let offset3 = log.append(b"Third and final record")?;
    println!("Wrote record 3 at offset: {}", offset3);

    let records = log.read_all()?;


    for (i, rec) in records.iter().enumerate() {
        let payload_str = String::from_utf8_lossy(&rec.payload);
        println!(" Record {}: checksum={}, length={}, payload='{}'",
         i, rec.checksum, rec.length, payload_str);
    }

    std::fs::remove_file("test.log")?;



    Ok(())


}
