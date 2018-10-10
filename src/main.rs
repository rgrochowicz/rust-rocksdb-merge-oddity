extern crate rocksdb;

use rocksdb::{ColumnFamilyOptions, DB, DBOptions, MergeOperands, Writable};
use std::fs;
use std::str;


fn str_to_u32(u: &[u8]) -> u32 {
    match str::from_utf8(u) {
        Ok(value) => value.parse().unwrap_or_default(),
        Err(_) => 0
    }
}

fn addition_merge(_new_key: &[u8], existing_val: Option<&[u8]>,
                  operands: &mut MergeOperands) -> Vec<u8> {
    let mut existing: u32 = match existing_val {
        Some(value) => str_to_u32(value),
        None => 0
    };

    for op in operands {
        let x = str_to_u32(op);
        existing = existing + x;
    }
    return existing.to_string().as_bytes().to_vec();
}

const TESTKEY: &[u8; 4] = b"test";

fn create_db() -> Result<u32, String> {
    let mut db_opts = DBOptions::new();
    db_opts.create_if_missing(true);
    let mut cf_opts = ColumnFamilyOptions::new();
    cf_opts.add_merge_operator("addition operator", addition_merge);
    let db = DB::open_cf(db_opts, "./testdb", vec![("default", cf_opts)])?;

    for _ in 0..50 {
        db.merge(TESTKEY, b"1")?;
    }

    let val = db.get(TESTKEY)?;

    match val {
        Some(value) => Ok(str_to_u32(value.as_ref())),
        None => Err("can't find value".to_string()),
    }
}

fn main() {
    println!("After first create: {:?} should be Ok(50)", create_db());
    println!("After second create: {:?} should be Ok(100)", create_db());
    fs::remove_dir_all("./testdb").expect("couldn't delete db");
}
