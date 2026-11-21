// Parquet writing for both output tables (snappy, DATE-typed day, UTF8 strings).

use std::fs;
use std::path::Path;
use std::sync::Arc;

use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

use crate::pipeline::UDRow;

const RG: usize = 262_144;

fn open(file: fs::File, msg: &str) -> SerializedFileWriter<fs::File> {
    let tpe = parse_message_type(msg).expect("schema");
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    SerializedFileWriter::new(file, Arc::new(tpe), Arc::new(props)).expect("parquet writer")
}

macro_rules! col {
    ($rg:expr, $ty:ty, $vals:expr) => {{
        let mut cw = $rg.next_column().unwrap().unwrap();
        cw.typed::<$ty>().write_batch($vals, None, None).unwrap();
        cw.close().unwrap();
    }};
}
macro_rules! col_opt {
    ($rg:expr, $ty:ty, $vals:expr, $defs:expr) => {{
        let mut cw = $rg.next_column().unwrap().unwrap();
        cw.typed::<$ty>().write_batch($vals, Some($defs), None).unwrap();
        cw.close().unwrap();
    }};
}

pub fn write_user_daily(
    path: &Path,
    rows: &[UDRow],
    names: &[Box<[u8]>],
    name_of_root: &[u32],
    cats: &[Box<[u8]>],
) {
    let msg = "
        message user_daily {
            REQUIRED BYTE_ARRAY user_id (UTF8);
            REQUIRED INT32 day (DATE);
            REQUIRED INT64 sessions;
            REQUIRED INT64 events;
            REQUIRED INT64 revenue_cents;
            OPTIONAL BYTE_ARRAY top_category (UTF8);
        }";
    let file = fs::File::create(path).expect("create parquet");
    let mut w = open(file, msg);
    let mut uid: Vec<ByteArray> = Vec::with_capacity(RG);
    let mut day: Vec<i32> = Vec::with_capacity(RG);
    let mut sess: Vec<i64> = Vec::with_capacity(RG);
    let mut evs: Vec<i64> = Vec::with_capacity(RG);
    let mut rev: Vec<i64> = Vec::with_capacity(RG);
    let mut top: Vec<ByteArray> = Vec::with_capacity(RG);
    let mut top_def: Vec<i16> = Vec::with_capacity(RG);
    for chunk in rows.chunks(RG) {
        uid.clear();
        day.clear();
        sess.clear();
        evs.clear();
        rev.clear();
        top.clear();
        top_def.clear();
        for r in chunk {
            uid.push(ByteArray::from(
                names[name_of_root[r.root as usize] as usize].to_vec(),
            ));
            day.push(r.day);
            sess.push(r.sessions);
            evs.push(r.events);
            rev.push(r.revenue);
            if r.top >= 0 {
                top_def.push(1);
                top.push(ByteArray::from(cats[r.top as usize].to_vec()));
            } else {
                top_def.push(0);
            }
        }
        let mut rg = w.next_row_group().expect("row group");
        col!(rg, ByteArrayType, &uid);
        col!(rg, Int32Type, &day);
        col!(rg, Int64Type, &sess);
        col!(rg, Int64Type, &evs);
        col!(rg, Int64Type, &rev);
        col_opt!(rg, ByteArrayType, &top, &top_def);
        rg.close().unwrap();
    }
    w.close().unwrap();
}

pub struct CampRow {
    pub id: String,
    pub channel: String,
    pub day: i32,
    pub sessions: i64,
    pub purchasing: i64,
    pub revenue: i64,
    pub assisted: i64,
}

pub fn write_campaign_daily(path: &Path, rows: &[CampRow]) {
    let msg = "
        message campaign_daily {
            REQUIRED BYTE_ARRAY campaign_id (UTF8);
            REQUIRED BYTE_ARRAY channel (UTF8);
            REQUIRED INT32 day (DATE);
            REQUIRED INT64 sessions;
            REQUIRED INT64 purchasing_sessions;
            REQUIRED INT64 revenue_cents;
            REQUIRED INT64 assisted_sessions;
        }";
    let file = fs::File::create(path).expect("create parquet");
    let mut w = open(file, msg);
    let mut cid: Vec<ByteArray> = Vec::with_capacity(RG);
    let mut chan: Vec<ByteArray> = Vec::with_capacity(RG);
    let mut day: Vec<i32> = Vec::with_capacity(RG);
    let mut sess: Vec<i64> = Vec::with_capacity(RG);
    let mut pur: Vec<i64> = Vec::with_capacity(RG);
    let mut rev: Vec<i64> = Vec::with_capacity(RG);
    let mut ass: Vec<i64> = Vec::with_capacity(RG);
    for chunk in rows.chunks(RG) {
        cid.clear();
        chan.clear();
        day.clear();
        sess.clear();
        pur.clear();
        rev.clear();
        ass.clear();
        for r in chunk {
            cid.push(ByteArray::from(r.id.as_bytes().to_vec()));
            chan.push(ByteArray::from(r.channel.as_bytes().to_vec()));
            day.push(r.day);
            sess.push(r.sessions);
            pur.push(r.purchasing);
            rev.push(r.revenue);
            ass.push(r.assisted);
        }
        let mut rg = w.next_row_group().expect("row group");
        col!(rg, ByteArrayType, &cid);
        col!(rg, ByteArrayType, &chan);
        col!(rg, Int32Type, &day);
        col!(rg, Int64Type, &sess);
        col!(rg, Int64Type, &pur);
        col!(rg, Int64Type, &rev);
        col!(rg, Int64Type, &ass);
        rg.close().unwrap();
    }
    w.close().unwrap();
}
