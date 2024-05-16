//! for convenient testing purpose
use rocksdb::{Options, DB};
use std::env::args;

fn main() {
    let args = args().collect::<Vec<_>>();
    if args.len() < 3 {
        println!("usage: ./create_cf_db path cf1 cf2 cf3...");
        return;
    }

    let path = &args[1];
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    DB::open_cf(&opts, path, &args[2..]).expect("failed to create db");
}
