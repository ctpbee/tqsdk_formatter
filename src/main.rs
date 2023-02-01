#![allow(dead_code)]

mod pool;

use indicatif::ProgressBar;

use polars::prelude::*;
use std::fs::{File, read_dir, create_dir};
use std::path::PathBuf;
use std::sync::Mutex;
use clap::Parser;
use pool::Pool;

fn read_file(dir_path: PathBuf, name: String) -> PolarsResult<DataFrame> {
    let path = dir_path.join(format!("{}.csv", name));
    let file = File::open(path).expect("could not open file");
    CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .finish()
}

fn read_parquet(name: String) -> PolarsResult<DataFrame> {
    let file = File::open(format!("{}.csv", name)).expect("could not open file");
    ParquetReader::new(file).finish()
}

fn blank(len: usize) -> Series {
    Series::new("blank", (0..len).into_iter().map(|_| 0.0).collect::<Vec<f32>>().as_slice())
}


fn change_formatter(name: String, frame: DataFrame) -> PolarsResult<DataFrame> {
    let len = frame.column("datetime")?.len();
    let frame = frame.lazy().with_column(
        col("datetime").str().strptime(StrpTimeOptions {
            date_dtype: DataType::Datetime(TimeUnit::Nanoseconds, None),
            ..Default::default()
        })).collect()?;
    let date = frame.column("datetime")?.cast(&DataType::Date)?;
    let time = frame.column("datetime")?.cast(&DataType::Time)?;
    let last_price = frame.column(format!("{}.last_price", name).as_str())?.cast(&DataType::Float32)?;
    let volume = frame.column(format!("{}.volume", name).as_str())?.cast(&DataType::Float32)?;
    let open_interest = frame.column(format!("{}.open_interest", name).as_str())?.cast(&DataType::Float32)?;
    let turnover = frame.column(format!("{}.amount", name).as_str())?.cast(&DataType::Float32)?;
    let ask_price_1 = frame.column(format!("{}.ask_price1", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;
    let ask_price_2 = frame.column(format!("{}.ask_price2", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;
    let ask_price_3 = frame.column(format!("{}.ask_price3", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;
    let ask_price_4 = frame.column(format!("{}.ask_price4", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;
    let ask_price_5 = frame.column(format!("{}.ask_price5", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;

    let bid_price_1 = frame.column(format!("{}.bid_price1", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;
    let bid_price_2 = frame.column(format!("{}.bid_price2", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;
    let bid_price_3 = frame.column(format!("{}.bid_price3", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;
    let bid_price_4 = frame.column(format!("{}.bid_price4", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;
    let bid_price_5 = frame.column(format!("{}.bid_price5", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;

    let bid_volume_1 = frame.column(format!("{}.bid_volume1", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;
    let bid_volume_2 = frame.column(format!("{}.bid_volume2", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;
    let bid_volume_3 = frame.column(format!("{}.bid_volume3", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;
    let bid_volume_4 = frame.column(format!("{}.bid_volume4", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;
    let bid_volume_5 = frame.column(format!("{}.bid_volume5", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;

    let ask_volume_1 = frame.column(format!("{}.ask_volume1", name).as_str())?.cast(&DataType::Float32)?;
    let ask_volume_2 = frame.column(format!("{}.ask_volume2", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;
    let ask_volume_3 = frame.column(format!("{}.ask_volume3", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;
    let ask_volume_4 = frame.column(format!("{}.ask_volume4", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;
    let ask_volume_5 = frame.column(format!("{}.ask_volume5", name).as_str()).unwrap_or(&blank(len)).cast(&DataType::Float32)?;
    df!(
        "date"=>date,
        "time"=>time,
        "volume"=>volume,
        "open_interest"=>open_interest,
        "turnover"=>turnover,
        "last_price" => last_price,
        "ask_price_1" => ask_price_1,
        "ask_price_2" => ask_price_2,
        "ask_price_3" => ask_price_3,
        "ask_price_4" => ask_price_4,
        "ask_price_5" => ask_price_5,
        "ask_volume_1" => ask_volume_1,
        "ask_volume_2" => ask_volume_2,
        "ask_volume_3" => ask_volume_3,
        "ask_volume_4" => ask_volume_4,
        "ask_volume_5" => ask_volume_5,
        "bid_price_1"=>bid_price_1,
        "bid_price_2"=>bid_price_2,
        "bid_price_3"=>bid_price_3,
        "bid_price_4"=>bid_price_4,
        "bid_price_5"=>bid_price_5,
        "bid_volume_1"=>bid_volume_1,
        "bid_volume_2"=>bid_volume_2,
        "bid_volume_3"=>bid_volume_3,
        "bid_volume_4"=>bid_volume_4,
        "bid_volume_5"=>bid_volume_5,
    )
}

fn write_parquet(name: String, dir_path: PathBuf, mut frame: DataFrame) {
    let path = dir_path.join(format!("{}.parquet", name));
    let mut file = File::create(path).unwrap();
    ParquetWriter::new(&mut file).finish(&mut frame).unwrap();
}


fn write_csv(name: String, dir_path: PathBuf, mut frame: DataFrame) {
    let path = dir_path.join(format!("{}.csv", name));
    let mut file = File::create(path).unwrap();
    CsvWriter::new(&mut file).finish(&mut frame).unwrap()
}


fn read_info(args: Args) -> (PathBuf, PathBuf, Vec<String>) {
    let config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let created = args.created;
    let read_path = if args.from_path.starts_with(".") {
        config_path.join(args.from_path.clone())
    } else {
        args.from_path.into()
    };

    let entries = read_dir(read_path.clone()).unwrap();
    let mut file_list = vec![];
    for entry in entries.flatten() {
        let filename = entry.file_name().to_str().unwrap().to_string();
        if filename.ends_with(".csv") {
            file_list.push(filename);
        }
    }

    let out_path = if args.to_path.starts_with(".") {
        let p = config_path.join(args.to_path);
        if !p.exists() & created {
            create_dir(p.clone()).unwrap();
        };
        p
    } else {
        args.to_path.into()
    };
    (read_path, out_path, file_list)
}


/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = String::from("./"))]
    from_path: String,
    #[arg(short, long, default_value_t = true)]
    created: bool,
    #[arg(short, long, default_value_t = String::from("./tick"))]
    to_path: String,
    #[arg(short, long, default_value_t = 5)]
    number_cpu: i32,
    #[arg(short, long, default_value_t = String::from("parquet"))]
    write_formatter: String,
}


fn main() {
    let args = Args::parse();
    let cpu_number = args.number_cpu as usize;
    let file_type = args.write_formatter.clone();
    let (read_path, write_path, file_list) = read_info(args);
    let bar = Arc::new(Mutex::new(ProgressBar::new(file_list.len() as u64)));
    let pool = Pool::new(cpu_number);
    println!("tasks number:{} starts with thread_number: {} write formatter:{}", file_list.len(), cpu_number, file_type);
    for file in file_list.clone() {
        let rp = read_path.clone();
        let wp = write_path.clone();
        let process = bar.clone();
        let filetype = file_type.clone();
        pool.execute(move || {
            let name = file.split(".csv").next().unwrap().to_string();
            let frame = read_file(rp, name.clone()).unwrap();
            let formatter = change_formatter(name.clone(), frame).unwrap();
            if filetype.eq("parquet") {
                write_parquet(name.clone(), wp, formatter);
            } else {
                write_csv(name.clone(), wp, formatter);
            }
            process.lock().unwrap().inc(1);
        });
    }
}