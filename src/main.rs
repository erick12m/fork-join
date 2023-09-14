use csv::ReaderBuilder;
use std::{
    collections::HashMap,
    error::Error,
    fs::{read_dir, File},
    io::{BufRead, BufReader, Read},
    time::Instant,
};

fn read_csv(data: &str) -> Result<(), Box<dyn Error>> {
    let mut rdr = ReaderBuilder::new()
        .delimiter(b',')
        .from_reader(data.as_bytes());

    let result = rdr
        .records()
        .map(|l| {
            let line = l.unwrap();
            (line[3].to_string(), line[7].parse::<i32>().unwrap())
        })
        .fold(HashMap::new(), |mut acc, (key, value)| {
            *acc.entry(key).or_insert(0) += value;
            acc
        });
    println!("{:?}", result);
    Ok(())
}

/* fn main() -> Result<(), Box<dyn Error>> {
    let mut f = File::open("dataset/CAvideos.csv")?;
    let mut buffer = String::new();
    f.read_to_string(&mut buffer)?;
    read_csv(&buffer)?;
    Ok(())
}*/

fn main() -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    let result = read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/dataset"))?
        .map(|d| d.unwrap().path())
        .flat_map(|path| {
            let mut f = File::open(path).unwrap();
            let mut buffer = String::new();
            if let Ok(_) = f.read_to_string(&mut buffer) {
                let mut rdr = ReaderBuilder::new()
                    .delimiter(b',')
                    .from_reader(buffer.as_bytes());
                let result2 = rdr.records();
                result2
                    .map(|l| {
                        let line = l.unwrap();
                        (line[3].to_string(), line[7].parse::<i64>().unwrap())
                    })
                    .fold(HashMap::new(), |mut acc, (key, value)| {
                        *acc.entry(key).or_insert(0) += value;
                        acc
                    })
            } else {
                HashMap::new()
            }
        })
        .fold(HashMap::new(), |mut acc, (key, value)| {
            *acc.entry(key).or_insert(0) += value;
            acc
        });
    println!("{:?}", start.elapsed());
    println!("{:?}", result.get("marshmello"));
    Ok(())
}

//test
#[test]
fn test() {
    let start = Instant::now();
    let result = read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/data"))
        .unwrap()
        .map(|d| d.unwrap().path())
        .flat_map(|path| {
            let file = File::open(path);
            let reader = BufReader::new(file.unwrap());
            reader.lines()
        })
        .map(|l| {
            let line = l.unwrap();
            let words = line.split(' ');
            let mut counts = HashMap::new();
            words.for_each(|w| *counts.entry(w.to_string()).or_insert(0) += 1);
            counts
        })
        .fold(HashMap::new(), |mut acc, words| {
            words
                .iter()
                .for_each(|(k, v)| *acc.entry(k.clone()).or_insert(0) += v);
            acc
        });
    println!("{:?}", start.elapsed());

    println!("{:?}", result);
}
