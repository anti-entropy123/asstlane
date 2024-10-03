use std::{
    fs::File,
    io::{self, Read, Write},
    time::SystemTime,
};

use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
struct ReducerInput {
    reduce_part: usize,
    content: Vec<String>,
}

fn main() {
    let read_start = SystemTime::now();

    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input).unwrap();
    // println!("{}", input);

    let input: ReducerInput = serde_json::from_str(&input).unwrap();
    let reduce_part = input.reduce_part;
    let raw_datas: Vec<String> = input.content;

    let read_end = SystemTime::now();

    let mut counter: hashbrown::HashMap<String, u32> = hashbrown::HashMap::new();
    for raw_data in raw_datas {
        let pairs = raw_data.split(';').map(|s| s.split(':'));
        for mut pair in pairs {
            match (pair.next(), pair.next(), pair.next()) {
                (Some(word), Some(count), None) => {
                    let old_val = counter.get(word).copied().unwrap_or(0);
                    counter.insert(word.to_owned(), old_val + count.parse::<u32>().unwrap());
                }
                _ => panic!("wrong redis input object: {:?}", pair),
            }
        }
    }

    let output_entries: Vec<_> = counter
        .iter_mut()
        .map(|(word, count)| format!("{}:{}", word, count))
        .collect();
    let output = output_entries.join("\n");
    let comp_end = SystemTime::now();

    put_object(&format!("output/part-{}", reduce_part), &output).unwrap();
    let store_end = SystemTime::now();

    let resp = json!({
        "read_time": read_end.duration_since(read_start).unwrap().as_millis(),
        "comp_time": comp_end.duration_since(read_end).unwrap().as_millis(),
        "store_time": store_end.duration_since(comp_end).unwrap().as_millis(),
    });

    println!("{}", resp)
}

fn put_object(object_name: &String, val: &String) -> Result<(), io::Error> {
    let mut f = File::create(object_name)?;
    f.write_all(val.as_bytes())?;

    Ok(())
}
