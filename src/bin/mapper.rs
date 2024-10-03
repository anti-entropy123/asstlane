use std::{
    fs::File,
    hash::BuildHasher,
    io::{self, Read},
    time::SystemTime,
};

// use hashbrown::DefaultHashBuilder;
use hashbrown::HashMap;
use serde_json::{json, Value};

fn main() {
    let read_start = SystemTime::now();

    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input).unwrap();
    let input: Value = serde_json::from_str(&input).unwrap();
    let input = input.as_object().unwrap();

    // let input_name = input["input_name"].as_str().unwrap();
    let input_part = input["input_part"].as_u64().unwrap();
    let reduce_num = input["reduce_num"].as_u64().unwrap();

    let content = get_object(&format!("fake_data_{}.txt", input_part)).unwrap();
    let read_end: SystemTime = SystemTime::now();

    // println!(
    //     "read cost: {} ms",
    //     read_end.duration_since(read_start).unwrap().as_millis()
    // );

    let mut counter = hashbrown::HashMap::new();

    // if debug {
    //     println!("{}", &content[0..100])
    // }

    for line in content.lines() {
        let words = line
            .trim()
            .split(' ')
            .filter(|word| word.chars().all(char::is_alphabetic));

        for word in words {
            let old_count = *counter.entry(word).or_insert(0u32);
            counter.insert(word, old_count + 1);
        }
    }

    let shuffle = shuffle_counter(reduce_num, &counter);

    let com_end = SystemTime::now();
    // println!(
    //     "compute cost: {} ms",
    //     com_end.duration_since(read_end).unwrap().as_millis()
    // );

    let mut resp_data: HashMap<String, String> = HashMap::new();
    for i in 0..reduce_num {
        if let Some(val) = shuffle.get(&i) {
            // if debug {
            //     println!("shuffle[{}]={}", i, val)
            // }
            resp_data.insert(format!("{}:{}", input_part, i), val.to_owned());
        }
    }
    let store_end = SystemTime::now();

    let resp = json!({
        "read_time": read_end.duration_since(read_start).unwrap().as_millis(),
        "comp_time": com_end.duration_since(read_end).unwrap().as_millis(),
        "store_time": store_end.duration_since(com_end).unwrap().as_millis(),
        "count_num": counter.len(),
        "resp_data": resp_data,
    });

    println!("{}", resp)
}

fn get_object(object_name: &String) -> Result<String, io::Error> {
    let mut file = File::open(object_name)?;
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    Ok(buf)
}

fn shuffle_counter(
    reducer_num: u64,
    counter: &hashbrown::HashMap<&str, u32>,
) -> HashMap<u64, String> {
    let mut shuffle: HashMap<u64, String> = HashMap::with_capacity((reducer_num + 5) as usize);

    for (word, count) in counter {
        let reduce_id = foldhash::fast::FixedState::default().hash_one(word) % reducer_num;
        let old_val = shuffle
            .get(&reduce_id)
            .map(|s| s.as_str())
            .unwrap_or_else(|| "");

        shuffle.insert(reduce_id, format!("{}{}:{};", old_val, word, count));
    }

    for val in shuffle.values_mut() {
        let s = val.char_indices();
        let (idx, c) = s.last().unwrap();
        if c == ';' {
            *val = val.as_str()[0..idx].to_string();
        }
    }

    shuffle
}

#[test]
fn test_shuffle_counter() {
    let mut input: hashbrown::HashMap<&str, u32> = hashbrown::HashMap::new();
    input.insert("yjn", 3);
    input.insert("abc", 2);
    input.insert("rust", 10);
    input.insert("libos", 7);

    let result = shuffle_counter(4, &input);
    println!("{:?}", result)
}
