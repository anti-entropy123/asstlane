use std::io::Read;

use serde_json::Value;

fn main() {
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input).unwrap();

    let input: Value = serde_json::from_str(&input).unwrap();
    let input = input.as_object().unwrap();
    let a = input["a"].as_i64().unwrap();
    let b = input["b"].as_i64().unwrap();
    println!("{}", a + b)
}
