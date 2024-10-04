#![feature(new_uninit)]

use std::{
    io::{Read, Write},
    process::{Command, Stdio},
    sync::{Arc, RwLock},
    thread,
    time::SystemTime,
};

use hashbrown::HashMap;
use serde::Deserialize;
use serde_json::json;

fn run_workflow(app_list: &[&str], input_datas: &[String], resps: Arc<RwLock<Vec<String>>>) {
    thread::scope(|scope| {
        for (idx, app) in app_list.iter().enumerate() {
            let resps = Arc::clone(&resps);
            thread::Builder::new()
                .spawn_scoped(scope, move || {
                    let process = match Command::new(format!("target/release/{app}"))
                        .stdin(Stdio::piped())
                        .stdout(Stdio::piped())
                        .spawn()
                    {
                        Err(why) => panic!("couldn't spawn child proc: {:?}", why),
                        Ok(process) => process,
                    };

                    process
                        .stdin
                        .unwrap()
                        .write_all(input_datas[idx].as_bytes())
                        .expect("couldn't write to child proc stdin");

                    let mut s = String::new();
                    process.stdout.unwrap().read_to_string(&mut s).unwrap();
                    resps.write().unwrap()[idx] = s;
                })
                .unwrap();
        }
    });
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct MapperResponse {
    read_time: usize,
    comp_time: usize,
    store_time: usize,
    count_num: usize,
    resp_data: HashMap<String, String>,
}

fn map_reduce() {
    let run_list_1 = ["mapper", "mapper", "mapper"];
    let run_list_2 = ["reducer", "reducer", "reducer"];
    let reducer_num = run_list_2.len();

    let mapper_nums = run_list_1.len();
    let input_datas = (0..mapper_nums)
        .map(|idx| {
            json!({
                "input_part": idx,
                "reduce_num": reducer_num,
            })
            .to_string()
        })
        .collect::<Vec<String>>();

    let resps = new_resps(mapper_nums);
    run_workflow(&run_list_1, &input_datas, Arc::clone(&resps));
    // println!("{:?}", resps.write().unwrap());
    let resps = resps
        .read()
        .unwrap()
        .iter()
        .map(|resp| {
            // println!("{}", resp);
            let obj: MapperResponse = serde_json::from_str(resp).unwrap();
            // println!("{}", obj);
            obj.resp_data
        })
        .collect::<Vec<_>>();

    let mut input_datas: Vec<String> = Vec::new();
    for reducer_id in 0..reducer_num {
        let mut input_item: Vec<String> = Vec::new();
        for (mapper_id, output) in resps.iter().enumerate() {
            for (k, v) in output {
                if *k == format!("{}:{}", mapper_id, reducer_id) {
                    input_item.push(v.to_owned());
                }
            }
        }
        let input_item = json! {{
            "reduce_part": reducer_id,
            "content": input_item,
        }};
        input_datas.push(serde_json::to_string_pretty(&input_item).unwrap());
    }

    // println!("{:?}", input_datas);
    let resps = new_resps(reducer_num);
    run_workflow(&run_list_2, &input_datas, Arc::clone(&resps));
    println!("{:?}", resps.write().unwrap());
}

fn main() {
    let start = SystemTime::now();
    map_reduce();
    println!(
        "cost time: {:?}",
        SystemTime::now().duration_since(start).unwrap()
    )
}

fn _add_array() {
    let run_list = ["add", "add", "add"];
    let input_data = json!({
        "a": 1,
        "b": 2
    });
    let input_datas = vec![
        input_data.to_string(),
        input_data.to_string(),
        input_data.to_string(),
    ];
    run_workflow(&run_list, &input_datas, new_resps(3));
}

fn new_resps(len: usize) -> Arc<RwLock<Vec<String>>> {
    let mut resps = Vec::new();
    for _ in 0..len {
        resps.push(String::new())
    }
    Arc::new(RwLock::new(resps))
}
