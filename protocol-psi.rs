use deprog::{DeProg, Participant, ProtocolEntry};
use rand::RngCore;
use redis::{Client, Commands};
use sha2::{Digest, Sha256};
use std::{
    io::Read,
    sync::{Arc, Mutex},
};
use threadpool::ThreadPool;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct PSIParam {
    pub sender_folder: String,
    pub receiver_folder: String,
    pub bin_num: i64,
    pub worker_num: i64,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct LibPSIParam {
    pub sender_key: String,
    pub receiver_key: String,
}

async fn tokenize(
    handler: &DeProg,
    client: &Client,
    params: &PSIParam,
    dir: &str,
    prefix: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let num_threads = usize::from(std::thread::available_parallelism()?);
    let pool = ThreadPool::new(num_threads);
    println!("tokenize threads: {}", num_threads);
    let num = Arc::new(Mutex::new(vec![0; params.bin_num as usize]));
    let tot_items = Arc::new(Mutex::new(0));
    for entry in std::fs::read_dir(dir)? {
        let file_path = entry?.path();
        let mut con = client.get_connection()?;
        let bin_num = params.bin_num;
        let prefix = prefix.to_string();
        let num = num.clone();
        let tot_items = tot_items.clone();
        pool.execute(move || {
            let mut file = std::fs::File::open(file_path).unwrap();
            let mut items: Vec<u8> = Vec::new();
            file.read_to_end(&mut items).unwrap();
            let items = items.chunks_exact(16).collect::<Vec<_>>();
            *tot_items.lock().unwrap() += items.len();
            let mut a = vec![];
            for _ in 0..bin_num {
                a.push(vec![]);
            }
            for item in items {
                let hash = &Sha256::digest(item);
                let idx = ((hash[0] as u64) << 8 | hash[1] as u64) << 8 | hash[2] as u64;
                let idx = idx % bin_num as u64;
                a[idx as usize].push(item.to_vec());
            }
            for i in 0..bin_num as usize {
                let _: i32 = con
                    .append(&format!("{}:{}", prefix, i), &a[i].concat())
                    .unwrap();
            }
            for i in 0..bin_num as usize {
                num.lock().unwrap()[i] += a[i].len();
            }
        });
    }
    pool.join();
    let mut mx = 0;
    for i in 0..params.bin_num as usize {
        if num.lock().unwrap()[i] > mx {
            mx = num.lock().unwrap()[i];
        }
    }
    let epsilon = 0.1;
    let mx_len = (1.0 + epsilon) * (*tot_items.lock().unwrap() as f64 / params.bin_num as f64);
    let mx_len = mx_len.ceil() as usize;
    assert!(mx < mx_len);
    let mut a = vec![];
    let mut data: [u8; 16] = [0; 16];
    let mut rng = rand::thread_rng();
    let mut con = client.get_connection()?;
    for i in 0..params.bin_num as usize {
        for _ in num.lock().unwrap()[i]..mx_len {
            rng.fill_bytes(&mut data);
            a.push(data.to_vec());
        }
        con.append(&format!("{}:{}", prefix, i), &a.concat())?;
        a.clear();
    }
    Ok(())
}

struct Receiver;
#[deprog::async_trait]
impl ProtocolEntry for Receiver {
    async fn start(
        &self,
        handler: DeProg,
        param: Vec<u8>,
        participants: Vec<Participant>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let params: PSIParam = serde_json::from_slice(&param)?;
        let ip = handler.request_info().await?.requestor_ip;
        let redis_addr = format!("redis://{}/", ip);
        handler.update_entry("redis_addr", redis_addr.as_bytes()).await?;
        let client = redis::Client::open(&*redis_addr)?;
        let tokenize_start = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_millis();
        tokenize(
            &handler,
            &client,
            &params,
            &params.receiver_folder,
            &format!("{}:receiver", handler.get_task_id()?),
        )
        .await?;
        let tokenize_end = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_millis();
        println!("tokenize time: {} ms", tokenize_end - tokenize_start);
        handler.get_variable("ready", &participants[1]).await?;
        let task_ids = Arc::new(Mutex::new(Vec::new()));
        let pool = ThreadPool::new(params.worker_num as usize);
        let psi_start = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_millis();
        for i in 0..params.bin_num {
            let handler = handler.clone();
            let participants = participants.clone();
            let task_ids = task_ids.clone();
            pool.execute(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async move {
                        let task_id = handler
                            .run_task(
                                "libpsi",
                                &serde_json::to_vec(&LibPSIParam {
                                    sender_key: format!("{}:sender:{}", handler.get_task_id()?, i),
                                    receiver_key: format!("{}:receiver:{}", handler.get_task_id()?, i),
                                })?,
                                &participants,
                                true,
                            )
                            .await?;
                        handler.wait_task(&task_id).await?;
                        task_ids.lock().unwrap().push(task_id);
                        Ok::<(), Box<dyn std::error::Error + Send + Sync + 'static>>(())
                    })
                    .unwrap();
            });
        }
        pool.join();
        let psi_end = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_millis();
        println!("psi time: {} ms", psi_end - psi_start);
        let ans = serde_json::to_vec(&task_ids.lock().unwrap().clone())?;
        handler.set_variable("finish", &ans, &[participants[1].clone()])
            .await?;
        handler.create_entry(&format!("tasks:{}:output", handler.get_task_id()?), &ans)
            .await?;
        Ok(())
    }
}

struct Sender;
#[deprog::async_trait]
impl ProtocolEntry for Sender {
    async fn start(
        &self,
        handler: DeProg,
        param: Vec<u8>,
        participants: Vec<Participant>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let params: PSIParam = serde_json::from_slice(&param)?;
        let ip = handler.request_info().await?.requestor_ip;
        let redis_addr = format!("redis://{}/", ip);
        handler.update_entry("redis_addr", redis_addr.as_bytes()).await?;
        let client = redis::Client::open(&*redis_addr)?;
        let tokenize_start = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_millis();
        tokenize(
            &handler,
            &client,
            &params,
            &params.sender_folder,
            &format!("{}:sender", handler.get_task_id()?),
        )
        .await?;
        let tokenize_end = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_millis();
        println!("tokenize time: {} ms", tokenize_end - tokenize_start);
        handler.set_variable("ready", Default::default(), &[participants[0].clone()])
            .await?;
        handler.get_variable("finish", &participants[0]).await?;
        Ok(())
    }
}

deprog::protocol_start!(("psi:receiver", Receiver), ("psi:sender", Sender));
