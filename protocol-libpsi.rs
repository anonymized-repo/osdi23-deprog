use deprog::{DeProg, Participant, ProtocolEntry};
use rand::Rng;
use redis::Commands;
use std::{
    io::{Read, Write},
    process::Command,
};

#[derive(serde::Serialize, serde::Deserialize)]
pub struct LibPSIParam {
    pub sender_key: String,
    pub receiver_key: String,
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
        let mut timestamp =
            std::fs::File::create(&format!("receiver.{}.timestamp", handler.get_task_id()?))?;
        let start_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_millis();
        timestamp.write_all(format!("{}\n", start_timestamp).as_bytes())?;

        let params: LibPSIParam = serde_json::from_slice(&param)?;
        let redis_addr = handler.read_entry("redis_addr").await?;
        let redis_addr = String::from_utf8_lossy(&redis_addr);
        let client = redis::Client::open(&*redis_addr)?;
        let mut con = client.get_connection()?;
        let items: Vec<u8> = con.get(&params.receiver_key)?;
        let mut file = std::fs::File::create(&format!("receiver.{}.bin", handler.get_task_id()?))?;
        file.write_all(&items)?;
        let ip = handler.request_info().await?.requestor_ip;
        let mut port = rand::thread_rng().gen_range(5000..30000);
        while std::net::TcpStream::connect(&format!("{}:{}", ip, port)).is_ok() {
            port = rand::thread_rng().gen_range(5000..30000);
        }
        let addr = format!("{}:{}", ip, port);
        handler.set_variable("addr", addr.as_bytes(), &[participants[1].clone()])
            .await?;
        Command::new("./libpsi-frontend")
            .args([
                "-kkrt",
                "-r",
                "1",
                "-in",
                &format!("receiver.{}.bin", handler.get_task_id()?),
                "-ip",
                &addr,
                "-padSmallSet",
            ])
            .status()?;
        let mut file = std::fs::File::open(&format!("receiver.{}.bin.out", handler.get_task_id()?))?;
        let mut buffer = [0; 8];
        let mut ans = vec![];
        loop {
            match file.read_exact(&mut buffer) {
                Ok(_) => {
                    let idx = u64::from_le_bytes(buffer) as usize;
                    ans.push(&items[idx * 16..(idx + 1) * 16]);
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        Err(e)?;
                    }
                }
            }
        }
        con.set(&format!("{}:out", handler.get_task_id()?), &ans.concat())?;

        let end_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_millis();
        timestamp.write_all(format!("{}\n", end_timestamp).as_bytes())?;
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
        let mut timestamp =
            std::fs::File::create(&format!("sender.{}.timestamp", handler.get_task_id()?))?;
        let start_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_millis();
        timestamp.write_all(format!("{}\n", start_timestamp).as_bytes())?;

        let params: LibPSIParam = serde_json::from_slice(&param)?;
        let redis_addr = handler.read_entry("redis_addr").await?;
        let redis_addr = String::from_utf8_lossy(&redis_addr);
        let client = redis::Client::open(&*redis_addr)?;
        let mut con = client.get_connection()?;
        let items: Vec<u8> = con.get(&params.sender_key)?;
        let mut file = std::fs::File::create(&format!("sender.{}.bin", handler.get_task_id()?))?;
        file.write_all(&items)?;
        let addr = handler.get_variable("addr", &participants[0]).await?;
        let addr = String::from_utf8_lossy(&addr);
        Command::new("./libpsi-frontend")
            .args([
                "-kkrt",
                "-r",
                "0",
                "-in",
                &format!("sender.{}.bin", handler.get_task_id()?),
                "-ip",
                &addr,
                "-padSmallSet",
            ])
            .status()?;

        let end_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_millis();
        timestamp.write_all(format!("{}\n", end_timestamp).as_bytes())?;
        Ok(())
    }
}

deprog::protocol_start!(("libpsi:receiver", Receiver), ("libpsi:sender", Sender));
