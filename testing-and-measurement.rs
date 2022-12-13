use deprog::*;
use redis::Commands;
use std::{collections::HashSet, io::Read};

const ADDRA: &str = "https://cluster-a.anonymous.osdi23";
const JWTA: &str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJwcml2aWxlZ2UiOiJ1c2VyIiwidXNlcl9pZCI6IjAzM2I3ODlhYWE0M2NkZDBlMWI0YTY3NDg1MTYzMjI1ZTE4ZmE3NjVmMmZiNWMyNzJlNGM4MDRjYzM5ODFhYTdiMSIsImV4cCI6MjAwMDAwMDAwMH0.B8PiEk5Io0NSNqjFXDxQO5pYMtTC7LTzn1JDvsw_LqI";
const JWTB: &str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJwcml2aWxlZ2UiOiJ1c2VyIiwidXNlcl9pZCI6IjAzMjQ5YmI0YmI5YTAyNTExN2Y1NmFmMWM0NGRkZjkxMTg0MzNkZGYwNmE2MDQ2MTlhMzU4ZTg5NzRhZWViZTE0ZSIsImV4cCI6MjAwMDAwMDAwMH0.cOzGwEPLGfp5uT9PsV125DO4RgcDArE14MoTtqY76As";

#[derive(serde::Serialize, serde::Deserialize)]
pub struct PSIParam {
    pub sender_folder: String,
    pub receiver_folder: String,
    pub bin_num: i64,
    pub worker_num: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let handler = DeProg::new(ADDRA, &JWTA);
    let jwt_b = decode_jwt_without_validation(&JWTB)?;
    let participants = vec![
        Participant {
            user_id: handler.get_user_id()?,
            role: "receiver".to_string(),
        },
        Participant {
            user_id: jwt_b.user_id.clone(),
            role: "sender".to_string(),
        },
    ];
    let param = serde_json::to_vec(&PSIParam {
        sender_folder: "sender".to_string(),
        receiver_folder: "receiver".to_string(),
        bin_num: 1024,
        worker_num: 40,
    })?;
    let start_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)?
        .as_millis();
    let task_id = handler.run_task("psi", &param, &participants, true).await?;
    let res = handler
        .read_or_wait(&format!("tasks:{}:output", task_id))
        .await?;
    let end_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)?
        .as_millis();
    let res: Vec<String> = serde_json::from_slice(&res)?;
    println!("{:?}", res);
    println!("PSI Time: {} ms", end_timestamp - start_timestamp);
    let client = redis::Client::open("redis://cluster-a.anonymous.osdi23/")?;
    let mut con = client.get_connection()?;
    let mut tot = 0;
    for out in &res {
        let len: i32 = con.strlen(format!("{}:out", out))?;
        tot += len / 16;
    }
    println!("tot: {}", tot);
    let mut set = HashSet::new();
    for entry in std::fs::read_dir("ans")? {
        let file_path = entry?.path();
        let mut file = std::fs::File::open(file_path).unwrap();
        let mut items: Vec<u8> = Vec::new();
        file.read_to_end(&mut items).unwrap();
        let items = items.chunks_exact(16).collect::<Vec<_>>();
        for item in items {
            let a = <[u8; 16]>::try_from(item).unwrap();
            set.insert(a);
        }
    }
    assert!(set.len() as i32 == tot);
    for out in &res {
        let items: Vec<u8> = con.get(format!("{}:out", out))?;
        let items = items.chunks_exact(16).collect::<Vec<_>>();
        for item in items {
            let a = <[u8; 16]>::try_from(item).unwrap();
            if !set.contains(&a) {
                Err("Wrong answer")?;
            } else {
                set.remove(&a);
            }
        }
    }
    println!("Verification pass");
    Ok(())
}
