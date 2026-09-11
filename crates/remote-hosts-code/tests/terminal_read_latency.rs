//! Compare retained legacy full-log reads with the stable seek path, not network RTT.
use remote_hosts_code::{files::Workspace, random, store::Store, terminal::Terminals};
use serde_json::{Value, json};
use std::{path::Path, time::Instant};

#[tokio::test]
#[ignore = "explicit local benchmark; no timing threshold in functional CI"]
async fn compare_legacy_and_stable_terminal_tail_reads() {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/terminal-test-tmp");
    std::fs::create_dir_all(&base).unwrap();
    let dir = tempfile::tempdir_in(base).unwrap();
    let store = Store::open(&dir.path().join("state")).await.unwrap();
    let logs = dir.path().join("logs");
    let terminals = Terminals::new(store.clone(), logs.clone(), random())
        .await
        .unwrap();
    let ws = Workspace {
        id: random(),
        device_id: random(),
        root: dir.path().canonicalize().unwrap(),
    };
    let size = 8 * 1024 * 1024;
    let bytes = vec![b'x'; size];
    let mut results = Vec::new();
    for (format, label) in [(0, "legacy_full_log"), (1, "stable_seek")] {
        let id = uuid::Uuid::new_v4().to_string();
        std::fs::write(logs.join(format!("{id}.log")), &bytes).unwrap();
        store.put("terminal", &id, &json!({"id":id,"workspace_id":ws.id,"state":"exited","exit_code":0,"output_truncated":false,"created_at":0,"pty":false,"log_format":format,"output_complete":true}), i64::MAX).await.unwrap();
        let request = json!({"terminal_id":id,"cursor":size-1024,"max_bytes":1024});
        let mut samples = Vec::new();
        for n in 0..13 {
            let start = Instant::now();
            let output = terminals.read(&ws, &request).await.unwrap();
            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(output["output"].as_str().unwrap().as_bytes(), &[b'x'; 1024]);
            assert_eq!(output["cursor"], size);
            assert_eq!(output["has_more"], false);
            if n >= 3 {
                samples.push(elapsed);
            }
        }
        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        samples.sort_by(f64::total_cmp);
        results.push(json!({"path":label,"mean_ms":mean,"p50_ms":samples[4],"p95_ms":samples[9],"samples":samples.len(),"returned_text_bytes":10240}));
    }
    let report: Value = json!({"scope":"local Terminals::read + SQLite + file I/O; debug; warm cache; no gateway/network/model","log_bytes":size,"page_bytes":1024,"results":results});
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    let destination = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/review-round2-20260909/terminal-read-benchmark.json");
    std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
    std::fs::write(destination, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
}
