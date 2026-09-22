//! Real local process tests; no production device, credentials or gateway required.
#![cfg(unix)]
use remote_hosts_code::{AgentConfig, files::Workspace, random, store::Store, terminal::Terminals};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};

struct Fixture {
    _dir: tempfile::TempDir,
    config: AgentConfig,
    ws: Workspace,
    store: Store,
    terminals: Terminals,
}
impl Fixture {
    async fn new() -> Self {
        let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/terminal-test-tmp");
        std::fs::create_dir_all(&base).unwrap();
        let dir = tempfile::tempdir_in(base).unwrap();
        let root = dir.path().join("project");
        std::fs::create_dir(&root).unwrap();
        let config = AgentConfig {
            gateway_url: "https://example.invalid".into(),
            device_id: uuid::Uuid::new_v4().to_string(),
            device_token: "0123456789abcdef0123456789abcdefFEDCBA9876543210FEDCBA9876543210".into(),
            state_dir: dir.path().join("state"),
            roots: vec![root.canonicalize().unwrap()],
            allow_write: true,
            allow_exec: true,
            shell: "/bin/sh".into(),
        };
        let ws = Workspace {
            id: random(),
            device_id: config.device_id.clone(),
            root: config.roots[0].clone(),
        };
        let store = Store::open(&config.state_dir).await.unwrap();
        let terminals = Terminals::new(
            store.clone(),
            config.state_dir.join("terminals"),
            config.device_token.clone(),
        )
        .await
        .unwrap();
        Self {
            _dir: dir,
            config,
            ws,
            store,
            terminals,
        }
    }
    async fn start(&self, command: &str, pty: bool, timeout: u64) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        self.terminals
            .start(
                &self.config,
                &self.ws,
                &json!({"command":command,"pty":pty,"timeout_seconds":timeout}),
                &id,
            )
            .await
            .unwrap();
        id
    }
    async fn read(&self, id: &str, cursor: usize) -> anyhow::Result<Value> {
        self.terminals
            .read(
                &self.ws,
                &json!({"terminal_id":id,"cursor":cursor,"max_bytes":65536}),
            )
            .await
    }
    async fn done(&self, id: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let v = self.read(id, 0).await.unwrap();
                if !matches!(
                    v["terminal"]["state"].as_str(),
                    Some("running" | "starting")
                ) && v["terminal"]["output_complete"] == true
                    && (v["terminal"]["exit_code"].is_i64() || v["terminal"]["state"] == "failed")
                {
                    return v;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("terminal did not finish")
    }
    async fn ready(&self) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !self.ws.root.join("ready").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        // The marker is written after printf. Allow its pipe collector to publish that chunk.
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    fn release(&self) {
        std::fs::write(self.ws.root.join("go"), "").unwrap();
    }
}
const BARRIER: &str = "touch ready; while [ ! -e go ]; do sleep 0.01; done";

#[tokio::test]
async fn noninteractive_uses_real_pipes_and_stdin_eof() {
    let f = Fixture::new().await;
    let id = f.start("for fd in 0 1 2; do if [ -t \"$fd\" ]; then printf 'tty:%s\\n' \"$fd\"; else printf 'pipe:%s\\n' \"$fd\"; fi; done; if [ ! -t 0 ]; then if read -r value; then printf 'unexpected input\\n'; else printf 'stdin:eof\\n'; fi; fi; printf 'stdout\\n'; printf 'stderr\\n' >&2; exit 7", false, 5).await;
    let v = f.done(&id).await;
    assert_eq!(v["terminal"]["exit_code"], 7);
    assert_eq!(
        v["output"],
        "pipe:0\npipe:1\npipe:2\nstdin:eof\nstdout\nstderr\n"
    );
}

#[tokio::test]
async fn noninteractive_rejects_terminal_input() {
    let f = Fixture::new().await;
    let id = f.start("sleep 30", false, 5).await;
    let input = f
        .terminals
        .input(&f.ws, &json!({"terminal_id":id,"text":"unused\n"}))
        .await;
    f.terminals
        .cancel(&f.ws, &json!({"terminal_id":id}))
        .await
        .unwrap();
    let error = input
        .expect_err("noninteractive commands must not accept interactive input")
        .to_string();
    assert!(error.contains("pty=true"), "{error}");
}

#[tokio::test]
async fn split_device_secret_never_becomes_a_published_prefix() {
    let f = Fixture::new().await;
    let secret = &f.config.device_token;
    let command = format!(
        "printf 'before:{}'; {BARRIER}; printf '{}:after\\n'",
        &secret[..32],
        &secret[32..]
    );
    let id = f.start(&command, false, 5).await;
    f.ready().await;
    let first = f.read(&id, 0).await.unwrap();
    f.release();
    let done = f.done(&id).await;
    let next = f
        .read(&id, first["cursor"].as_u64().unwrap() as usize)
        .await;
    assert_eq!(first["output"], "before:");
    assert_eq!(done["output"], "before:[REDACTED]:after\n");
    assert_eq!(
        format!(
            "{}{}",
            first["output"].as_str().unwrap(),
            next.unwrap()["output"].as_str().unwrap()
        ),
        done["output"]
    );
}

#[tokio::test]
async fn utf8_split_across_reads_does_not_move_existing_cursors() {
    let f = Fixture::new().await;
    let command = format!("printf '\\344'; {BARRIER}; printf '\\275\\240\\n'");
    let id = f.start(&command, false, 5).await;
    f.ready().await;
    let first = f.read(&id, 0).await.unwrap();
    f.release();
    let done = f.done(&id).await;
    let next = f
        .read(&id, first["cursor"].as_u64().unwrap() as usize)
        .await
        .unwrap();
    assert_eq!(first["output"], "");
    assert_eq!(
        format!(
            "{}{}",
            first["output"].as_str().unwrap(),
            next["output"].as_str().unwrap()
        ),
        "你\n"
    );
    assert_eq!(done["output"], "你\n");
}

#[tokio::test]
async fn unfinished_quoted_credentials_do_not_leak_or_shift_cursors() {
    let f = Fixture::new().await;
    let command = format!("printf \"token='alpha \"; {BARRIER}; printf \"beta' done\\n\"");
    let id = f.start(&command, false, 5).await;
    f.ready().await;
    let first = f.read(&id, 0).await.unwrap();
    f.release();
    let done = f.done(&id).await;
    let next = f
        .read(&id, first["cursor"].as_u64().unwrap() as usize)
        .await
        .unwrap();
    assert_eq!(first["output"], "token=[REDACTED]");
    assert_eq!(
        format!(
            "{}{}",
            first["output"].as_str().unwrap(),
            next["output"].as_str().unwrap()
        ),
        "token=[REDACTED] done\n"
    );
    assert_eq!(done["output"], "token=[REDACTED] done\n");
}

#[tokio::test]
async fn failed_spawn_does_not_leave_a_starting_terminal() {
    let f = Fixture::new().await;
    let mut config = f.config.clone();
    config.shell = f.ws.root.join("no-such-shell");
    let id = uuid::Uuid::new_v4().to_string();
    assert!(
        f.terminals
            .start(&config, &f.ws, &json!({"command":"true","pty":false}), &id)
            .await
            .is_err()
    );
    let status: Value = f.store.get("terminal", &id).await.unwrap().unwrap();
    assert_eq!(status["state"], "failed");
}

#[tokio::test]
async fn interactive_prompt_without_newline_remains_usable() {
    let f = Fixture::new().await;
    let id = f
        .start(
            "printf 'ready> '; read -r line; printf 'received:%s\\n' \"$line\"",
            true,
            5,
        )
        .await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if f.read(&id, 0).await.unwrap()["output"]
                .as_str()
                .unwrap()
                .contains("ready> ")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("prompt withheld until LF");
    f.terminals
        .input(&f.ws, &json!({"terminal_id":id,"text":"hello\n"}))
        .await
        .unwrap();
    let v = f.done(&id).await;
    assert!(v["output"].as_str().unwrap().contains("received:hello"));
    assert_eq!(v["terminal"]["exit_code"], 0);
    assert_eq!(v["terminal"]["pty"], true);
}

#[tokio::test]
async fn cancellation_kills_the_process_group_and_is_repeatable() {
    let f = Fixture::new().await;
    let id = f
        .start(
            "(while [ ! -f release-child ]; do sleep 0.02; done; touch child-survived) & touch ready; wait",
            false,
            10,
        )
        .await;
    f.ready().await;
    let request = json!({"terminal_id":id});
    let first = f.terminals.cancel(&f.ws, &request).await.unwrap();
    let second = f.terminals.cancel(&f.ws, &request).await.unwrap();
    assert_eq!(first["terminal"]["state"], "cancelled");
    assert_eq!(second["terminal"]["state"], "cancelled");
    assert_eq!(f.done(&id).await["terminal"]["state"], "cancelled");
    // Release a surviving child only after cancellation. A one-second sleep
    // could finish before the cancellation request on a busy test host.
    tokio::fs::write(f.ws.root.join("release-child"), b"")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(!f.ws.root.join("child-survived").exists());
}

#[tokio::test]
async fn timeout_seals_output_and_keeps_the_terminal_id() {
    let f = Fixture::new().await;
    let id = f.start("printf 'started\\n'; sleep 30", false, 1).await;
    let v = f.done(&id).await;
    assert_eq!(v["terminal"]["id"], id);
    assert_eq!(v["terminal"]["state"], "timed_out");
    assert_eq!(v["output"], "started\n");
    assert_eq!(v["cursor_format"], "sanitized_utf8_v1");
    let tail = f
        .read(&id, v["cursor"].as_u64().unwrap() as usize)
        .await
        .unwrap();
    assert_eq!(tail["output"], "");
    assert_eq!(tail["has_more"], false);
}

#[tokio::test]
async fn exited_shell_does_not_leave_output_holding_descendants() {
    let f = Fixture::new().await;
    let id = f
        .start(
            "(sleep 1; touch child-survived) & printf 'done\\n'; exit 0",
            false,
            5,
        )
        .await;
    let v = f.done(&id).await;
    assert_eq!(v["terminal"]["state"], "exited");
    assert_eq!(v["terminal"]["exit_code"], 0);
    assert_eq!(v["output"], "done\n");
    assert!(v["terminal"]["output_error"].is_null());
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(!f.ws.root.join("child-survived").exists());
}

#[tokio::test]
async fn terminal_capacity_is_released_after_cancellation() {
    let f = Fixture::new().await;
    let mut ids = Vec::new();
    for _ in 0..8 {
        ids.push(f.start("sleep 30", false, 10).await);
    }
    let extra = uuid::Uuid::new_v4().to_string();
    let result = f
        .terminals
        .start(&f.config, &f.ws, &json!({"command":"true"}), &extra)
        .await;
    for id in &ids {
        f.terminals
            .cancel(&f.ws, &json!({"terminal_id":id}))
            .await
            .unwrap();
    }
    for id in &ids {
        f.done(id).await;
    }
    assert!(result.unwrap_err().to_string().contains("capacity"));
    let id = f.start("printf 'recovered'", false, 5).await;
    assert_eq!(f.done(&id).await["output"], "recovered");
}

#[tokio::test]
async fn legacy_raw_logs_keep_redaction_and_restart_reports_runtime_loss() {
    let f = Fixture::new().await;
    let id = uuid::Uuid::new_v4().to_string();
    f.store.put("terminal", &id, &json!({"id":id,"workspace_id":f.ws.id,"state":"running","exit_code":null,"output_truncated":false,"created_at":0}), i64::MAX).await.unwrap();
    std::fs::write(
        f.config
            .state_dir
            .join("terminals")
            .join(format!("{id}.log")),
        format!("{} token=synthetic\n", f.config.device_token),
    )
    .unwrap();
    let restarted = Terminals::new(
        f.store.clone(),
        f.config.state_dir.join("terminals"),
        f.config.device_token.clone(),
    )
    .await
    .unwrap();
    let v = restarted
        .read(&f.ws, &json!({"terminal_id":id}))
        .await
        .unwrap();
    assert_eq!(v["terminal"]["state"], "runtime_lost");
    assert_eq!(v["output"], "[REDACTED] token=[REDACTED]\n");
    assert_eq!(v["cursor_format"], "legacy_transformed");
    assert!(
        restarted
            .input(&f.ws, &json!({"terminal_id":id,"text":"no"}))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn terminal_output_and_controls_remain_workspace_bound() {
    let f = Fixture::new().await;
    let id = f.start("sleep 30", true, 5).await;
    let mut other = f.ws.clone();
    other.id = random();
    assert!(
        f.terminals
            .read(&other, &json!({"terminal_id":id}))
            .await
            .is_err()
    );
    assert!(
        f.terminals
            .input(&other, &json!({"terminal_id":id,"text":"no"}))
            .await
            .is_err()
    );
    assert!(
        f.terminals
            .cancel(&other, &json!({"terminal_id":id}))
            .await
            .is_err()
    );
    f.terminals
        .cancel(&f.ws, &json!({"terminal_id":id}))
        .await
        .unwrap();
    f.done(&id).await;
}

#[tokio::test]
async fn missing_logs_are_errors_not_silent_empty_success() {
    let f = Fixture::new().await;
    let id = f.start("printf 'done'", false, 5).await;
    f.done(&id).await;
    std::fs::remove_file(
        f.config
            .state_dir
            .join("terminals")
            .join(format!("{id}.log")),
    )
    .unwrap();
    assert!(
        f.read(&id, 0)
            .await
            .unwrap_err()
            .to_string()
            .contains("unavailable")
    );
}
