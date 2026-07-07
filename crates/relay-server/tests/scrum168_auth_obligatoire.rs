use std::process::Command;
use std::time::Duration;

const BOOT_DEADLINE: Duration = Duration::from_secs(10);

fn run_relay_with_config(contents: &str, file_stem: &str) -> std::process::Output {
    let cfg = std::env::temp_dir().join(format!("relay-{file_stem}.toml"));
    std::fs::write(&cfg, contents).expect("write test config");
    run_relay(Command::new(env!("CARGO_BIN_EXE_relay")).env("RELAY_CONFIG", &cfg))
}

fn run_relay_without_config_file() -> std::process::Output {
    let missing = std::env::temp_dir().join("relay-scrum168-missing.toml");
    let _ = std::fs::remove_file(&missing);
    run_relay(Command::new(env!("CARGO_BIN_EXE_relay")).env("RELAY_CONFIG", &missing))
}

fn run_relay(command: &mut Command) -> std::process::Output {
    let mut child = command
        .env("RUST_LOG", "off")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn relay binary");

    let deadline = std::time::Instant::now() + BOOT_DEADLINE;
    loop {
        if child.try_wait().expect("poll relay process").is_some() {
            return child.wait_with_output().expect("collect relay output");
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("relay did not exit on invalid config; it booted instead");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn refuses_to_boot_without_auth_section() {
    let output = run_relay_with_config(
        "tcp_addr = \"127.0.0.1:21897\"\n\
         ws_addr = \"127.0.0.1:28097\"\n",
        "scrum168-no-auth",
    );

    assert!(
        !output.status.success(),
        "broker must refuse to boot without [auth]"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.to_lowercase().contains("auth"),
        "boot failure must mention the missing auth config, got: {stderr}"
    );
}

#[test]
fn refuses_to_boot_when_config_file_is_missing() {
    let output = run_relay_without_config_file();

    assert!(
        !output.status.success(),
        "broker must refuse to boot without a config file"
    );
}
