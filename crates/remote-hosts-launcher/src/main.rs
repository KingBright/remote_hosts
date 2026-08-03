//! Stable, minimal launcher for versioned Remote Hosts Windows releases.

use std::{
    env,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::{Command, ExitCode, Stdio},
};

struct LaunchRequest {
    pointer_path: PathBuf,
    child_args: Vec<OsString>,
}

fn main() -> ExitCode {
    match run(env::args_os().skip(1)) {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            eprintln!("remote-hosts-launcher: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(arguments: impl IntoIterator<Item = OsString>) -> Result<u8, String> {
    let request = parse_request(arguments)?;
    let binary = read_binary_pointer(&request.pointer_path)?;
    let status = Command::new(&binary)
        .args(request.child_args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|error| format!("start {}: {error}", binary.display()))?;
    let code = status.code().unwrap_or(1);
    u8::try_from(code).map_err(|_| format!("child returned unsupported exit code {code}"))
}

fn parse_request(arguments: impl IntoIterator<Item = OsString>) -> Result<LaunchRequest, String> {
    let mut arguments = arguments.into_iter();
    let mut pointer_path = None;
    let mut child_args = Vec::new();
    let mut after_separator = false;

    while let Some(argument) = arguments.next() {
        if after_separator {
            child_args.push(argument);
            continue;
        }
        if argument == "--" {
            after_separator = true;
            continue;
        }
        if argument == "--pointer" {
            pointer_path = Some(PathBuf::from(
                arguments
                    .next()
                    .ok_or_else(|| "--pointer requires a path".to_owned())?,
            ));
            continue;
        }
        return Err(format!(
            "unexpected launcher argument {}; use [--pointer PATH] -- <remote-hosts arguments>",
            argument.to_string_lossy()
        ));
    }

    if !after_separator || child_args.is_empty() {
        return Err("missing `-- <remote-hosts arguments>`".to_owned());
    }
    let pointer_path = match pointer_path {
        Some(path) => path,
        None => default_pointer_path()?,
    };
    Ok(LaunchRequest {
        pointer_path,
        child_args,
    })
}

fn default_pointer_path() -> Result<PathBuf, String> {
    let local_app_data = env::var_os("LOCALAPPDATA")
        .ok_or_else(|| "LOCALAPPDATA is unavailable; pass --pointer PATH".to_owned())?;
    Ok(PathBuf::from(local_app_data)
        .join("RemoteHosts")
        .join("config")
        .join("current-binary.txt"))
}

fn read_binary_pointer(path: &Path) -> Result<PathBuf, String> {
    let value = fs::read_to_string(path)
        .map_err(|error| format!("read binary pointer {}: {error}", path.display()))?;
    let value = value.trim_matches(['\u{feff}', '\r', '\n', ' ']);
    if value.is_empty() {
        return Err(format!("binary pointer is empty: {}", path.display()));
    }
    let binary = PathBuf::from(value);
    if !binary.is_file() {
        return Err(format!(
            "pointed Remote Hosts binary does not exist: {}",
            binary.display()
        ));
    }
    Ok(binary)
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::parse_request;

    #[test]
    fn parses_explicit_pointer_and_preserves_child_arguments() -> Result<(), String> {
        let request = parse_request([
            OsString::from("--pointer"),
            OsString::from("C:\\RemoteHosts\\current.txt"),
            OsString::from("--"),
            OsString::from("mcp-stdio"),
            OsString::from("--tool-profile"),
            OsString::from("agent"),
        ])?;

        assert_eq!(
            request.pointer_path,
            std::path::PathBuf::from("C:\\RemoteHosts\\current.txt")
        );
        assert_eq!(
            request.child_args,
            ["mcp-stdio", "--tool-profile", "agent"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>()
        );
        Ok(())
    }

    #[test]
    fn requires_a_child_command() {
        assert!(parse_request([OsString::from("--")]).is_err());
    }
}
