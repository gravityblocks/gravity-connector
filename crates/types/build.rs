use std::{env, process::Command};

fn git_version() -> String {
    Command::new("git")
        .args(["describe", "--tags", "--dirty"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|version| version.trim().to_owned())
        .filter(|version| !version.is_empty())
        .unwrap_or_else(|| env::var("CARGO_PKG_VERSION").unwrap())
}

fn main() {
    let output = Command::new("git").args(["rev-parse", "HEAD"]).output().unwrap();
    let git_hash = String::from_utf8(output.stdout).unwrap();

    let output = Command::new("git").args(["rev-parse", "--abbrev-ref", "HEAD"]).output().unwrap();
    let git_branch = String::from_utf8(output.stdout).unwrap();

    let output = Command::new("date").args(["-u", "+%Y-%m-%dT%H:%M:%SZ"]).output().unwrap();
    let built_at = String::from_utf8(output.stdout).unwrap().trim().to_string();

    // Tags carry a leading `v` that every display site already adds back.
    let version = git_version();
    let version = version.strip_prefix('v').unwrap_or(&version);

    println!("cargo:rustc-env=GIT_VERSION={version}");
    println!("cargo:rustc-env=GIT_HASH={git_hash}");
    println!("cargo:rustc-env=GIT_BRANCH={git_branch}");
    println!("cargo:rustc-env=BUILT_AT={built_at}");
}
