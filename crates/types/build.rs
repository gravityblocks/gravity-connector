use std::process::Command;

fn git(args: &[&str]) -> String {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn main() {
    // Commit/dirty metadata depends on the whole checkout, including
    // connector-only edits. Resolve Git paths through Git so linked
    // worktrees also watch their shared refs.
    println!("cargo:rerun-if-changed=build.rs");
    for args in [
        [
            "rev-parse",
            "--git-path",
            "HEAD",
            "--git-path",
            "refs",
            "--git-path",
            "packed-refs",
            "--git-path",
            "index",
        ]
        .as_slice(),
        ["ls-files", "--", "../.."].as_slice(),
    ] {
        if let Ok(output) = Command::new("git").args(args).output() {
            if output.status.success() {
                for path in String::from_utf8_lossy(&output.stdout).lines() {
                    if std::path::Path::new(path).exists() {
                        println!("cargo:rerun-if-changed={path}");
                    }
                }
            }
        }
    }

    let mut git_hash = git(&["rev-parse", "HEAD"]);
    // `describe --dirty` rewrites the index, invalidating our own Cargo cache.
    if Command::new("git")
        .args(["--no-optional-locks", "diff", "--quiet", "HEAD", "--"])
        .output()
        .is_ok_and(|output| output.status.code() == Some(1))
    {
        git_hash.push_str("-dirty");
    }

    let git_branch = git(&["rev-parse", "--abbrev-ref", "HEAD"]);

    let output = Command::new("date").args(["-u", "+%Y-%m-%dT%H:%M:%SZ"]).output().unwrap();
    let built_at = String::from_utf8(output.stdout).unwrap().trim().to_string();

    println!("cargo:rustc-env=GIT_HASH={git_hash}");
    println!("cargo:rustc-env=GIT_BRANCH={git_branch}");
    println!("cargo:rustc-env=BUILT_AT={built_at}");
}
