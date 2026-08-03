use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("git").args(["rev-parse", "HEAD"]).output().unwrap();
    let git_hash = String::from_utf8(output.stdout).unwrap();

    let output = Command::new("git").args(["rev-parse", "--abbrev-ref", "HEAD"]).output().unwrap();
    let git_branch = String::from_utf8(output.stdout).unwrap();

    let output = Command::new("date").args(["-u", "+%Y-%m-%dT%H:%M:%SZ"]).output().unwrap();
    let built_at = String::from_utf8(output.stdout).unwrap().trim().to_string();

    println!("cargo:rustc-env=GIT_HASH={git_hash}");
    println!("cargo:rustc-env=GIT_BRANCH={git_branch}");
    println!("cargo:rustc-env=BUILT_AT={built_at}");

    if std::env::var("REGENERATE_PROTO").is_err() {
        return Ok(());
    }
    tonic_prost_build::configure().out_dir("src/generated").compile_protos(
        &[
            "mev-protos/auth.proto",
            "mev-protos/packet.proto",
            "mev-protos/shared.proto",
            "mev-protos/bundle.proto",
            "mev-protos/block_engine.proto",
        ],
        &["mev-protos"],
    )?;
    println!("cargo:rerun-if-changed=mev-protos");
    Ok(())
}
