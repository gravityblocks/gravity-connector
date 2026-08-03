fn main() -> Result<(), Box<dyn std::error::Error>> {
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
            "mev-protos/searcher.proto",
            "mev-protos/shredstream.proto",
        ],
        &["mev-protos"],
    )?;
    println!("cargo:rerun-if-changed=mev-protos");
    Ok(())
}
