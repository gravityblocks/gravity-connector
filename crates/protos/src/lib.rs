#![allow(warnings)]
// To re-generate the bindings run: REGENERATE_PROTO=1 cargo build
pub mod auth {
    include!("generated/auth.rs");
}

pub mod packet {
    include!("generated/packet.rs");
}

pub mod shared {
    include!("generated/shared.rs");
}

pub mod bundle {
    include!("generated/bundle.rs");
}

pub mod block_engine {
    include!("generated/block_engine.rs");
}

pub mod searcher {
    include!("generated/searcher.rs");
}

pub mod shredstream {
    include!("generated/shredstream.rs");
}
