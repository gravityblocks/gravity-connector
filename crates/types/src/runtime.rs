use std::sync::OnceLock;

static BACKGROUND_RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

pub fn init_background_runtime() {
    BACKGROUND_RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .thread_name("bg-tokio")
            .build()
            .unwrap()
    });
}

pub fn background_runtime() -> &'static tokio::runtime::Runtime {
    BACKGROUND_RT.get().expect("background runtime not initialized")
}
