pub const MAX_ALLOCATION_SZ: usize = 4096;
pub const MAX_TXS_PER_BUNDLE: usize = 5;
pub const MAX_TX_SZ: usize = solana_packet::PACKET_DATA_SIZE;
pub const MAX_ALLOCS_PER_BUNDLE: usize =
    (MAX_TX_SZ * MAX_TXS_PER_BUNDLE).div_ceil(MAX_ALLOCATION_SZ);
const ALLOCATOR_SIZE_DIVISOR: u32 = 2 * 1024 * 1024;
// rts-alloc file size should fit u32
pub const MAX_ALLOCATOR_FILE_SIZE: usize =
    (u32::MAX - (u32::MAX % ALLOCATOR_SIZE_DIVISOR)) as usize;

pub const MAX_THREADS: u8 = 32;
pub const MAX_THREADS_USIZE: usize = MAX_THREADS as usize;

pub const MAX_TXS_PER_MESSAGE: usize = 16;
