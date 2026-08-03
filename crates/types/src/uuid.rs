#![allow(clippy::new_without_default)]

use flux::type_hash_derive::TypeHash;
use rand::Rng;
use rts_alloc::Allocator;
use solana_signature::Signature;
use uuid::Uuid;
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::order::TxBytesOffset;

wincode::pod_wrapper! {
    unsafe struct PodUuid(Uuid);
}

#[derive(
    Clone,
    Copy,
    Hash,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    SchemaRead,
    SchemaWrite,
    TypeHash,
)]
#[repr(transparent)]
pub struct BatchUuid {
    #[wincode(with = "PodUuid")]
    #[serde(with = "clickhouse::serde::uuid")]
    #[type_hash(literal = "Uuid")]
    inner: Uuid,
}

impl BatchUuid {
    pub fn new() -> Self {
        Self { inner: uuid::Uuid::new_v4() }
    }
}

impl From<uuid::Uuid> for BatchUuid {
    fn from(u: uuid::Uuid) -> Self {
        Self { inner: u }
    }
}

impl std::fmt::Display for BatchUuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl std::fmt::Debug for BatchUuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

/// Ids one mini-block graph. Unique across builders, so a connector serving
/// several builders can trace each graph to its source.
#[derive(
    Clone,
    Copy,
    Hash,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    SchemaRead,
    SchemaWrite,
    TypeHash,
)]
#[repr(transparent)]
pub struct MiniBlockUuid {
    #[wincode(with = "PodUuid")]
    #[serde(with = "clickhouse::serde::uuid")]
    #[type_hash(literal = "Uuid")]
    inner: Uuid,
}

impl MiniBlockUuid {
    pub fn new() -> Self {
        Self { inner: uuid::Uuid::new_v4() }
    }
}

impl std::fmt::Display for MiniBlockUuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl std::fmt::Debug for MiniBlockUuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

/// First 16 bytes of the first transaction signature. Used for builder <->
/// connector transaction identifier
#[derive(
    Clone,
    Copy,
    Hash,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    SchemaRead,
    SchemaWrite,
    TypeHash,
)]
#[allow(clippy::unsafe_derive_deserialize)]
#[repr(transparent)]
pub struct SigPrefix(pub [u8; 16]);

impl std::fmt::Display for SigPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&bs58::encode(self.0).into_string())
    }
}

impl std::fmt::Debug for SigPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl SigPrefix {
    pub const LEN: usize = 16;

    pub fn new(b: [u8; 16]) -> Self {
        Self(b)
    }

    pub fn new_from_sig(s: Signature) -> Self {
        let mut out = [0u8; Self::LEN];
        out.copy_from_slice(&s.as_array()[..Self::LEN]);
        Self(out)
    }

    /// Read the first 16 bytes of the first signature directly from the
    /// shmem-resident tx.
    ///
    /// # Safety
    /// Caller must guarantee:
    /// - `tx` was allocated by `allocator` and has not been freed.
    /// - The bytes at `tx` are a sigverified Solana tx, so `bytes[0]` is the
    ///   compact-u16 sig count (1 byte; Solana caps sigs well under 128) and
    ///   `bytes[1..65]` is the first signature.
    pub unsafe fn new_from_allocator(tx: TxBytesOffset, allocator: &Allocator) -> Self {
        let base = unsafe { allocator.ptr_from_offset(tx.0.offset).as_ptr() };
        let mut out = [0u8; Self::LEN];
        unsafe {
            core::ptr::copy_nonoverlapping(base.add(1), out.as_mut_ptr(), Self::LEN);
        }
        Self(out)
    }
}

/// Jito bundle identifier (SHA256, 32 bytes).
#[derive(
    Clone,
    Copy,
    Hash,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    TypeHash,
    SchemaRead,
    SchemaWrite,
)]
#[repr(transparent)]
pub struct BundleId(pub [u8; 32]);

impl BundleId {
    pub const SYNTHETIC_PREFIX: [u8; 8] = [0xFF; 8];

    pub fn from_hex(s: &str) -> Option<Self> {
        let mut out = [0u8; 32];
        const_hex::decode_to_slice(s, &mut out).ok()?;
        Some(Self(out))
    }

    pub fn new_synthetic() -> Self {
        let mut out = [0u8; 32];
        out[..8].copy_from_slice(&Self::SYNTHETIC_PREFIX);

        rand::rng().fill_bytes(&mut out[8..]);

        Self(out)
    }
}

impl std::fmt::Display for BundleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&const_hex::encode(self.0))
    }
}

impl std::fmt::Debug for BundleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
