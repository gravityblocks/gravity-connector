#![allow(clippy::new_without_default)]

use flux::type_hash_derive::TypeHash;
use rand::Rng;
use rts_alloc::Allocator;
use solana_signature::{SIGNATURE_BYTES, Signature};
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
    const VERSION_PREFIX: u8 = 0x80;
    const V1_PREFIX: u8 = Self::VERSION_PREFIX | 1;

    pub fn new(b: [u8; 16]) -> Self {
        Self(b)
    }

    pub fn new_from_sig(s: Signature) -> Self {
        let mut out = [0u8; Self::LEN];
        out.copy_from_slice(&s.as_array()[..Self::LEN]);
        Self(out)
    }

    /// Read the first signature prefix from a serialized transaction.
    #[inline]
    pub fn try_from_transaction_bytes(bytes: &[u8]) -> Option<Self> {
        let first_byte = *bytes.first()?;
        let signature_offset = match first_byte {
            // Legacy and v0 transactions start with a one-byte compact-u16
            // signature count, followed immediately by their signatures.
            signature_count
                if signature_count != 0 && signature_count & Self::VERSION_PREFIX == 0 =>
            {
                1
            }
            // V1 transactions start with their version and signature count,
            // and place all signatures at the end of the transaction.
            Self::V1_PREFIX => {
                let num_signatures = usize::from(*bytes.get(1)?);
                if num_signatures == 0 {
                    return None;
                }
                let signatures_len = num_signatures.checked_mul(SIGNATURE_BYTES)?;
                bytes.len().checked_sub(signatures_len)?
            }
            _ => return None,
        };
        let signature_end = signature_offset.checked_add(Self::LEN)?;
        let mut out = [0u8; Self::LEN];
        out.copy_from_slice(bytes.get(signature_offset..signature_end)?);
        Some(Self(out))
    }

    /// Read the first 16 bytes of the first signature directly from the
    /// shmem-resident tx.
    ///
    /// # Safety
    /// Caller must guarantee:
    /// - `tx` was allocated by `allocator` and has not been freed.
    /// - The bytes at `tx` are a supported, sigverified Solana transaction.
    pub unsafe fn new_from_allocator(tx: TxBytesOffset, allocator: &Allocator) -> Self {
        let base = unsafe { allocator.ptr_from_offset(tx.0.offset).as_ptr() };
        let bytes = unsafe { core::slice::from_raw_parts(base, tx.0.length) };
        Self::try_from_transaction_bytes(bytes)
            .expect("sigverified transaction must contain a supported signature layout")
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

#[cfg(test)]
mod tests {
    use solana_keypair::Keypair;
    use solana_message::{Message, MessageHeader, VersionedMessage, v0, v1};
    use solana_signer::Signer;
    use solana_transaction::versioned::VersionedTransaction;

    use super::*;

    fn assert_sig_prefix(tx: &VersionedTransaction) {
        let bytes = solana_wincode::serialize(tx).unwrap();
        let expected = SigPrefix::new_from_sig(tx.signatures[0]);

        assert_eq!(SigPrefix::try_from_transaction_bytes(&bytes), Some(expected));
    }

    #[test]
    fn sig_prefix_from_legacy_transaction() {
        let signer = Keypair::new();
        let message = VersionedMessage::Legacy(Message {
            header: MessageHeader { num_required_signatures: 1, ..MessageHeader::default() },
            account_keys: vec![signer.pubkey()],
            ..Message::default()
        });
        let tx = VersionedTransaction::try_new(message, &[&signer]).unwrap();

        assert_sig_prefix(&tx);
    }

    #[test]
    fn sig_prefix_from_v0_transaction() {
        let signer = Keypair::new();
        let message = VersionedMessage::V0(v0::Message {
            header: MessageHeader { num_required_signatures: 1, ..MessageHeader::default() },
            account_keys: vec![signer.pubkey()],
            ..v0::Message::default()
        });
        let tx = VersionedTransaction::try_new(message, &[&signer]).unwrap();

        assert_sig_prefix(&tx);
    }

    #[test]
    fn sig_prefix_from_v1_transaction() {
        let first_signer = Keypair::new();
        let second_signer = Keypair::new();
        let message = VersionedMessage::V1(v1::Message {
            header: MessageHeader { num_required_signatures: 2, ..MessageHeader::default() },
            account_keys: vec![first_signer.pubkey(), second_signer.pubkey()],
            ..v1::Message::default()
        });
        let tx = VersionedTransaction::try_new(message, &[&first_signer, &second_signer]).unwrap();

        assert_sig_prefix(&tx);
    }

    #[test]
    fn sig_prefix_rejects_invalid_or_unknown_layouts() {
        assert_eq!(SigPrefix::try_from_transaction_bytes(&[]), None);
        assert_eq!(SigPrefix::try_from_transaction_bytes(&[0]), None);
        assert_eq!(SigPrefix::try_from_transaction_bytes(&[SigPrefix::V1_PREFIX, 0]), None);
        assert_eq!(
            SigPrefix::try_from_transaction_bytes(&[SigPrefix::VERSION_PREFIX | 2, 1]),
            None
        );
    }
}
