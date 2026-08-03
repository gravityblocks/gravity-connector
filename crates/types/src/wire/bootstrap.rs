use std::{error::Error, fmt};

use flux::type_hash_derive::{TypeHash, type_hash_lock};
use solana_address::Address;
use solana_keypair::Keypair;
use solana_signature::Signature;
use solana_signer::Signer;
use wincode_derive::{SchemaRead, SchemaWrite};

use super::PodPubkey;

pub const BOOTSTRAP_MAGIC: [u8; 8] = *b"SOLGTC01";

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite, TypeHash)]
#[type_hash_lock(hash = 14193140106532823995)]
pub struct ClientHello {
    #[wincode(with = "PodPubkey")]
    #[type_hash(literal = "Address")]
    pub identity: Address,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite, TypeHash)]
#[type_hash_lock(hash = 9175329275788487672)]
pub struct ServerHello {
    pub challenge: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite, TypeHash)]
#[type_hash_lock(hash = 10170949049231151793)]
pub struct AuthProof {
    pub signature: [u8; 64],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite, TypeHash)]
#[type_hash_lock(hash = 13416240893985091315)]
pub enum RejectReason {
    #[wincode(tag = 0)]
    InvalidHello,
    #[wincode(tag = 1)]
    UnknownIdentity,
    #[wincode(tag = 2)]
    InvalidProof,
    #[wincode(tag = 3)]
    Timeout,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub enum BootstrapFrame {
    #[wincode(tag = 0)]
    ClientHello(ClientHello),
    #[wincode(tag = 1)]
    ServerHello(ServerHello),
    #[wincode(tag = 2)]
    AuthProof(AuthProof),
    #[wincode(tag = 3)]
    Accepted,
    #[wincode(tag = 4)]
    Rejected { reason: RejectReason },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapDecodeError {
    NotBootstrap,
    InvalidFrame,
}

impl fmt::Display for BootstrapDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotBootstrap => f.write_str("not a relay bootstrap frame"),
            Self::InvalidFrame => f.write_str("invalid relay bootstrap frame"),
        }
    }
}

impl Error for BootstrapDecodeError {}

#[must_use]
pub fn is_bootstrap_frame(bytes: &[u8]) -> bool {
    bytes.starts_with(&BOOTSTRAP_MAGIC)
}

#[must_use]
pub fn encode_bootstrap_frame(frame: &BootstrapFrame) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(BOOTSTRAP_MAGIC.len() + 128);
    bytes.extend_from_slice(&BOOTSTRAP_MAGIC);
    wincode::serialize_into(&mut bytes, frame).expect("bootstrap frame serialization");
    bytes
}

pub fn decode_bootstrap_frame(bytes: &[u8]) -> Result<BootstrapFrame, BootstrapDecodeError> {
    let payload = bytes.strip_prefix(&BOOTSTRAP_MAGIC).ok_or(BootstrapDecodeError::NotBootstrap)?;
    wincode::deserialize(payload).map_err(|_| BootstrapDecodeError::InvalidFrame)
}

#[must_use]
pub fn sign_auth_proof(keypair: &Keypair, challenge: &[u8; 32]) -> AuthProof {
    let identity = keypair.pubkey();
    AuthProof { signature: keypair.sign_message(&auth_message(&identity, challenge)).into() }
}

#[must_use]
pub fn verify_auth_proof(identity: &Address, proof: &AuthProof, challenge: &[u8; 32]) -> bool {
    Signature::from(proof.signature).verify(identity.as_array(), &auth_message(identity, challenge))
}

fn auth_message(identity: &Address, challenge: &[u8; 32]) -> [u8; 72] {
    let mut message = [0; 72];
    message[..8].copy_from_slice(&BOOTSTRAP_MAGIC);
    message[8..40].copy_from_slice(identity.as_array());
    message[40..].copy_from_slice(challenge);
    message
}
