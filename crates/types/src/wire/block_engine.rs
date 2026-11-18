//! The login challenge of a block engine that the relay connects to as the
//! validator. The validator signs only `"{identity}-{challenge}"`, for a
//! challenge of exactly [`BlockEngineChallenge::LEN`] ASCII alphanumeric
//! bytes, so the relay can never make it sign anything else.

use flux::type_hash_derive::{TypeHash, type_hash_lock};
use solana_address::Address;
use solana_keypair::Keypair;
use solana_signature::Signature;
use solana_signer::Signer;
use wincode_derive::{SchemaRead, SchemaWrite};

const LEN: usize = 9;

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite, TypeHash)]
#[type_hash_lock(hash = 3610281998414603413)]
pub struct BlockEngineChallenge([u8; LEN]);

impl BlockEngineChallenge {
    pub const LEN: usize = LEN;

    /// None unless `challenge` is [`Self::LEN`] ASCII alphanumeric bytes.
    #[must_use]
    pub fn new(challenge: &[u8]) -> Option<Self> {
        let challenge = Self(challenge.try_into().ok()?);
        challenge.is_valid().then_some(challenge)
    }

    /// A decoded challenge holds any bytes; only a valid one is signed.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.0.iter().all(u8::is_ascii_alphanumeric)
    }

    /// The text the block engine signs in: `"{identity}-{challenge}"`. None
    /// for an invalid challenge.
    #[must_use]
    pub fn login_message(&self, identity: &Address) -> Option<String> {
        let challenge = std::str::from_utf8(&self.0).ok().filter(|_| self.is_valid())?;
        Some(format!("{identity}-{challenge}"))
    }

    /// The identity's signature of [`Self::login_message`]. None for an
    /// invalid challenge.
    #[must_use]
    pub fn sign(&self, keypair: &Keypair) -> Option<[u8; 64]> {
        let message = self.login_message(&keypair.pubkey())?;
        Some(keypair.sign_message(message.as_bytes()).into())
    }

    #[must_use]
    pub fn verify(&self, identity: &Address, signature: &[u8; 64]) -> bool {
        self.login_message(identity).is_some_and(|message| {
            Signature::from(*signature).verify(identity.as_array(), message.as_bytes())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signs_only_alphanumeric_challenges_of_fixed_length() {
        let keypair = Keypair::new();
        let identity = keypair.pubkey();
        let challenge = BlockEngineChallenge::new(b"aZ09bY18c").unwrap();
        assert_eq!(challenge.login_message(&identity).unwrap(), format!("{identity}-aZ09bY18c"));
        let signature = challenge.sign(&keypair).unwrap();
        assert!(challenge.verify(&identity, &signature));
        assert!(!challenge.verify(&Keypair::new().pubkey(), &signature));

        for invalid in [&b"aZ09bY18"[..], b"aZ09bY18cd", b"aZ09bY18-", b"aZ09bY18\0"] {
            assert!(BlockEngineChallenge::new(invalid).is_none());
        }
        // A decoded challenge bypasses `new`.
        let decoded: BlockEngineChallenge = wincode::deserialize(b"aZ09bY18\xff").unwrap();
        assert!(decoded.sign(&keypair).is_none());
        assert!(decoded.login_message(&identity).is_none());
    }
}
