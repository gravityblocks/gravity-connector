//! The login challenge of a block engine that the relay connects to as the
//! validator. The validator signs only `"{identity}-{challenge}"`, for a
//! challenge of exactly [`BlockEngineChallenge::LEN`] ASCII alphanumeric
//! bytes, so the relay can never make it sign anything else.

use std::mem::MaybeUninit;

use flux::type_hash_derive::{TypeHash, type_hash_lock};
use solana_address::Address;
use solana_keypair::Keypair;
use solana_signature::Signature;
use solana_signer::Signer;
use wincode::{ReadResult, SchemaRead, config::ConfigCore, error::invalid_value, io::Reader};
use wincode_derive::SchemaWrite;

const LEN: usize = 9;

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaWrite, TypeHash)]
#[type_hash_lock(hash = 3610281998414603413)]
pub struct BlockEngineChallenge([u8; LEN]);

impl BlockEngineChallenge {
    pub const LEN: usize = LEN;

    /// None unless `challenge` is [`Self::LEN`] ASCII alphanumeric bytes.
    #[must_use]
    pub fn new(challenge: &[u8]) -> Option<Self> {
        let challenge: [u8; LEN] = challenge.try_into().ok()?;
        is_valid(&challenge).then_some(Self(challenge))
    }

    /// The text the block engine signs in: `"{identity}-{challenge}"`.
    #[must_use]
    pub fn login_message(&self, identity: &Address) -> String {
        let challenge = std::str::from_utf8(&self.0).expect("ASCII alphanumeric");
        format!("{identity}-{challenge}")
    }

    /// The identity's signature of [`Self::login_message`].
    #[must_use]
    pub fn sign(&self, keypair: &Keypair) -> [u8; 64] {
        let message = self.login_message(&keypair.pubkey());
        keypair.sign_message(message.as_bytes()).into()
    }

    #[must_use]
    pub fn verify(&self, identity: &Address, signature: &[u8; 64]) -> bool {
        let message = self.login_message(identity);
        Signature::from(*signature).verify(identity.as_array(), message.as_bytes())
    }
}

// SAFETY: `dst` is written only on success.
unsafe impl<'de, C: ConfigCore> SchemaRead<'de, C> for BlockEngineChallenge {
    type Dst = Self;

    fn read(reader: impl Reader<'de>, dst: &mut MaybeUninit<Self>) -> ReadResult<()> {
        let challenge = <[u8; LEN] as SchemaRead<'de, C>>::get(reader)?;
        if !is_valid(&challenge) {
            return Err(invalid_value("block engine challenge is not ASCII alphanumeric"));
        }
        dst.write(Self(challenge));
        Ok(())
    }
}

fn is_valid(challenge: &[u8; LEN]) -> bool {
    challenge.iter().all(u8::is_ascii_alphanumeric)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signs_only_alphanumeric_challenges_of_fixed_length() {
        let keypair = Keypair::new();
        let identity = keypair.pubkey();
        let challenge = BlockEngineChallenge::new(b"aZ09bY18c").unwrap();
        assert_eq!(challenge.login_message(&identity), format!("{identity}-aZ09bY18c"));
        let signature = challenge.sign(&keypair);
        assert!(challenge.verify(&identity, &signature));
        assert!(!challenge.verify(&Keypair::new().pubkey(), &signature));

        for invalid in [&b"aZ09bY18"[..], b"aZ09bY18cd", b"aZ09bY18-", b"aZ09bY18\0"] {
            assert!(BlockEngineChallenge::new(invalid).is_none());
        }
        let decoded: BlockEngineChallenge = wincode::deserialize(b"aZ09bY18c").unwrap();
        assert_eq!(decoded, challenge);
        assert!(wincode::deserialize::<BlockEngineChallenge>(b"aZ09bY18\xff").is_err());
    }
}
