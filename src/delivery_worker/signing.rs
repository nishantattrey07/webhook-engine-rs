use sha2::{Digest, Sha256};

pub fn sha256_hex(body: &[u8]) -> String {
    hex::encode(Sha256::digest(body))
}
