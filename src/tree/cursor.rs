use std::sync::OnceLock;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::{
    hmac,
    rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize};

use crate::registry::Digest;

use super::{MAX_LISTING_PAGE_SIZE, StoragePath, TreeError};

const CURSOR_VERSION: u32 = 1;
const MAX_CURSOR_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CursorPayload {
    pub version: u32,
    pub root_digest: String,
    pub directory_digest: String,
    pub directory: Vec<String>,
    pub last_name: String,
    pub limit: u32,
}

impl CursorPayload {
    pub fn new(
        root: &Digest,
        directory_digest: &Digest,
        directory: &StoragePath,
        last_name: &str,
        limit: usize,
    ) -> Result<Self, TreeError> {
        let limit = u32::try_from(limit).map_err(|_| TreeError::InvalidCursor)?;
        let payload = Self {
            version: CURSOR_VERSION,
            root_digest: root.to_string(),
            directory_digest: directory_digest.to_string(),
            directory: directory.components().to_vec(),
            last_name: last_name.to_owned(),
            limit,
        };
        payload.validate()?;
        Ok(payload)
    }

    pub fn validate(&self) -> Result<(), TreeError> {
        if self.version != CURSOR_VERSION
            || self.limit == 0
            || self.limit as usize > MAX_LISTING_PAGE_SIZE
        {
            return Err(TreeError::InvalidCursor);
        }
        Digest::parse(&self.root_digest).map_err(|_| TreeError::InvalidCursor)?;
        Digest::parse(&self.directory_digest).map_err(|_| TreeError::InvalidCursor)?;
        StoragePath::from_components(self.directory.clone())
            .map_err(|_| TreeError::InvalidCursor)?;
        StoragePath::from_components(vec![self.last_name.clone()])
            .map_err(|_| TreeError::InvalidCursor)?;
        Ok(())
    }
}

pub(super) fn encode(payload: &CursorPayload) -> Result<String, TreeError> {
    payload.validate()?;
    let bytes = serde_json::to_vec(payload).map_err(|_| TreeError::InvalidCursor)?;
    let signature = hmac::sign(hmac_key()?, &bytes);
    Ok(format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(bytes),
        URL_SAFE_NO_PAD.encode(signature.as_ref())
    ))
}

pub(super) fn decode(token: &str) -> Result<CursorPayload, TreeError> {
    if token.len() > MAX_CURSOR_BYTES {
        return Err(TreeError::InvalidCursor);
    }
    let (payload_part, signature_part) = token.split_once('.').ok_or(TreeError::InvalidCursor)?;
    if signature_part.contains('.') {
        return Err(TreeError::InvalidCursor);
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(payload_part)
        .map_err(|_| TreeError::InvalidCursor)?;
    let signature = URL_SAFE_NO_PAD
        .decode(signature_part)
        .map_err(|_| TreeError::InvalidCursor)?;
    hmac::verify(hmac_key()?, &bytes, &signature).map_err(|_| TreeError::InvalidCursor)?;

    let payload: CursorPayload =
        super::format::parse_unique_json(&bytes).map_err(|_| TreeError::InvalidCursor)?;
    payload.validate()?;
    let canonical = serde_json::to_vec(&payload).map_err(|_| TreeError::InvalidCursor)?;
    if canonical != bytes {
        return Err(TreeError::InvalidCursor);
    }
    Ok(payload)
}

fn hmac_key() -> Result<&'static hmac::Key, TreeError> {
    static HMAC_KEY: OnceLock<Option<hmac::Key>> = OnceLock::new();
    HMAC_KEY
        .get_or_init(|| {
            let mut bytes = [0u8; 32];
            SystemRandom::new()
                .fill(&mut bytes)
                .ok()
                .map(|()| hmac::Key::new(hmac::HMAC_SHA256, &bytes))
        })
        .as_ref()
        .ok_or(TreeError::CryptoFailure)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors_are_opaque_and_detect_mutation() {
        let root = Digest::from_bytes(b"root");
        let directory = Digest::from_bytes(b"directory");
        let path = StoragePath::from_components(vec!["packages".to_owned()]).unwrap();
        let payload = CursorPayload::new(&root, &directory, &path, "first", 10).unwrap();
        let token = encode(&payload).unwrap();
        assert_eq!(decode(&token).unwrap().last_name, "first");

        let mut characters = token.into_bytes();
        let middle = characters.len() / 2;
        characters[middle] ^= 1;
        let altered = String::from_utf8(characters).unwrap();
        assert!(decode(&altered).is_err());
    }
}
