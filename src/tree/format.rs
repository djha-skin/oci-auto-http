use std::{collections::BTreeMap, fmt};

use crate::registry::{Digest, OCI_IMAGE_INDEX, OCI_IMAGE_MANIFEST};
use serde::{
    Deserialize, Serialize,
    de::{DeserializeOwned, Error as _, MapAccess, SeqAccess, Visitor},
};
use serde_json::Value;

use super::{MAX_INDEX_BYTES, MAX_INDEX_DESCRIPTORS, StoragePath, TreeError};

pub(crate) const FILE_ARTIFACT_TYPE: &str = "application/vnd.oci-auto-http.file.v1";
pub(crate) const FILE_CONFIG_MEDIA_TYPE: &str = "application/vnd.oci-auto-http.file.config.v1+json";
pub(crate) const FILE_DATA_MEDIA_TYPE: &str = "application/vnd.oci-auto-http.file.data.v1";
pub(crate) const NODE_KIND_ANNOTATION: &str = "io.github.djha-skin.oci-auto-http.node-kind";
pub(crate) const FORMAT_VERSION_ANNOTATION: &str =
    "io.github.djha-skin.oci-auto-http.format-version";
pub(crate) const ENTRY_NAME_ANNOTATION: &str = "io.github.djha-skin.oci-auto-http.entry-name";
pub(crate) const FIRST_NAME_ANNOTATION: &str = "io.github.djha-skin.oci-auto-http.first-name";
pub(crate) const LAST_NAME_ANNOTATION: &str = "io.github.djha-skin.oci-auto-http.last-name";

const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IndexKind {
    Leaf,
    Branch,
}

impl IndexKind {
    fn annotation_value(self) -> &'static str {
        match self {
            Self::Leaf => "directory-leaf",
            Self::Branch => "directory-branch",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct OciDescriptor {
    #[serde(rename = "mediaType")]
    pub media_type: String,
    pub digest: String,
    pub size: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
}

impl OciDescriptor {
    pub fn new(media_type: &str, digest: &Digest, size: u64) -> Self {
        Self {
            media_type: media_type.to_owned(),
            digest: digest.to_string(),
            size,
            annotations: BTreeMap::new(),
        }
    }

    pub fn with_entry_name(mut self, name: &str) -> Self {
        self.annotations
            .insert(ENTRY_NAME_ANNOTATION.to_owned(), name.to_owned());
        self
    }

    pub fn with_range(mut self, first: &str, last: &str) -> Self {
        self.annotations
            .insert(FIRST_NAME_ANNOTATION.to_owned(), first.to_owned());
        self.annotations
            .insert(LAST_NAME_ANNOTATION.to_owned(), last.to_owned());
        self
    }

    pub fn digest(&self) -> Result<Digest, TreeError> {
        Digest::parse(&self.digest).map_err(|_| TreeError::IntegrityError)
    }

    pub fn validate(&self) -> Result<(), TreeError> {
        if self.size > i64::MAX as u64 {
            return Err(TreeError::IntegrityError);
        }
        self.digest()?;
        Ok(())
    }

    pub fn entry_name(&self) -> Option<&str> {
        self.annotations
            .get(ENTRY_NAME_ANNOTATION)
            .map(String::as_str)
    }

    pub fn range(&self) -> Option<(&str, &str)> {
        Some((
            self.annotations.get(FIRST_NAME_ANNOTATION)?.as_str(),
            self.annotations.get(LAST_NAME_ANNOTATION)?.as_str(),
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OciIndex {
    #[serde(rename = "schemaVersion")]
    pub schema_version: u32,
    #[serde(rename = "mediaType")]
    pub media_type: String,
    pub manifests: Vec<OciDescriptor>,
    pub annotations: BTreeMap<String, String>,
}

impl OciIndex {
    pub fn new(kind: IndexKind, manifests: Vec<OciDescriptor>) -> Self {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            NODE_KIND_ANNOTATION.to_owned(),
            kind.annotation_value().to_owned(),
        );
        annotations.insert(
            FORMAT_VERSION_ANNOTATION.to_owned(),
            FORMAT_VERSION.to_string(),
        );
        Self {
            schema_version: 2,
            media_type: OCI_IMAGE_INDEX.to_owned(),
            manifests,
            annotations,
        }
    }

    pub fn kind(&self) -> Result<IndexKind, TreeError> {
        if self.schema_version != 2 || self.media_type != OCI_IMAGE_INDEX {
            return Err(TreeError::IntegrityError);
        }
        if self.annotations.len() != 2
            || self
                .annotations
                .get(FORMAT_VERSION_ANNOTATION)
                .map(String::as_str)
                != Some("1")
        {
            return Err(TreeError::IntegrityError);
        }
        match self
            .annotations
            .get(NODE_KIND_ANNOTATION)
            .map(String::as_str)
        {
            Some("directory-leaf") => Ok(IndexKind::Leaf),
            Some("directory-branch") => Ok(IndexKind::Branch),
            _ => Err(TreeError::IntegrityError),
        }
    }

    pub fn validate(&self) -> Result<IndexKind, TreeError> {
        let kind = self.kind()?;
        if self.manifests.len() > MAX_INDEX_DESCRIPTORS {
            return Err(TreeError::IntegrityError);
        }
        match kind {
            IndexKind::Leaf => self.validate_leaf()?,
            IndexKind::Branch => self.validate_branch()?,
        }
        Ok(kind)
    }

    fn validate_leaf(&self) -> Result<(), TreeError> {
        let mut previous: Option<&str> = None;
        for descriptor in &self.manifests {
            descriptor.validate()?;
            if descriptor.annotations.len() != 1 {
                return Err(TreeError::IntegrityError);
            }
            let name = descriptor.entry_name().ok_or(TreeError::IntegrityError)?;
            validate_entry_name(name)?;
            if previous.is_some_and(|previous| previous.as_bytes() >= name.as_bytes()) {
                return Err(TreeError::IntegrityError);
            }
            if descriptor.media_type != OCI_IMAGE_INDEX
                && descriptor.media_type != OCI_IMAGE_MANIFEST
            {
                return Err(TreeError::IntegrityError);
            }
            previous = Some(name);
        }
        Ok(())
    }

    fn validate_branch(&self) -> Result<(), TreeError> {
        let mut previous_last: Option<&str> = None;
        for descriptor in &self.manifests {
            descriptor.validate()?;
            if descriptor.media_type != OCI_IMAGE_INDEX || descriptor.annotations.len() != 2 {
                return Err(TreeError::IntegrityError);
            }
            let (first, last) = descriptor.range().ok_or(TreeError::IntegrityError)?;
            validate_entry_name(first)?;
            validate_entry_name(last)?;
            if first.as_bytes() > last.as_bytes()
                || previous_last.is_some_and(|previous| previous.as_bytes() >= first.as_bytes())
            {
                return Err(TreeError::IntegrityError);
            }
            previous_last = Some(last);
        }
        if self.manifests.is_empty() {
            return Err(TreeError::IntegrityError);
        }
        Ok(())
    }

    pub fn bounds(&self) -> Result<Option<(&str, &str)>, TreeError> {
        match self.validate()? {
            IndexKind::Leaf => Ok(self.manifests.first().and_then(|first| {
                Some((first.entry_name()?, self.manifests.last()?.entry_name()?))
            })),
            IndexKind::Branch => Ok(self
                .manifests
                .first()
                .and_then(|first| Some((first.range()?.0, self.manifests.last()?.range()?.1)))),
        }
    }

    pub fn serialize(&self) -> Result<Vec<u8>, TreeError> {
        let kind = self.validate()?;
        if kind == IndexKind::Branch && self.manifests.is_empty() {
            return Err(TreeError::IntegrityError);
        }
        let bytes = serde_json::to_vec(self).map_err(|_| TreeError::IntegrityError)?;
        if bytes.len() > MAX_INDEX_BYTES {
            return Err(TreeError::IntegrityError);
        }
        Ok(bytes)
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, TreeError> {
        if bytes.len() > MAX_INDEX_BYTES {
            return Err(TreeError::IntegrityError);
        }
        let index: Self = parse_unique_json(bytes)?;
        index.validate()?;
        Ok(index)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileManifest {
    #[serde(rename = "schemaVersion")]
    pub schema_version: u32,
    #[serde(rename = "mediaType")]
    pub media_type: String,
    #[serde(rename = "artifactType")]
    pub artifact_type: String,
    pub config: OciDescriptor,
    pub layers: Vec<OciDescriptor>,
}

impl FileManifest {
    pub fn new(config: OciDescriptor, layer: OciDescriptor) -> Self {
        Self {
            schema_version: 2,
            media_type: OCI_IMAGE_MANIFEST.to_owned(),
            artifact_type: FILE_ARTIFACT_TYPE.to_owned(),
            config,
            layers: vec![layer],
        }
    }

    pub fn validate(&self) -> Result<(), TreeError> {
        if self.schema_version != 2
            || self.media_type != OCI_IMAGE_MANIFEST
            || self.artifact_type != FILE_ARTIFACT_TYPE
            || self.layers.len() != 1
            || self.config.media_type != FILE_CONFIG_MEDIA_TYPE
            || !self.config.annotations.is_empty()
            || self.layers[0].media_type != FILE_DATA_MEDIA_TYPE
            || !self.layers[0].annotations.is_empty()
        {
            return Err(TreeError::IntegrityError);
        }
        self.config.validate()?;
        self.layers[0].validate()?;
        Ok(())
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, TreeError> {
        if bytes.len() > MAX_INDEX_BYTES {
            return Err(TreeError::IntegrityError);
        }
        let manifest: Self = parse_unique_json(bytes)?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn serialize(&self) -> Result<Vec<u8>, TreeError> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| TreeError::IntegrityError)?;
        if bytes.len() > MAX_INDEX_BYTES {
            return Err(TreeError::IntegrityError);
        }
        Ok(bytes)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileConfig {
    #[serde(rename = "formatVersion")]
    pub format_version: u32,
    pub size: u64,
}

impl FileConfig {
    pub fn new(size: u64) -> Self {
        Self {
            format_version: FORMAT_VERSION,
            size,
        }
    }

    pub fn validate(&self) -> Result<(), TreeError> {
        if self.format_version != FORMAT_VERSION || self.size > i64::MAX as u64 {
            return Err(TreeError::IntegrityError);
        }
        Ok(())
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, TreeError> {
        if bytes.len() > 16 * 1024 {
            return Err(TreeError::IntegrityError);
        }
        let config: Self = parse_unique_json(bytes)?;
        config.validate()?;
        Ok(config)
    }

    pub fn serialize(&self) -> Result<Vec<u8>, TreeError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|_| TreeError::IntegrityError)
    }
}

fn validate_entry_name(name: &str) -> Result<(), TreeError> {
    StoragePath::from_components(vec![name.to_owned()])
        .map(|_| ())
        .map_err(|_| TreeError::IntegrityError)
}

pub(super) fn parse_unique_json<T>(bytes: &[u8]) -> Result<T, TreeError>
where
    T: DeserializeOwned,
{
    let unique: UniqueJsonValue =
        serde_json::from_slice(bytes).map_err(|_| TreeError::IntegrityError)?;
    serde_json::from_value(unique.0).map_err(|_| TreeError::IntegrityError)
}

struct UniqueJsonValue(Value);

impl<'de> Deserialize<'de> for UniqueJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct UniqueVisitor;

        impl<'de> Visitor<'de> for UniqueVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON value with no duplicate object keys")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Value, E> {
                Ok(Value::Bool(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Value, E> {
                Ok(Value::Number(value.into()))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Value, E> {
                Ok(Value::Number(value.into()))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Value, E>
            where
                E: serde::de::Error,
            {
                serde_json::Number::from_f64(value)
                    .map(Value::Number)
                    .ok_or_else(|| E::custom("non-finite JSON number"))
            }

            fn visit_str<E>(self, value: &str) -> Result<Value, E> {
                Ok(Value::String(value.to_owned()))
            }

            fn visit_string<E>(self, value: String) -> Result<Value, E> {
                Ok(Value::String(value))
            }

            fn visit_unit<E>(self) -> Result<Value, E> {
                Ok(Value::Null)
            }

            fn visit_none<E>(self) -> Result<Value, E> {
                Ok(Value::Null)
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element::<UniqueJsonValue>()? {
                    values.push(value.0);
                }
                Ok(Value::Array(values))
            }

            fn visit_map<A>(self, mut object: A) -> Result<Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = serde_json::Map::new();
                while let Some((key, value)) = object.next_entry::<String, UniqueJsonValue>()? {
                    if values.contains_key(&key) {
                        return Err(A::Error::custom("duplicate JSON object key"));
                    }
                    values.insert(key, value.0);
                }
                Ok(Value::Object(values))
            }
        }

        deserializer
            .deserialize_any(UniqueVisitor)
            .map(UniqueJsonValue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_formats_validate_annotations_and_order() {
        let first = OciDescriptor::new(OCI_IMAGE_MANIFEST, &Digest::from_bytes(b"file"), 10)
            .with_entry_name("alpha");
        let second = OciDescriptor::new(OCI_IMAGE_INDEX, &Digest::from_bytes(b"directory"), 20)
            .with_entry_name("beta");
        let bytes = OciIndex::new(IndexKind::Leaf, vec![first, second])
            .serialize()
            .unwrap();
        let parsed = OciIndex::parse(&bytes).unwrap();
        assert_eq!(parsed.kind().unwrap(), IndexKind::Leaf);
        assert_eq!(parsed.bounds().unwrap(), Some(("alpha", "beta")));
    }

    #[test]
    fn branch_ranges_must_be_sorted_and_disjoint() {
        let first = OciDescriptor::new(OCI_IMAGE_INDEX, &Digest::from_bytes(b"one"), 4)
            .with_range("a", "c");
        let overlapping = OciDescriptor::new(OCI_IMAGE_INDEX, &Digest::from_bytes(b"two"), 4)
            .with_range("c", "z");
        let page = OciIndex::new(IndexKind::Branch, vec![first, overlapping]);
        assert_eq!(
            page.validate().unwrap_err().to_string(),
            "reachable OCI tree metadata is corrupt"
        );
        let one_child = OciIndex::new(
            IndexKind::Branch,
            vec![
                OciDescriptor::new(OCI_IMAGE_INDEX, &Digest::from_bytes(b"one"), 4)
                    .with_range("a", "z"),
            ],
        );
        assert_eq!(one_child.validate().unwrap(), IndexKind::Branch);
    }

    #[test]
    fn file_manifest_and_config_are_strict_and_deterministic() {
        let config_bytes = FileConfig::new(3).serialize().unwrap();
        assert_eq!(config_bytes, br#"{"formatVersion":1,"size":3}"#);
        assert_eq!(FileConfig::parse(&config_bytes).unwrap().size, 3);
        assert!(FileConfig::parse(br#"{"formatVersion":1,"size":3,"extra":true}"#).is_err());

        let config = OciDescriptor::new(
            FILE_CONFIG_MEDIA_TYPE,
            &Digest::from_bytes(&config_bytes),
            config_bytes.len() as u64,
        );
        let layer = OciDescriptor::new(FILE_DATA_MEDIA_TYPE, &Digest::from_bytes(b"abc"), 3);
        let manifest = FileManifest::new(config, layer);
        let bytes = manifest.serialize().unwrap();
        let parsed = FileManifest::parse(&bytes).unwrap();
        assert_eq!(parsed, manifest);
    }

    #[test]
    fn leaf_indexes_reject_duplicate_names() {
        let first = OciDescriptor::new(OCI_IMAGE_MANIFEST, &Digest::from_bytes(b"first"), 1)
            .with_entry_name("duplicate");
        let second = OciDescriptor::new(OCI_IMAGE_INDEX, &Digest::from_bytes(b"second"), 2)
            .with_entry_name("duplicate");
        assert!(
            OciIndex::new(IndexKind::Leaf, vec![first, second])
                .validate()
                .is_err()
        );
    }

    #[test]
    fn duplicate_json_keys_are_rejected_at_any_depth() {
        let duplicate_top = br#"{"schemaVersion":2,"schemaVersion":2}"#;
        assert!(parse_unique_json::<Value>(duplicate_top).is_err());
        let duplicate_nested = br#"{"annotations":{"name":"a","name":"b"}}"#;
        assert!(parse_unique_json::<Value>(duplicate_nested).is_err());
    }
}
