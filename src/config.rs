use std::{fs, net::SocketAddr, path::Path};

use serde::Deserialize;
use url::Url;

use crate::error::AppError;

const CONFIG_VERSION: u32 = 1;
const DEFAULT_LISTEN: &str = "127.0.0.1:8080";
const DEFAULT_ROOT_TAG: &str = "oci-auto-http-tree-v1";

#[derive(Clone, Debug)]
pub struct Config {
    pub listen: SocketAddr,
    pub mounts: Vec<Mount>,
}

#[derive(Clone, Debug)]
#[allow(dead_code)] // consumed by routing and OCI transport in subsequent implementation issues
pub struct Mount {
    pub name: String,
    /// Canonical encoded absolute URL path, with no trailing slash except `/`.
    pub prefix: String,
    /// Decoded components used for segment-boundary route matching.
    pub prefix_components: Vec<String>,
    pub registry: Url,
    pub repository: String,
    pub root_tag: String,
    /// Canonical HTTPS authorities (`host:port`) permitted for blob redirects.
    pub redirect_allowlist: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    version: u32,
    #[serde(default)]
    server: RawServer,
    #[serde(default)]
    mounts: Vec<RawMount>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawServer {
    listen: Option<String>,
}

impl Default for RawServer {
    fn default() -> Self {
        Self {
            listen: Some(DEFAULT_LISTEN.to_owned()),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMount {
    name: String,
    prefix: String,
    registry: String,
    repository: String,
    #[serde(default)]
    root_tag: Option<String>,
    #[serde(default)]
    redirect_allowlist: Vec<String>,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, AppError> {
        let contents = fs::read_to_string(path)
            .map_err(|_| invalid_config(path, "could not read configuration file"))?;
        Self::parse(path, &contents)
    }

    pub fn parse(path: &Path, contents: &str) -> Result<Self, AppError> {
        let raw: RawConfig = toml::from_str(contents)
            .map_err(|error| invalid_config(path, &toml_error_location(contents, &error)))?;

        if raw.version != CONFIG_VERSION {
            return Err(invalid_config(
                path,
                "unsupported configuration version; expected version 1",
            ));
        }

        let listen = raw
            .server
            .listen
            .as_deref()
            .unwrap_or(DEFAULT_LISTEN)
            .parse::<SocketAddr>()
            .map_err(|_| invalid_config(path, "server.listen must be a valid IP:PORT address"))?;

        if raw.mounts.is_empty() {
            return Err(invalid_config(
                path,
                "at least one [[mounts]] entry is required",
            ));
        }

        let mut mounts = Vec::with_capacity(raw.mounts.len());
        let mut names = Vec::with_capacity(raw.mounts.len());
        let mut prefixes: Vec<Vec<String>> = Vec::with_capacity(raw.mounts.len());

        for (index, raw_mount) in raw.mounts.into_iter().enumerate() {
            if !valid_mount_name(&raw_mount.name) {
                return Err(invalid_config(
                    path,
                    &format!("mount {} has an invalid name", index + 1),
                ));
            }
            if names.iter().any(|name| name == &raw_mount.name) {
                return Err(invalid_config(path, "mount names must be unique"));
            }
            names.push(raw_mount.name.clone());

            let prefix_components =
                decode_canonical_prefix(&raw_mount.prefix).ok_or_else(|| {
                    invalid_config(
                        path,
                        &format!(
                            "mount {} has an invalid or non-canonical URL prefix",
                            index + 1
                        ),
                    )
                })?;
            if prefixes.iter().any(|existing| {
                existing.starts_with(&prefix_components) || prefix_components.starts_with(existing)
            }) {
                return Err(invalid_config(
                    path,
                    "mount URL prefixes must be unique and non-overlapping",
                ));
            }
            prefixes.push(prefix_components.clone());

            let registry = validate_registry(&raw_mount.registry).ok_or_else(|| {
                invalid_config(
                    path,
                    &format!(
                        "mount {} registry must be an HTTPS origin without credentials",
                        index + 1
                    ),
                )
            })?;
            if !valid_repository(&raw_mount.repository) {
                return Err(invalid_config(
                    path,
                    &format!("mount {} has an invalid OCI repository name", index + 1),
                ));
            }

            let root_tag = raw_mount
                .root_tag
                .unwrap_or_else(|| DEFAULT_ROOT_TAG.to_owned());
            if !valid_tag(&root_tag) {
                return Err(invalid_config(
                    path,
                    &format!("mount {} has an invalid OCI root tag", index + 1),
                ));
            }

            let mut redirect_allowlist = Vec::with_capacity(raw_mount.redirect_allowlist.len());
            for authority in raw_mount.redirect_allowlist {
                let canonical = canonical_https_authority(&authority).ok_or_else(|| {
                    invalid_config(
                        path,
                        &format!(
                            "mount {} has an invalid HTTPS redirect authority",
                            index + 1
                        ),
                    )
                })?;
                if redirect_allowlist.contains(&canonical) {
                    return Err(invalid_config(
                        path,
                        &format!("mount {} has duplicate redirect authorities", index + 1),
                    ));
                }
                redirect_allowlist.push(canonical);
            }

            mounts.push(Mount {
                name: raw_mount.name,
                prefix: raw_mount.prefix,
                prefix_components,
                registry,
                repository: raw_mount.repository,
                root_tag,
                redirect_allowlist,
            });
        }

        Ok(Self { listen, mounts })
    }
}

fn invalid_config(path: &Path, reason: &str) -> AppError {
    AppError::InvalidConfig {
        path: path.to_path_buf(),
        reason: reason.to_owned(),
    }
}

fn toml_error_location(contents: &str, error: &toml::de::Error) -> String {
    let Some(span) = error.span() else {
        return "invalid TOML syntax".to_owned();
    };
    let offset = span.start.min(contents.len());
    let prefix = &contents.as_bytes()[..offset];
    let line = prefix.iter().filter(|byte| **byte == b'\n').count() + 1;
    let column = prefix
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(prefix.len() + 1, |newline| prefix.len() - newline);
    format!("invalid TOML syntax at line {line}, column {column}")
}

fn valid_mount_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty()
        || bytes.len() > 64
        || !(bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        || !(bytes[bytes.len() - 1].is_ascii_lowercase() || bytes[bytes.len() - 1].is_ascii_digit())
    {
        return false;
    }
    let mut previous_was_hyphen = false;
    for byte in &bytes[1..] {
        if *byte == b'-' {
            if previous_was_hyphen {
                return false;
            }
            previous_was_hyphen = true;
        } else if byte.is_ascii_lowercase() || byte.is_ascii_digit() {
            previous_was_hyphen = false;
        } else {
            return false;
        }
    }
    true
}

fn validate_registry(value: &str) -> Option<Url> {
    if value.contains('@') {
        return None;
    }
    let url = Url::parse(value).ok()?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    Some(url)
}

fn canonical_https_authority(value: &str) -> Option<String> {
    if value.is_empty()
        || value
            .chars()
            .any(|character| matches!(character, '/' | '?' | '#' | '@' | '\\'))
    {
        return None;
    }
    let url = Url::parse(&format!("https://{value}/")).ok()?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    Some(format!(
        "{}:{}",
        url.host_str()?,
        url.port_or_known_default()?
    ))
}

fn valid_repository(repository: &str) -> bool {
    if repository.is_empty() || repository.len() > 255 {
        return false;
    }
    repository.split('/').all(valid_repository_component)
}

fn valid_repository_component(component: &str) -> bool {
    let bytes = component.as_bytes();
    if bytes.is_empty()
        || !is_lower_alphanumeric(bytes[0])
        || !is_lower_alphanumeric(*bytes.last().unwrap())
    {
        return false;
    }

    let mut previous_was_alphanumeric = true;
    for byte in &bytes[1..] {
        if is_lower_alphanumeric(*byte) {
            previous_was_alphanumeric = true;
        } else if matches!(*byte, b'.' | b'_' | b'-') && previous_was_alphanumeric {
            previous_was_alphanumeric = false;
        } else {
            return false;
        }
    }
    previous_was_alphanumeric
}

fn is_lower_alphanumeric(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit()
}

fn valid_tag(tag: &str) -> bool {
    let bytes = tag.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && (bytes[0].is_ascii_alphanumeric() || bytes[0] == b'_')
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'.' | b'-'))
}

fn decode_canonical_prefix(prefix: &str) -> Option<Vec<String>> {
    if prefix == "/" {
        return Some(Vec::new());
    }
    if !prefix.starts_with('/')
        || prefix.ends_with('/')
        || prefix
            .chars()
            .any(|character| matches!(character, '?' | '#'))
    {
        return None;
    }

    let raw_components: Vec<&str> = prefix[1..].split('/').collect();
    if raw_components.is_empty() || raw_components.len() > 128 {
        return None;
    }

    let mut decoded = Vec::with_capacity(raw_components.len());
    let mut total_bytes = 0usize;
    for raw in raw_components {
        if raw.is_empty() || !raw.is_ascii() {
            return None;
        }
        let mut bytes = Vec::with_capacity(raw.len());
        let raw_bytes = raw.as_bytes();
        let mut index = 0;
        while index < raw_bytes.len() {
            if raw_bytes[index] == b'%' {
                if index + 2 >= raw_bytes.len() {
                    return None;
                }
                let high = hex_value(raw_bytes[index + 1])?;
                let low = hex_value(raw_bytes[index + 2])?;
                bytes.push((high << 4) | low);
                index += 3;
            } else {
                let byte = raw_bytes[index];
                if !is_unreserved(byte) {
                    return None;
                }
                bytes.push(byte);
                index += 1;
            }
        }

        if bytes.is_empty() || bytes.len() > 255 || bytes.contains(&b'/') || bytes.contains(&b'\\')
        {
            return None;
        }
        let component = std::str::from_utf8(&bytes).ok()?;
        if component == "." || component == ".." || component.chars().any(is_control) {
            return None;
        }
        if encode_component(&bytes) != raw {
            return None;
        }

        total_bytes += bytes.len();
        if !decoded.is_empty() {
            total_bytes += 1;
        }
        if total_bytes > 4096 {
            return None;
        }
        decoded.push(component.to_owned());
    }
    Some(decoded)
}

fn encode_component(bytes: &[u8]) -> String {
    let mut encoded = String::new();
    for byte in bytes {
        if is_unreserved(*byte) {
            encoded.push(char::from(*byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(hex_digit(byte >> 4)));
            encoded.push(char::from(hex_digit(byte & 0x0f)));
        }
    }
    encoded
}

fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn hex_digit(value: u8) -> u8 {
    match value {
        0..=9 => b'0' + value,
        10..=15 => b'A' + value - 10,
        _ => unreachable!("hex nibbles are less than 16"),
    }
}

fn is_control(character: char) -> bool {
    matches!(character as u32, 0x00..=0x1f | 0x7f..=0x9f)
}
