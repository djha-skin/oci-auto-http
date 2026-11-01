use std::io;

use axum::{
    body::Body,
    http::{HeaderMap, StatusCode, header},
    response::Response,
};
use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream};
use oci_auto_http::{
    registry::{Descriptor, Digest},
    tree::{StoragePath, TreeError, TreeFile},
};
use tracing::warn;

use super::{error_response, tree_error_response};

const MAX_CONDITIONAL_HEADER_BYTES: usize = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ByteRange {
    start: u64,
    end: u64,
}

impl ByteRange {
    fn length(self) -> u64 {
        self.end - self.start + 1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedRange {
    Satisfiable(ByteRange),
    Unsatisfiable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeSelection {
    Full,
    Partial(ByteRange),
    Unsatisfiable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EntityTag {
    weak: bool,
    opaque: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EntityTagCondition {
    Any,
    Tags(Vec<EntityTag>),
}

pub(super) async fn get_response(
    file: TreeFile,
    path: &StoragePath,
    headers: &HeaderMap,
) -> Response {
    let size = file.descriptor.size;
    let etag = etag_value(&file.descriptor.digest);
    let content_type = content_type(path);

    if !if_match_passes(headers, &etag) {
        return precondition_failed(&etag);
    }
    if if_none_match_matches(headers, &etag) {
        return not_modified(&etag);
    }

    let range = select_range(headers, size, &etag);
    match range {
        RangeSelection::Unsatisfiable => range_not_satisfiable(size, &etag),
        RangeSelection::Full if size == 0 => match verify_empty_blob(file.body).await {
            Ok(()) => file_response(
                StatusCode::OK,
                size,
                content_type,
                &etag,
                None,
                Body::empty(),
            ),
            Err(error) => tree_error_response(error),
        },
        RangeSelection::Full => file_response(
            StatusCode::OK,
            size,
            content_type,
            &etag,
            None,
            Body::from_stream(withhold_final_chunk(file.body)),
        ),
        RangeSelection::Partial(range) => file_response(
            StatusCode::PARTIAL_CONTENT,
            range.length(),
            content_type,
            &etag,
            Some(format!("bytes {}-{}/{size}", range.start, range.end)),
            Body::from_stream(withhold_range(file.body, range)),
        ),
    }
}

pub(super) fn head_response(
    descriptor: &Descriptor,
    path: &StoragePath,
    headers: &HeaderMap,
) -> Response {
    let size = descriptor.size;
    let etag = etag_value(&descriptor.digest);
    let content_type = content_type(path);

    if !if_match_passes(headers, &etag) {
        return precondition_failed(&etag);
    }
    if if_none_match_matches(headers, &etag) {
        return not_modified(&etag);
    }

    match select_range(headers, size, &etag) {
        RangeSelection::Unsatisfiable => range_not_satisfiable(size, &etag),
        RangeSelection::Full => file_response(
            StatusCode::OK,
            size,
            content_type,
            &etag,
            None,
            Body::empty(),
        ),
        RangeSelection::Partial(range) => file_response(
            StatusCode::PARTIAL_CONTENT,
            range.length(),
            content_type,
            &etag,
            Some(format!("bytes {}-{}/{size}", range.start, range.end)),
            Body::empty(),
        ),
    }
}

fn file_response(
    status: StatusCode,
    content_length: u64,
    content_type: &'static str,
    etag: &str,
    content_range: Option<String>,
    body: Body,
) -> Response {
    let mut response = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, content_length.to_string())
        .header(header::ETAG, etag)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .header(header::CACHE_CONTROL, "private, no-store")
        .header(header::VARY, "Authorization");
    if let Some(content_range) = content_range {
        response = response.header(header::CONTENT_RANGE, content_range);
    }
    response
        .body(body)
        .expect("static file response headers are valid")
}

fn not_modified(etag: &str) -> Response {
    Response::builder()
        .status(StatusCode::NOT_MODIFIED)
        .header(header::ETAG, etag)
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .header(header::CACHE_CONTROL, "private, no-store")
        .header(header::VARY, "Authorization")
        .body(Body::empty())
        .expect("static 304 response headers are valid")
}

fn precondition_failed(etag: &str) -> Response {
    let mut response = error_response(StatusCode::PRECONDITION_FAILED, "precondition failed\n");
    response
        .headers_mut()
        .insert(header::ETAG, header_value(etag));
    response
}

fn range_not_satisfiable(size: u64, etag: &str) -> Response {
    let mut response = error_response(StatusCode::RANGE_NOT_SATISFIABLE, "range not satisfiable\n");
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_RANGE,
        header_value(&format!("bytes */{size}")),
    );
    headers.insert(header::ETAG, header_value(etag));
    headers.insert(header::ACCEPT_RANGES, header_value("bytes"));
    response
}

fn header_value(value: &str) -> axum::http::HeaderValue {
    axum::http::HeaderValue::from_str(value).expect("generated HTTP header value is valid")
}

fn etag_value(digest: &Digest) -> String {
    let hex = digest
        .as_str()
        .strip_prefix("sha256:")
        .expect("tree file digest is a validated sha256 digest");
    format!("\"sha256-{hex}\"")
}

fn content_type(path: &StoragePath) -> &'static str {
    let extension = path
        .name()
        .and_then(|name| name.rsplit_once('.').map(|(_, ext)| ext));
    let Some(extension) = extension else {
        return "application/octet-stream";
    };
    if extension.eq_ignore_ascii_case("html") || extension.eq_ignore_ascii_case("htm") {
        "text/html; charset=utf-8"
    } else if extension.eq_ignore_ascii_case("css") {
        "text/css; charset=utf-8"
    } else if extension.eq_ignore_ascii_case("js") {
        "text/javascript; charset=utf-8"
    } else if extension.eq_ignore_ascii_case("json") {
        "application/json"
    } else if extension.eq_ignore_ascii_case("xml") {
        "application/xml"
    } else if extension.eq_ignore_ascii_case("svg") {
        "image/svg+xml"
    } else if extension.eq_ignore_ascii_case("txt") {
        "text/plain; charset=utf-8"
    } else if extension.eq_ignore_ascii_case("pdf") {
        "application/pdf"
    } else if extension.eq_ignore_ascii_case("png") {
        "image/png"
    } else if extension.eq_ignore_ascii_case("jpg") || extension.eq_ignore_ascii_case("jpeg") {
        "image/jpeg"
    } else if extension.eq_ignore_ascii_case("gif") {
        "image/gif"
    } else if extension.eq_ignore_ascii_case("webp") {
        "image/webp"
    } else if extension.eq_ignore_ascii_case("wasm") {
        "application/wasm"
    } else if extension.eq_ignore_ascii_case("tar") {
        "application/x-tar"
    } else if extension.eq_ignore_ascii_case("gz") {
        "application/gzip"
    } else if extension.eq_ignore_ascii_case("zip") {
        "application/zip"
    } else {
        "application/octet-stream"
    }
}

fn if_match_passes(headers: &HeaderMap, etag: &str) -> bool {
    match parse_etag_header(headers, &header::IF_MATCH) {
        Ok(None) | Ok(Some(EntityTagCondition::Any)) => true,
        Ok(Some(EntityTagCondition::Tags(tags))) => {
            let opaque = etag_opaque(etag);
            tags.iter()
                .any(|tag| !tag.weak && tag.opaque.as_slice() == opaque.as_bytes())
        }
        Err(()) => false,
    }
}

fn if_none_match_matches(headers: &HeaderMap, etag: &str) -> bool {
    match parse_etag_header(headers, &header::IF_NONE_MATCH) {
        Ok(None) => false,
        Ok(Some(EntityTagCondition::Any)) => true,
        Ok(Some(EntityTagCondition::Tags(tags))) => {
            let opaque = etag_opaque(etag);
            tags.iter()
                .any(|tag| tag.opaque.as_slice() == opaque.as_bytes())
        }
        Err(()) => false,
    }
}

fn parse_etag_header(
    headers: &HeaderMap,
    name: &header::HeaderName,
) -> Result<Option<EntityTagCondition>, ()> {
    let mut combined = Vec::new();
    for value in headers.get_all(name).iter() {
        let bytes = value.as_bytes();
        let needed = combined
            .len()
            .checked_add(bytes.len())
            .and_then(|length| length.checked_add(1))
            .ok_or(())?;
        if needed > MAX_CONDITIONAL_HEADER_BYTES {
            return Err(());
        }
        if !combined.is_empty() {
            combined.push(b',');
        }
        combined.extend_from_slice(bytes);
    }
    if combined.is_empty() {
        return Ok(None);
    }
    parse_entity_tag_condition_bytes(&combined)
        .map(Some)
        .ok_or(())
}

#[cfg(test)]
fn parse_entity_tag_condition(value: &str) -> Option<EntityTagCondition> {
    parse_entity_tag_condition_bytes(value.as_bytes())
}

fn parse_entity_tag_condition_bytes(bytes: &[u8]) -> Option<EntityTagCondition> {
    if bytes.len() > MAX_CONDITIONAL_HEADER_BYTES {
        return None;
    }
    let mut index = skip_optional_whitespace(bytes, 0);
    if index < bytes.len() && bytes[index] == b'*' {
        index = skip_optional_whitespace(bytes, index + 1);
        return (index == bytes.len()).then_some(EntityTagCondition::Any);
    }

    let mut tags = Vec::new();
    loop {
        index = skip_optional_whitespace(bytes, index);
        while index < bytes.len() && bytes[index] == b',' {
            index = skip_optional_whitespace(bytes, index + 1);
        }
        if index == bytes.len() {
            break;
        }

        let weak = bytes.get(index..index + 2) == Some(b"W/");
        if weak {
            index += 2;
        }
        if bytes.get(index) != Some(&b'\"') {
            return None;
        }
        index += 1;
        let start = index;
        while index < bytes.len() && bytes[index] != b'\"' {
            let byte = bytes[index];
            if byte != 0x21 && !(0x23..=0x7e).contains(&byte) && byte < 0x80 {
                return None;
            }
            index += 1;
        }
        if index == bytes.len() {
            return None;
        }
        let opaque = bytes[start..index].to_vec();
        index += 1;
        tags.push(EntityTag { weak, opaque });
        index = skip_optional_whitespace(bytes, index);
        if index == bytes.len() {
            break;
        }
        if bytes[index] != b',' {
            return None;
        }
        index += 1;
    }
    (!tags.is_empty()).then_some(EntityTagCondition::Tags(tags))
}

fn skip_optional_whitespace(bytes: &[u8], mut index: usize) -> usize {
    while bytes
        .get(index)
        .is_some_and(|byte| matches!(byte, b' ' | b'\t'))
    {
        index += 1;
    }
    index
}

fn etag_opaque(etag: &str) -> &str {
    etag.strip_prefix('\"')
        .and_then(|value| value.strip_suffix('\"'))
        .expect("generated ETag is quoted")
}

fn select_range(headers: &HeaderMap, size: u64, etag: &str) -> RangeSelection {
    let mut values = headers.get_all(header::RANGE).iter();
    let Some(value) = values.next() else {
        return RangeSelection::Full;
    };
    if values.next().is_some() {
        return RangeSelection::Full;
    }
    let Ok(value) = value.to_str() else {
        return RangeSelection::Full;
    };
    let Some(parsed) = parse_byte_range(value, size) else {
        return RangeSelection::Full;
    };
    if headers.contains_key(header::IF_RANGE) && !if_range_matches(headers, etag) {
        return RangeSelection::Full;
    }
    match parsed {
        ParsedRange::Satisfiable(range) => RangeSelection::Partial(range),
        ParsedRange::Unsatisfiable => RangeSelection::Unsatisfiable,
    }
}

fn if_range_matches(headers: &HeaderMap, etag: &str) -> bool {
    let mut values = headers.get_all(header::IF_RANGE).iter();
    let Some(value) = values.next() else {
        return true;
    };
    if values.next().is_some() {
        return false;
    }
    match parse_entity_tag_condition_bytes(value.as_bytes()) {
        Some(EntityTagCondition::Tags(tags)) if tags.len() == 1 => {
            let tag = &tags[0];
            !tag.weak && tag.opaque.as_slice() == etag_opaque(etag).as_bytes()
        }
        _ => false,
    }
}

fn parse_byte_range(value: &str, size: u64) -> Option<ParsedRange> {
    let (unit, value) = value.trim().split_once('=')?;
    if !unit.trim().eq_ignore_ascii_case("bytes") || value.contains(',') {
        return None;
    }
    let (first, last) = value.trim().split_once('-')?;
    if last.contains('-') {
        return None;
    }
    if first.is_empty() {
        let suffix = last.trim().parse::<u64>().ok()?;
        if suffix == 0 || size == 0 {
            return Some(ParsedRange::Unsatisfiable);
        }
        let length = suffix.min(size);
        return Some(ParsedRange::Satisfiable(ByteRange {
            start: size - length,
            end: size - 1,
        }));
    }

    let start = first.trim().parse::<u64>().ok()?;
    if size == 0 || start >= size {
        return Some(ParsedRange::Unsatisfiable);
    }
    let end = if last.trim().is_empty() {
        size - 1
    } else {
        let end = last.trim().parse::<u64>().ok()?;
        if end < start {
            return Some(ParsedRange::Unsatisfiable);
        }
        end.min(size - 1)
    };
    Some(ParsedRange::Satisfiable(ByteRange { start, end }))
}

async fn verify_empty_blob(
    mut body: impl Stream<Item = Result<Bytes, TreeError>> + Unpin,
) -> Result<(), TreeError> {
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(bytes) if bytes.is_empty() => {}
            Ok(_) => return Err(TreeError::IntegrityError),
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn withhold_final_chunk(
    body: impl Stream<Item = Result<Bytes, TreeError>> + Unpin + Send + 'static,
) -> impl Stream<Item = Result<Bytes, io::Error>> + Send + 'static {
    stream::unfold(
        (body, None, false),
        |(mut body, mut held, done)| async move {
            if done {
                return None;
            }
            loop {
                match body.next().await {
                    Some(Ok(bytes)) if bytes.is_empty() => continue,
                    Some(Ok(bytes)) => {
                        if let Some(previous) = held.replace(bytes) {
                            return Some((Ok(previous), (body, held, false)));
                        }
                    }
                    Some(Err(_)) => {
                        warn!("OCI file stream failed digest/size verification or transport");
                        return Some((
                            Err(io::Error::other("verified OCI content stream failed")),
                            (body, None, true),
                        ));
                    }
                    None => {
                        return held.map(|last| (Ok(last), (body, None, true)));
                    }
                }
            }
        },
    )
}

fn withhold_range(
    body: impl Stream<Item = Result<Bytes, TreeError>> + Unpin + Send + 'static,
    range: ByteRange,
) -> impl Stream<Item = Result<Bytes, io::Error>> + Send + 'static {
    stream::unfold(
        (body, 0_u64, None, false),
        move |(mut body, mut offset, mut held, done)| async move {
            if done {
                return None;
            }
            loop {
                match body.next().await {
                    Some(Ok(bytes)) if bytes.is_empty() => continue,
                    Some(Ok(bytes)) => {
                        let chunk_start = offset;
                        let Some(next_offset) = chunk_start.checked_add(bytes.len() as u64) else {
                            return Some((
                                Err(io::Error::other("verified OCI content stream overflowed")),
                                (body, offset, None, true),
                            ));
                        };
                        let overlap_start = chunk_start.max(range.start);
                        let overlap_end = next_offset.min(range.end + 1);
                        if overlap_start < overlap_end {
                            let start = (overlap_start - chunk_start) as usize;
                            let end = (overlap_end - chunk_start) as usize;
                            let selected = &bytes[start..end];
                            let mut output =
                                Vec::with_capacity(selected.len() + usize::from(held.is_some()));
                            if let Some(previous) = held.take() {
                                output.push(previous);
                            }
                            if selected.len() > 1 {
                                output.extend_from_slice(&selected[..selected.len() - 1]);
                                held = selected.last().copied();
                            } else {
                                held = selected.first().copied();
                            }
                            if !output.is_empty() {
                                return Some((
                                    Ok(Bytes::from(output)),
                                    (body, next_offset, held, false),
                                ));
                            }
                        }
                        offset = next_offset;
                    }
                    Some(Err(_)) => {
                        warn!("OCI range stream failed digest/size verification or transport");
                        return Some((
                            Err(io::Error::other("verified OCI content stream failed")),
                            (body, offset, None, true),
                        ));
                    }
                    None => {
                        return held
                            .map(|last| (Ok(Bytes::from(vec![last])), (body, offset, None, true)));
                    }
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;
    use futures_util::TryStreamExt;

    use super::*;

    #[test]
    fn maps_mime_types_by_case_insensitive_final_extension() {
        for (path, expected) in [
            ("index.HTML", "text/html; charset=utf-8"),
            ("site.css", "text/css; charset=utf-8"),
            ("image.JpEg", "image/jpeg"),
            ("archive.tar.gz", "application/gzip"),
            ("unknown.bin", "application/octet-stream"),
            ("no-extension", "application/octet-stream"),
        ] {
            assert_eq!(
                content_type(&StoragePath::parse_relative(path).unwrap()),
                expected
            );
        }
    }

    #[test]
    fn parses_single_byte_ranges_and_ignores_malformed_or_multiple_ranges() {
        assert_eq!(
            parse_byte_range("bytes=2-5", 10),
            Some(ParsedRange::Satisfiable(ByteRange { start: 2, end: 5 }))
        );
        assert_eq!(
            parse_byte_range("bytes=7-", 10),
            Some(ParsedRange::Satisfiable(ByteRange { start: 7, end: 9 }))
        );
        assert_eq!(
            parse_byte_range("bytes=-3", 10),
            Some(ParsedRange::Satisfiable(ByteRange { start: 7, end: 9 }))
        );
        assert_eq!(
            parse_byte_range("bytes=-30", 10),
            Some(ParsedRange::Satisfiable(ByteRange { start: 0, end: 9 }))
        );
        assert_eq!(
            parse_byte_range("bytes=10-", 10),
            Some(ParsedRange::Unsatisfiable)
        );
        assert_eq!(
            parse_byte_range("bytes=5-2", 10),
            Some(ParsedRange::Unsatisfiable)
        );
        assert_eq!(
            parse_byte_range("bytes=-0", 10),
            Some(ParsedRange::Unsatisfiable)
        );
        assert_eq!(
            parse_byte_range("bytes=0-0", 0),
            Some(ParsedRange::Unsatisfiable)
        );
        assert_eq!(parse_byte_range("items=0-1", 10), None);
        assert_eq!(parse_byte_range("bytes=0-1,4-5", 10), None);
        assert_eq!(parse_byte_range("bytes=abc-def", 10), None);
    }

    #[test]
    fn parses_etag_lists_and_applies_weak_and_strong_comparison() {
        let etag = "\"sha256-0123456789abcdef\"";
        let opaque = etag_opaque(etag);
        let strong = parse_entity_tag_condition("\"other\", \"sha256-0123456789abcdef\"").unwrap();
        let weak = parse_entity_tag_condition("W/\"sha256-0123456789abcdef\"").unwrap();
        assert!(
            matches!(strong, EntityTagCondition::Tags(tags) if tags.iter().any(|tag| !tag.weak && tag.opaque.as_slice() == opaque.as_bytes()))
        );
        assert!(
            matches!(weak, EntityTagCondition::Tags(tags) if tags.iter().any(|tag| tag.weak && tag.opaque.as_slice() == opaque.as_bytes()))
        );
        assert_eq!(
            parse_entity_tag_condition("*"),
            Some(EntityTagCondition::Any)
        );
        assert!(parse_entity_tag_condition("W/invalid").is_none());
        assert!(parse_entity_tag_condition("\"unterminated").is_none());

        let mut headers = HeaderMap::new();
        headers.insert(
            header::IF_MATCH,
            HeaderValue::from_static("W/\"sha256-0123456789abcdef\""),
        );
        assert!(!if_match_passes(&headers, etag));
        headers.insert(
            header::IF_MATCH,
            HeaderValue::from_static("\"different\", \"sha256-0123456789abcdef\""),
        );
        assert!(if_match_passes(&headers, etag));
        headers.remove(header::IF_MATCH);
        headers.insert(
            header::IF_NONE_MATCH,
            HeaderValue::from_static("W/\"sha256-0123456789abcdef\""),
        );
        assert!(if_none_match_matches(&headers, etag));
        headers.insert(
            header::IF_RANGE,
            HeaderValue::from_static("W/\"sha256-0123456789abcdef\""),
        );
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=0-1"));
        assert_eq!(select_range(&headers, 10, etag), RangeSelection::Full);
        headers.insert(header::IF_RANGE, HeaderValue::from_static(etag));
        assert_eq!(
            select_range(&headers, 10, etag),
            RangeSelection::Partial(ByteRange { start: 0, end: 1 })
        );
    }

    #[tokio::test]
    async fn ranges_drain_the_full_blob_and_withhold_the_final_response_byte() {
        let source = stream::iter(vec![
            Ok::<_, TreeError>(Bytes::from_static(b"01")),
            Ok(Bytes::from_static(b"234")),
            Ok(Bytes::from_static(b"56789")),
        ]);
        let body = withhold_range(source, ByteRange { start: 2, end: 5 });
        futures_util::pin_mut!(body);
        let chunks: Vec<Bytes> = body.try_collect().await.unwrap();
        assert_eq!(chunks.concat(), b"2345");

        let corrupted = stream::iter(vec![
            Ok::<_, TreeError>(Bytes::from_static(b"012345")),
            Err(TreeError::IntegrityError),
        ]);
        let body = withhold_range(corrupted, ByteRange { start: 1, end: 3 });
        futures_util::pin_mut!(body);
        let mut delivered = Vec::new();
        let mut failed = false;
        while let Some(chunk) = body.next().await {
            match chunk {
                Ok(bytes) => delivered.extend_from_slice(&bytes),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        assert!(failed);
        assert_eq!(
            delivered, b"12",
            "the final range byte stays withheld on failure"
        );
    }
}
