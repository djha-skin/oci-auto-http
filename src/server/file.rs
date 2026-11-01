use std::{collections::VecDeque, io};

use axum::{
    body::Body,
    http::{HeaderMap, Method, StatusCode, header},
    response::Response,
};
use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream};
use oci_auto_http::{
    config::{Mount, ResolvedPath},
    registry::{Descriptor, Digest},
    tree::{DirectoryEntry, DirectoryPage, EntryKind, StoragePath, TreeError, TreeFile, TreeStore},
};
use tracing::warn;

use super::{error_response, tree_error_response};

const MAX_CONDITIONAL_HEADER_BYTES: usize = 8192;
pub(super) const AUTO_INDEX_PAGE_SIZE: usize = 128;

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

pub(super) fn accepts_html(headers: &HeaderMap) -> bool {
    let values = headers.get_all(header::ACCEPT);
    if values.iter().next().is_none() {
        return true;
    }

    let mut exact_html = None;
    let mut text_wildcard = None;
    let mut any_wildcard = None;
    for value in values.iter() {
        let Ok(value) = value.to_str() else {
            return false;
        };
        for media_range in value.split(',') {
            let mut parts = media_range.split(';');
            let media_type = parts.next().unwrap_or_default().trim();
            let mut quality = 1.0_f32;
            for parameter in parts {
                if let Some((name, value)) = parameter.trim().split_once('=')
                    && name.trim().eq_ignore_ascii_case("q")
                {
                    quality = value
                        .trim()
                        .parse::<f32>()
                        .ok()
                        .filter(|quality| quality.is_finite() && (0.0..=1.0).contains(quality))
                        .unwrap_or(0.0);
                }
            }
            let target = if media_type.eq_ignore_ascii_case("text/html") {
                &mut exact_html
            } else if media_type.eq_ignore_ascii_case("text/*") {
                &mut text_wildcard
            } else if media_type == "*/*" {
                &mut any_wildcard
            } else {
                continue;
            };
            *target = Some(target.map_or(quality, |previous: f32| previous.max(quality)));
        }
    }
    exact_html.or(text_wildcard).or(any_wildcard).unwrap_or(0.0) > 0.0
}

pub(super) fn directory_response(
    store: TreeStore,
    path: &ResolvedPath<'_>,
    first_page: DirectoryPage,
    method: Method,
) -> Response {
    let body = if method == Method::HEAD {
        Body::empty()
    } else {
        let root_digest = first_page.root_digest.clone();
        let phase_finished = first_page.next_cursor.is_none();
        let cursor = first_page.next_cursor;
        let pending = first_page
            .entries
            .into_iter()
            .filter(|entry| entry.kind == EntryKind::Directory)
            .collect();
        let state = AutoIndexStreamState {
            store,
            mount: path.mount.clone(),
            directory: path.storage_path.clone(),
            root_digest,
            cursor,
            pending,
            phase: AutoIndexPhase::Directories,
            phase_finished,
            prefix: Some(autoindex_prefix(path)),
            footer_pending: true,
        };
        Body::from_stream(autoindex_stream(state))
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .header(header::CACHE_CONTROL, "private, no-store")
        .header(header::VARY, "Accept, Authorization")
        .header(
            header::CONTENT_SECURITY_POLICY,
            "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
        )
        .header(header::REFERRER_POLICY, "no-referrer")
        .body(body)
        .expect("static autoindex response headers are valid")
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AutoIndexPhase {
    Directories,
    Files,
    Done,
}

struct AutoIndexStreamState {
    store: TreeStore,
    mount: Mount,
    directory: StoragePath,
    root_digest: Digest,
    cursor: Option<String>,
    pending: VecDeque<DirectoryEntry>,
    phase: AutoIndexPhase,
    phase_finished: bool,
    prefix: Option<Bytes>,
    footer_pending: bool,
}

fn autoindex_prefix(path: &ResolvedPath<'_>) -> Bytes {
    let canonical_path = escape_html(&path.canonical_url_path(true));
    let mut prefix = format!(
        r#"<!doctype html>
<html><head><meta charset="utf-8"><title>Index of {canonical_path}</title></head><body>
<h1>Index of {canonical_path}</h1>
<ul>
"#
    );
    if !path.storage_path.is_root() {
        let parent = ResolvedPath {
            mount: path.mount,
            storage_path: path
                .storage_path
                .parent()
                .expect("non-root path has a parent"),
            trailing_slash: true,
        };
        prefix.push_str(&format!(
            r#"<li><a href="{}">../</a></li>
"#,
            escape_html(&parent.canonical_url_path(true))
        ));
    }
    Bytes::from(prefix)
}

fn render_directory_entry(
    mount: &Mount,
    directory: &StoragePath,
    entry: DirectoryEntry,
) -> Result<Bytes, TreeError> {
    let is_directory = entry.kind == EntryKind::Directory;
    let child = directory.child(entry.name.clone())?;
    let path = ResolvedPath {
        mount,
        storage_path: child,
        trailing_slash: is_directory,
    };
    let href = escape_html(&path.canonical_url_path(is_directory));
    let suffix = if is_directory { "/" } else { "" };
    let label = escape_html(&format!("{}{suffix}", entry.name));
    Ok(Bytes::from(format!(
        r#"<li><a href="{href}">{label}</a></li>
"#
    )))
}

fn escape_html(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(character),
        }
    }
    escaped
}

fn autoindex_stream(
    state: AutoIndexStreamState,
) -> impl Stream<Item = Result<Bytes, io::Error>> + Send + 'static {
    stream::unfold(state, |mut state| async move {
        if let Some(prefix) = state.prefix.take() {
            return Some((Ok(prefix), state));
        }
        loop {
            if let Some(entry) = state.pending.pop_front() {
                return match render_directory_entry(&state.mount, &state.directory, entry) {
                    Ok(chunk) => Some((Ok(chunk), state)),
                    Err(_) => {
                        state.phase = AutoIndexPhase::Done;
                        state.pending.clear();
                        state.footer_pending = false;
                        Some((
                            Err(io::Error::other("directory metadata failed validation")),
                            state,
                        ))
                    }
                };
            }
            if state.phase == AutoIndexPhase::Done {
                if state.footer_pending {
                    state.footer_pending = false;
                    return Some((
                        Ok(Bytes::from_static(
                            br#"</ul>
</body></html>
"#,
                        )),
                        state,
                    ));
                }
                return None;
            }
            if state.phase_finished {
                match state.phase {
                    AutoIndexPhase::Directories => {
                        state.phase = AutoIndexPhase::Files;
                        state.cursor = None;
                        state.phase_finished = false;
                    }
                    AutoIndexPhase::Files => {
                        state.phase = AutoIndexPhase::Done;
                        state.footer_pending = true;
                    }
                    AutoIndexPhase::Done => unreachable!(),
                }
                continue;
            }

            let page = state
                .store
                .list_page_pinned(
                    &state.directory,
                    AUTO_INDEX_PAGE_SIZE,
                    &state.root_digest,
                    state.cursor.as_deref(),
                    None,
                )
                .await;
            let page = match page {
                Ok(page) if page.root_digest == state.root_digest => page,
                Ok(_) => {
                    state.phase = AutoIndexPhase::Done;
                    state.pending.clear();
                    state.footer_pending = false;
                    return Some((
                        Err(io::Error::other(
                            "directory snapshot changed during listing",
                        )),
                        state,
                    ));
                }
                Err(_) => {
                    warn!("OCI directory listing failed after response headers");
                    state.phase = AutoIndexPhase::Done;
                    state.pending.clear();
                    state.footer_pending = false;
                    return Some((Err(io::Error::other("OCI directory listing failed")), state));
                }
            };
            let wanted_kind = match state.phase {
                AutoIndexPhase::Directories => EntryKind::Directory,
                AutoIndexPhase::Files => EntryKind::File,
                AutoIndexPhase::Done => unreachable!(),
            };
            state.pending.extend(
                page.entries
                    .into_iter()
                    .filter(|entry| entry.kind == wanted_kind),
            );
            state.cursor = page.next_cursor;
            state.phase_finished = state.cursor.is_none();
        }
    })
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
    parse_single_entity_tag(value.as_bytes())
        .is_some_and(|tag| !tag.weak && tag.opaque.as_slice() == etag_opaque(etag).as_bytes())
}

fn parse_single_entity_tag(bytes: &[u8]) -> Option<EntityTag> {
    if bytes.len() > MAX_CONDITIONAL_HEADER_BYTES {
        return None;
    }
    let mut index = skip_optional_whitespace(bytes, 0);
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
    index = skip_optional_whitespace(bytes, index + 1);
    (index == bytes.len()).then_some(EntityTag { weak, opaque })
}

fn parse_byte_range(value: &str, size: u64) -> Option<ParsedRange> {
    let (unit, value) = value.trim().split_once('=')?;
    if !unit.eq_ignore_ascii_case("bytes") || value.contains(',') {
        return None;
    }
    let (first, last) = value.split_once('-')?;
    if last.contains('-') {
        return None;
    }
    if first.is_empty() {
        let suffix = last.parse::<u64>().ok()?;
        if suffix == 0 || size == 0 {
            return Some(ParsedRange::Unsatisfiable);
        }
        let length = suffix.min(size);
        return Some(ParsedRange::Satisfiable(ByteRange {
            start: size - length,
            end: size - 1,
        }));
    }

    let start = first.parse::<u64>().ok()?;
    if size == 0 || start >= size {
        return Some(ParsedRange::Unsatisfiable);
    }
    let end = if last.is_empty() {
        size - 1
    } else {
        let end = last.parse::<u64>().ok()?;
        if end < start {
            return None;
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
    use oci_auto_http::config::Config;

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
        assert_eq!(parse_byte_range("bytes=5-2", 10), None);
        assert_eq!(parse_byte_range("bytes=0 -1", 10), None);
        assert_eq!(parse_byte_range("bytes=0- 1", 10), None);
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
        headers.insert(
            header::IF_RANGE,
            HeaderValue::from_str(&format!("{etag},")).unwrap(),
        );
        assert_eq!(select_range(&headers, 10, etag), RangeSelection::Full);
    }

    #[test]
    fn directory_accept_negotiation_honors_specific_exclusion() {
        let mut headers = HeaderMap::new();
        assert!(accepts_html(&headers));
        headers.insert(header::ACCEPT, HeaderValue::from_static("text/html"));
        assert!(accepts_html(&headers));
        headers.insert(header::ACCEPT, HeaderValue::from_static("text/*;q=0.5"));
        assert!(accepts_html(&headers));
        headers.insert(header::ACCEPT, HeaderValue::from_static("application/json"));
        assert!(!accepts_html(&headers));
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static("text/html;q=0, */*;q=1"),
        );
        assert!(!accepts_html(&headers));
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static("text/*;q=0, */*;q=1"),
        );
        assert!(!accepts_html(&headers));
    }

    #[test]
    fn autoindex_escapes_names_and_canonicalizes_links() {
        let config = Config::parse(
            std::path::Path::new("autoindex.toml"),
            r#"version = 1

[[mounts]]
name = "packages"
prefix = "/packages"
registry = "https://registry.example"
repository = "team/packages"
"#,
        )
        .unwrap();
        let mount = &config.mounts[0];
        let directory = StoragePath::parse_relative("dir").unwrap();
        let entry = DirectoryEntry {
            name: r#"<script>&".txt"#.to_owned(),
            kind: EntryKind::File,
            digest: Digest::from_bytes(b"content"),
            size: 7,
        };
        let html = String::from_utf8(
            render_directory_entry(mount, &directory, entry)
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(html.contains(r#"href="/packages/dir/%3Cscript%3E%26%22.txt""#));
        assert!(html.contains("&lt;script&gt;&amp;&quot;.txt"));
        assert!(!html.contains("<script>"));
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
