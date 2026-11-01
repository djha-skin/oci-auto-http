use axum::http::HeaderValue;
use url::Url;

const MAX_CHALLENGE_BYTES: usize = 64 * 1024;

/// Validate upstream WWW-Authenticate fields without rewriting them.
///
/// Header values are relayed byte-for-byte only after the complete RFC 9110
/// challenge grammar has been consumed and any URL-valued Bearer realm is HTTPS.
pub(super) fn valid_challenges(challenges: &[HeaderValue]) -> bool {
    if challenges.is_empty() {
        return false;
    }
    let Some(total_bytes) = challenges.iter().try_fold(0_usize, |total, value| {
        total.checked_add(value.as_bytes().len())
    }) else {
        return false;
    };
    if total_bytes > MAX_CHALLENGE_BYTES {
        return false;
    }
    challenges
        .iter()
        .all(|challenge| ChallengeParser::new(challenge.as_bytes()).parse_field())
}

struct ChallengeParser<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> ChallengeParser<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    fn parse_field(mut self) -> bool {
        self.skip_ows();
        if self.at_end() {
            return false;
        }
        loop {
            if !self.parse_challenge() {
                return false;
            }
            self.skip_ows();
            if self.at_end() {
                return true;
            }
            if !self.consume(b',') {
                return false;
            }
            self.skip_ows();
            if self.at_end() {
                return false;
            }
        }
    }

    fn parse_challenge(&mut self) -> bool {
        let Some(scheme) = self.parse_token() else {
            return false;
        };
        let bearer = scheme.eq_ignore_ascii_case(b"Bearer");
        let mut realm_seen = false;
        let spaces_start = self.position;
        while self.peek() == Some(b' ') {
            self.position += 1;
        }
        if self.position == spaces_start {
            return self.at_end() || self.peek() == Some(b',');
        }
        self.skip_ows();
        if self.at_end() || self.peek() == Some(b',') {
            return true;
        }
        if self.parse_token68() {
            return self.at_end() || self.peek() == Some(b',');
        }

        loop {
            let Some((name, value)) = self.parse_auth_param() else {
                return false;
            };
            if bearer && name.eq_ignore_ascii_case(b"realm") {
                if realm_seen || !valid_bearer_realm(&value) {
                    return false;
                }
                realm_seen = true;
            }
            self.skip_ows();
            if self.peek() != Some(b',') {
                return self.at_end();
            }
            if self.next_element_is_auth_param() {
                self.position += 1;
                self.skip_ows();
                continue;
            }
            return true;
        }
    }

    fn parse_auth_param(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        let name = self.parse_token()?.to_vec();
        self.skip_ows();
        if !self.consume(b'=') {
            return None;
        }
        self.skip_ows();
        let value = self.parse_auth_param_value()?;
        Some((name, value))
    }

    fn parse_auth_param_value(&mut self) -> Option<Vec<u8>> {
        if self.consume(b'"') {
            let mut value = Vec::new();
            loop {
                let byte = self.next_byte()?;
                match byte {
                    b'"' => return Some(value),
                    b'\\' => {
                        let escaped = self.next_byte()?;
                        if !is_quoted_pair(escaped) {
                            return None;
                        }
                        value.push(escaped);
                    }
                    byte if is_quoted_text(byte) => value.push(byte),
                    _ => return None,
                }
            }
        } else {
            Some(self.parse_token()?.to_vec())
        }
    }

    fn parse_token68(&mut self) -> bool {
        let start = self.position;
        while self.peek().is_some_and(is_token68_base) {
            self.position += 1;
        }
        if self.position == start {
            return false;
        }
        while self.consume(b'=') {}
        self.skip_ows();
        if self.at_end() || self.peek() == Some(b',') {
            true
        } else {
            self.position = start;
            false
        }
    }

    fn next_element_is_auth_param(&self) -> bool {
        let mut position = self.position + 1;
        while self.input.get(position).is_some_and(|byte| is_ows(*byte)) {
            position += 1;
        }
        let name_start = position;
        while self.input.get(position).is_some_and(|byte| is_token(*byte)) {
            position += 1;
        }
        if position == name_start {
            return false;
        }
        while self.input.get(position).is_some_and(|byte| is_ows(*byte)) {
            position += 1;
        }
        self.input.get(position) == Some(&b'=')
    }

    fn parse_token(&mut self) -> Option<&'a [u8]> {
        let start = self.position;
        while self.peek().is_some_and(is_token) {
            self.position += 1;
        }
        (self.position > start).then(|| &self.input[start..self.position])
    }

    fn skip_ows(&mut self) {
        while self.peek().is_some_and(is_ows) {
            self.position += 1;
        }
    }

    fn at_end(&self) -> bool {
        self.position == self.input.len()
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.position).copied()
    }

    fn next_byte(&mut self) -> Option<u8> {
        let byte = self.peek()?;
        self.position += 1;
        Some(byte)
    }

    fn consume(&mut self, byte: u8) -> bool {
        if self.peek() == Some(byte) {
            self.position += 1;
            true
        } else {
            false
        }
    }
}

fn valid_bearer_realm(realm: &[u8]) -> bool {
    let Ok(realm) = std::str::from_utf8(realm) else {
        return false;
    };
    match Url::parse(realm) {
        Ok(url) if url.cannot_be_a_base() => true,
        Ok(url) if url.scheme() == "https" => {
            url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && !contains_raw_userinfo(realm)
                && valid_url_syntax(realm.as_bytes())
        }
        Ok(_) => false,
        Err(_) => {
            !realm.starts_with("//")
                && !realm
                    .get(..5)
                    .is_some_and(|scheme| scheme.eq_ignore_ascii_case("http:"))
                && !realm
                    .get(..6)
                    .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https:"))
                && !realm.contains("://")
        }
    }
}

fn contains_raw_userinfo(value: &str) -> bool {
    value
        .split_once("://")
        .and_then(|(_, authority_and_path)| authority_and_path.split(['/', '?', '#']).next())
        .is_some_and(|authority| authority.contains('@'))
}

fn valid_url_syntax(value: &[u8]) -> bool {
    let mut position = 0;
    while position < value.len() {
        let byte = value[position];
        if byte == b'%' {
            if position + 2 >= value.len()
                || !value[position + 1].is_ascii_hexdigit()
                || !value[position + 2].is_ascii_hexdigit()
            {
                return false;
            }
            position += 3;
        } else if is_uri_character(byte) {
            position += 1;
        } else {
            return false;
        }
    }
    true
}

fn is_uri_character(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'-' | b'.'
                | b'_'
                | b'~'
                | b':'
                | b'/'
                | b'?'
                | b'#'
                | b'['
                | b']'
                | b'@'
                | b'!'
                | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
        )
}

fn is_ows(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t')
}

fn is_token68_base(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'+' | b'/')
}

fn is_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn is_quoted_text(byte: u8) -> bool {
    matches!(byte, b'\t' | b' ' | b'!' | 0x23..=0x5b | 0x5d..=0x7e | 0x80..=0xff)
}

fn is_quoted_pair(byte: u8) -> bool {
    matches!(byte, b'\t' | b' ' | 0x21..=0x7e | 0x80..=0xff)
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::valid_challenges;

    #[test]
    fn validates_complete_challenge_lists_without_rewriting_values() {
        for challenge in [
            r#"Basic realm="registry""#,
            "Bearer",
            r#"Bearer realm="example""#,
            r#"Bearer realm="urn:example:registry""#,
            r#"Bearer realm="https://auth.example/token?a=one,two",service="registry",scope="repository:team/file:pull""#,
            r#"Bearer realm="https://auth.example/a%20b""#,
            "Negotiate YIIFhgYGKwYBBQUCoIIFdzCCBXOg",
            r#"Bearer realm = "https://auth.example/token",service = "registry""#,
            r#"Digest realm="registry",qop="auth,auth-int",nonce="n1""#,
            r#"Basic realm="basic", Bearer realm="https://auth.example/token""#,
        ] {
            assert!(
                valid_challenges(&[HeaderValue::from_str(challenge).unwrap()]),
                "{challenge}"
            );
        }
        assert!(valid_challenges(&[
            HeaderValue::from_static("Basic realm=\"registry\""),
            HeaderValue::from_static("Bearer realm=\"https://auth.example/token\"")
        ]));
    }

    #[test]
    fn rejects_malformed_or_insecure_authentication_challenges() {
        for challenge in [
            "",
            "Bearer realm=\"http://auth.example/token\"",
            "Bearer realm=\"https://user:secret@auth.example/token\"",
            "Bearer realm=\"https://@auth.example/token\"",
            "Bearer realm=\"ftp://auth.example/token\"",
            "Bearer realm=\"file:///etc/passwd\"",
            "Bearer realm=\"//auth.example/token\"",
            "Bearer realm=\"https://auth.example/token\",broken=\"unterminated",
            "Bearer realm=\"https://auth.example/token%\"",
            "Bearer realm=\"https://auth.example/a b\"",
            "Bearer realm=\"https://auth.example/token\",",
            "B@sic token",
            "Basic realm=\"registry\" garbage",
            "Basic realm=\"registry\", =bad",
        ] {
            let value = HeaderValue::from_str(challenge).unwrap();
            assert!(!valid_challenges(&[value]), "{challenge}");
        }
        assert!(!valid_challenges(&[]));
    }
}
