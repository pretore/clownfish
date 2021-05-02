use std::{error, fmt};
use std::convert::TryFrom;
use std::fmt::Formatter;

use crate::common::{CommonError, required};
use crate::uri::UriError::SchemeInvalidCharacter;

type Result<T> = std::result::Result<T, UriError>;

#[derive(Debug)]
pub enum UriError {
    Common(CommonError),
    /// Scheme names consist of a sequence of characters beginning with a
    /// letter.
    SchemeInvalidFirstCharacter(char),
    /// Scheme names then consist of any combination of letters, digits, plus
    /// ("+"), period ("."), or hyphen ("-")
    SchemeInvalidCharacter(char, usize),
    /// If authority is present, the path must either be empty or begin with a
    /// slash ("/") character.
    PathMalformedAuthorityPresent,
    /// If an authority is not present, the path cannot begin with two slash
    /// characters ("//").
    PathMalformedAuthorityAbsent
}

impl fmt::Display for UriError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> fmt::Result {
        match *self {
            UriError::Common(ref error) =>
                write!(f, "{}", error),
            UriError::SchemeInvalidFirstCharacter(char) =>
                write!(f, "the first character of the scheme is '{}' which is not a letter", char),
            UriError::SchemeInvalidCharacter(char, index) =>
                write!(f, "at index of {} we found '{}' which is not a valid scheme character \
                of letters, digits, plus, period or hyphen", index, char),
            UriError::PathMalformedAuthorityPresent =>
                write!(f, "authority is present, the path must either be empty or begin with \
                a slash '/' character"),
            UriError::PathMalformedAuthorityAbsent =>
                write!(f, "authority is not present, the path cannot begin with two slash \
                characters '//'"),
        }
    }
}

impl error::Error for UriError {
    fn source(
        &self
    ) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            UriError::Common(ref e) => Some(e),
            _ => None
        }
    }
}

impl From<CommonError> for UriError {
    fn from(
        error: CommonError
    ) -> Self {
        UriError::Common(error)
    }
}

/// Is a unique sequence of characters that identifies a resource used by web
/// technologies.
#[derive(Debug)]
pub struct Uri {
    scheme: String,
    authority: Option<String>,
    userinfo: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    path: String,
    query: Option<String>,
    fragment: Option<String>,
}

impl Uri {
    /// Get the `scheme`.
    ///
    /// The scheme defines in what context the rest of the URI should be
    /// interpreted.
    pub fn scheme(
        &self
    ) -> &String {
        &self.scheme
    }

    /// Get the `authority`.
    ///
    /// The authority, if present, identifies the entity that should interpret
    /// the components of the URI for the given scheme.
    pub fn authority(
        &self
    ) -> &Option<String> {
        &self.authority
    }

    /// Get the `userinfo`.
    ///
    /// The userinfo, if present, is a subcomponent of authority.
    pub fn userinfo(
        &self
    ) -> &Option<String> {
        &self.userinfo
    }

    /// Get the `host`.
    ///
    /// The host, if present, is a subcomponent of authority.
    pub fn host(
        &self
    ) -> &Option<String> {
        &self.host
    }

    /// Get the `port`.
    ///
    /// The port, if present, is a subcomponent of authority.
    pub fn port(
        &self
    ) -> &Option<u16> {
        &self.port
    }

    /// Get the `path`.
    ///
    /// The path in combination with the query serves to identify a resource
    /// within the scope of the scheme and authority.
    pub fn path(
        &self
    ) -> &String {
        &self.path
    }

    /// Get the `query`
    ///
    /// The query, if present, in combination with the path serves to identify
    /// a resource within the scope of the scheme and authority.
    pub fn query(
        &self
    ) -> &Option<String> {
        &self.query
    }

    /// Get the `fragment`
    ///
    /// The fragment, if present, allows the indirect identification of a
    /// secondary resource.
    pub fn fragment(
        &self
    ) -> &Option<String> {
        &self.fragment
    }

    /*  https://tools.ietf.org/html/rfc3986#section-3.1

        Scheme names consist of a sequence of characters beginning with a
        letter and followed by any combination of letters, digits, plus
        ("+"), period ("."), or hyphen ("-").  Although schemes are case-
        insensitive, the canonical form is lowercase and documents that
        specify schemes must do so with lowercase letters.  An implementation
        should accept uppercase letters as equivalent to lowercase in scheme
        names (e.g., allow "HTTP" as well as "http") for the sake of
        robustness but should only produce lowercase scheme names for
        consistency.

            scheme      = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
     */
    fn scheme_from(
        uri: &str
    ) -> Result<String> {
        required(uri, "uri")?;
        let mut scheme = String::new();
        let mut chars = uri.char_indices();
        match chars.next() {
            None => {}
            Some((_, c)) => {
                if !c.is_ascii_alphabetic() {
                    return Err(UriError::SchemeInvalidFirstCharacter(c));
                }
                scheme.push(c);
            }
        }
        for (index, char) in chars {
            if ':' == char {
                break;
            } else if !char.is_ascii_alphanumeric()
                && '+' != char
                && '.' != char
                && '-' != char {
                return Err(SchemeInvalidCharacter(char, index));
            }
            scheme.push(char);
        }
        Ok(scheme)
    }

    /*  https://tools.ietf.org/html/rfc3986#section-3.2

        The authority component is preceded by a double slash ("//") and is
        terminated by the next slash ("/"), question mark ("?"), or number
        sign ("#") character, or by the end of the URI.

            authority   = [ userinfo "@" ] host [ ":" port ]
     */
    fn authority_from(
        from: usize,
        uri: &str,
    ) -> Option<String> {
        let mut authority = String::new();
        let mut chars = uri.chars()
            .skip(1 + from);
        for _ in 0..2 {
            let char = match chars.next() {
                None => return None,
                Some(c) => c
            };
            if '/' != char {
                return None;
            }
        }
        for char in chars {
            if '/' == char
                || '?' == char
                || '#' == char {
                break;
            }
            authority.push(char);
        }
        Some(authority)
    }

    /*  https://tools.ietf.org/html/rfc3986#section-3.2.1

        The userinfo subcomponent may consist of a user name and, optionally,
        scheme-specific information about how to gain authorization to access
        the resource. The user information, if present, is followed by a
        commercial at-sign ("@") that delimits it from the host.

            userinfo    = *( unreserved / pct-encoded / sub-delims / ":" )
     */
    fn userinfo_from(
        authority: &Option<String>
    ) -> Option<String> {
        let authority = match authority {
            None => return None,
            Some(a) => a
        };
        let index = match authority.find('@') {
            None => return None,
            Some(i) => i
        };
        if 0 == index {
            return None;
        }
        Some(authority.chars()
            .take(index)
            .collect())
    }

    /*  https://tools.ietf.org/html/rfc3986#section-3.2.2

        The host subcomponent of authority is identified by an IP literal
        encapsulated within square brackets, an IPv4 address in dotted-
        decimal form, or a registered name.

            host        = IP-literal / IPv4address / reg-name
     */
    // TODO: interpret IP-literal / IPv4address / reg-name as per RFC3986
    fn host_from(
        userinfo: &Option<String>,
        authority: &Option<String>,
    ) -> Option<String> {
        let authority = match authority {
            None => return None,
            Some(a) => a
        };
        let userinfo = match userinfo {
            None => 0,
            Some(f) => 1 + f.len()
        };
        let mut chars = authority.chars()
            .skip(userinfo);
        let char = match chars.next() {
            None => return None,
            Some(c) => c
        };
        let mut host = String::new();
        match char {
            '[' => {
                let char = match chars.next() {
                    None => return None,
                    Some(c) => c
                };
                if 'v' == char {
                    for _ in 0..3 {
                        chars.next();
                    }
                } else {
                    host.push(char);
                }
                for char in chars {
                    if ']' == char {
                        return Some(host);
                    }
                    host.push(char);
                }
            }
            _ => {
                if ':' == char {
                    return None;
                }
                host.push(char);
                for char in chars {
                    if ':' == char {
                        break;
                    }
                    host.push(char);
                }
                if !host.is_empty() {
                    return Some(host);
                }
            }
        }
        None
    }

    /*  https://tools.ietf.org/html/rfc3986#section-3.2.3

        The port subcomponent of authority is designated by an optional port
        number in decimal following the host and delimited from it by a
        single colon (":") character.

            port        = *DIGIT
     */
    fn port_from(
        userinfo: &Option<String>,
        host: &Option<String>,
        authority: &Option<String>,
    ) -> Option<u16> {
        let userinfo = match userinfo {
            None => 0,
            Some(u) => 1 + u.len()
        };
        let host = match host {
            None => 0,
            Some(h) => h.len()
        };
        let authority = match authority {
            None => return None,
            Some(a) => a
        };
        let mut chars = authority.chars().skip(userinfo + host);
        match chars.next() {
            None => return None,
            Some(c) => {
                if ':' != c {
                    return None;
                }
            }
        }
        let mut port = String::new();
        for char in chars {
            if char.is_ascii_digit() {
                port.push(char);
            } else {
                return None;
            }
            if port.len() > 5 {
                return None;
            }
        }
        if port.is_empty() {
            return None;
        }
        let port = match port.chars()
            .collect::<String>()
            .parse::<u16>() {
            Ok(p) => p,
            Err(_) => return None
        };
        Some(port)
    }

    /*  https://tools.ietf.org/html/rfc3986#section-3.3

        The path component contains data, usually organized in hierarchical
        form, that, along with data in the non-hierarchical query component
        (Section 3.4), serves to identify a resource within the scope of the
        URI's scheme and naming authority (if any).  The path is terminated
        by the first question mark ("?") or number sign ("#") character, or
        by the end of the URI.
     */
    fn path_from(
        from: usize,
        authority: &Option<String>,
        uri: &str,
    ) -> Result<String> {
        required(uri, "uri")?;
        let authority = match authority {
            None => 0,
            Some(a) => 2 + a.len()
        };
        let skip = 1
            + from
            + authority;
        let chars = uri.chars()
            .skip(skip);
        let mut path = String::new();
        for char in chars {
            if '?' == char
                || '#' == char {
                break;
            }
            path.push(char);
        }
        if authority > 0
            && !path.is_empty()
            && !path.starts_with('/') {
            return Err(UriError::PathMalformedAuthorityPresent);
        } else if authority == 0
            && !path.is_empty()
            && path.starts_with("//") {
            return Err(UriError::PathMalformedAuthorityAbsent);
        }
        Ok(path)
    }

    /*  https://tools.ietf.org/html/rfc3986#section-3.4

        The query component contains non-hierarchical data that, along with
        data in the path component, serves to identify a resource within the
        scope of the URI's scheme and naming authority (if any).  The query
        component is indicated by the first question mark ("?") character
        and terminated by a number sign ("#") character or by the end of the
        URI.
     */
    fn query_from(
        from: usize,
        authority: &Option<String>,
        path: &str,
        uri: &str,
    ) -> Option<String> {
        let authority = match authority {
            None => 0,
            Some(a) => 2 + a.len()
        };
        let skip = 1
            + from
            + path.len()
            + authority;
        let mut chars = uri.chars()
            .skip(skip);
        match chars.next() {
            None => return None,
            Some(c) => {
                if '?' != c {
                    return None;
                }
            }
        }
        let mut query = String::new();
        for char in chars {
            if '#' == char {
                break;
            }
            query.push(char);
        }
        Some(query)
    }

    /* https://tools.ietf.org/html/rfc3986#section-3.5

        The fragment identifier component of a URI allows indirect
        identification of a secondary resource by reference to a primary
        resource and additional identifying information.  The identified
        secondary resource may be some portion or subset of the primary
        resource, some view on representations of the primary resource, or
        some other resource defined or described by those representations.  A
        fragment identifier component is indicated by the presence of a
        number sign ("#") character and terminated by the end of the URI.
     */
    fn fragment_from(
        from: usize,
        authority: &Option<String>,
        path: &str,
        query: &Option<String>,
        uri: &str,
    ) -> Option<String> {
        let authority = match authority {
            None => 0,
            Some(a) => 2 + a.len()
        };
        let query = match query {
            None => 0,
            Some(q) => 1 + q.len()
        };
        let skip = 1
            + from
            + path.len()
            + authority
            + query;
        let mut chars = uri.chars()
            .skip(skip);
        match chars.next() {
            None => return None,
            Some(c) => {
                if '#' != c {
                    return None;
                }
            }
        }
        Some(chars.collect::<String>())
    }
}

impl TryFrom<&str> for Uri {
    type Error = UriError;

    fn try_from(
        uri: &str
    ) -> Result<Self> {
        required(uri, "uri")?;
        let scheme = Uri::scheme_from(uri)?;
        let from = scheme.len();
        let authority = Uri::authority_from(from, uri);
        let userinfo = Uri::userinfo_from(&authority);
        let host = Uri::host_from(&userinfo, &authority);
        let port = Uri::port_from(&userinfo, &host, &authority);
        let path = Uri::path_from(from, &authority, uri)?;
        let query = Uri::query_from(from, &authority, &path, uri);
        let fragment = Uri::fragment_from(from, &authority, &path, &query, uri);
        Ok(Uri {
            scheme,
            authority,
            userinfo,
            host,
            port,
            path,
            query,
            fragment,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use crate::uri::{Uri, UriError};

    static ERROR_UNEXPECTED_ERROR: &str = "An unexpected error was returned";

    #[test]
    fn uri_error_on_scheme_from_invalid_first_character() {
        let error = Uri::scheme_from("1").unwrap_err();
        match error {
            UriError::SchemeInvalidFirstCharacter(_) => {
                assert_eq!("the first character of the scheme is '1' which is not a letter",
                           format!("{}", error))
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn uri_error_on_scheme_from_invalid_character() {
        let error = Uri::scheme_from("file!:").unwrap_err();
        match error {
            UriError::SchemeInvalidCharacter(..) => {
                assert_eq!("at index of 4 we found '!' which is not a valid scheme character \
                of letters, digits, plus, period or hyphen", format!("{}", error))
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn uri_success_scheme_from() {
        assert_eq!("f1+-.", Uri::scheme_from("f1+-.:").unwrap());
    }

    #[test]
    fn uri_skip_empty_authority_from() {
        assert_eq!(None, Uri::authority_from(0, ""));
    }

    #[test]
    fn uri_skip_missing_authority_from() {
        assert_eq!(None, Uri::authority_from(0, ":/!"));
    }

    #[test]
    fn uri_skip_blank_authority_from() {
        assert_eq!(Some(String::new()),
                   Uri::authority_from(0, ":///"));
    }

    #[test]
    fn uri_success_authority_from() {
        assert_eq!(Some(String::from("slash")),
                   Uri::authority_from(0, "://slash/"));
        assert_eq!(Some(String::from("question mark")),
                   Uri::authority_from(0, "://question mark?"));
        assert_eq!(Some(String::from("number sign")),
                   Uri::authority_from(0, "://number sign#"));
        assert_eq!(Some(String::from("end")),
                   Uri::authority_from(0, "://end"));
        assert_eq!(Some(String::from("slash")),
                   Uri::authority_from("file".len(), "file://slash/"));
    }

    #[test]
    fn uri_skip_empty_userinfo_from() {
        assert_eq!(None, Uri::userinfo_from(&None));
        assert_eq!(None, Uri::userinfo_from(&Some(String::from(""))));
        assert_eq!(None, Uri::userinfo_from(&Some(String::from("@"))));
    }

    #[test]
    fn uri_success_userinfo_from() {
        assert_eq!(Some(String::from("user")),
                   Uri::userinfo_from(&Some(String::from("user@"))));
    }

    #[test]
    fn uri_skip_empty_host_from() {
        assert_eq!(None, Uri::host_from(&None, &None));
        assert_eq!(None, Uri::host_from(&None, &Some(String::from(""))));
        assert_eq!(None, Uri::host_from(&Some(String::from("user")),
                                        &Some(String::from("user@"))));
    }

    #[test]
    fn uri_success_host_from() {
        assert_eq!(Some(String::from("localhost")),
                   Uri::host_from(&None, &Some(String::from("localhost"))));
        assert_eq!(Some(String::from("127.0.0.1")),
                   Uri::host_from(&None, &Some(String::from("127.0.0.1:80"))));
        assert_eq!(Some(String::from("2001:db8::7")),
                   Uri::host_from(&Some(String::from("user")),
                                  &Some(String::from("user@[2001:db8::7]:80"))));
    }

    #[test]
    fn uri_skip_empty_port_from() {
        assert_eq!(None, Uri::port_from(&None, &None, &None));
        assert_eq!(None, Uri::port_from(&None, &None, &Some(String::new())));
    }

    #[test]
    fn uri_success_port_from() {
        assert_eq!(Some(80), Uri::port_from(&None,
                                            &Some(String::from("127.0.0.1")),
                                            &Some(String::from("127.0.0.1:80"))));
        assert_eq!(Some(443),
                   Uri::port_from(&Some(String::from("user:password")),
                                  &Some(String::from("[2001:db8::7]")),
                                  &Some(String::from("user:password@[2001:db8::7]:443"))));
    }

    #[test]
    fn uri_error_on_path_from() {
        let error = Uri::path_from(String::from("http").len(),
                                   &Some(String::from("localhost")),
                                   "http://localhost!v1/user?test=true").unwrap_err();
        match error {
            UriError::PathMalformedAuthorityPresent => {
                assert_eq!("authority is present, the path must either be empty or \
                begin with a slash '/' character", format!("{}", error))
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }

        let error = Uri::path_from(String::from("file").len(),
                                   &None,
                                   "file://").unwrap_err();
        match error {
            UriError::PathMalformedAuthorityAbsent => {
                assert_eq!("authority is not present, the path cannot begin with two \
                slash characters '//'", format!("{}", error))
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn uri_success_path_from() {
        assert_eq!(String::new(),
                   Uri::path_from(String::from("file").len(),
                                  &Some(String::new()),
                                  "file://").unwrap());
        assert_eq!(String::from("/etc/fstab"),
                   Uri::path_from(String::from("file").len(),
                                  &Some(String::new()),
                                  "file:///etc/fstab").unwrap());
        assert_eq!(String::from("/v1/user"),
                   Uri::path_from(String::from("http").len(),
                                  &Some(String::from("localhost")),
                                  "http://localhost/v1/user?test=true").unwrap());
    }

    #[test]
    fn uri_success_query_from() {
        assert_eq!(None, Uri::query_from("http".len(),
                                         &Some(String::from("localhost")),
                                         "/",
                                         "http://localhost/"));
        assert_eq!(Some(String::from("")),
                   Uri::query_from("http".len(),
                                   &Some(String::from("localhost")),
                                   "/",
                                   "http://localhost/?#"));
        assert_eq!(Some(String::from("key=valu?e")),
                   Uri::query_from("http".len(),
                                   &Some(String::from("localhost")),
                                   "/",
                                   "http://localhost/?key=valu?e"));
    }

    #[test]
    fn uri_success_fragment_from() {
        assert_eq!(None, Uri::fragment_from("http".len(),
                                            &Some(String::from("localhost")),
                                            "/",
                                            &None,
                                            "http://localhost/"));
        assert_eq!(Some(String::from("")),
                   Uri::fragment_from("http".len(),
                                   &Some(String::from("localhost")),
                                   "/",
                                   &None,
                                   "http://localhost/#"));
        assert_eq!(Some(String::from("other")),
                   Uri::fragment_from("http".len(),
                                      &Some(String::from("localhost")),
                                      "/",
                                      &Some(String::from("key=value")),
                                      "http://localhost/?key=value#other"));
    }

    #[test]
    fn uri_error_on_try_from_with_empty_string() {
        let error = Uri::try_from("").unwrap_err();
        match error {
            UriError::Common(_) => {
                assert_eq!("'uri' is empty", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn uri_error_on_try_from_with_blank_string() {
        let error = Uri::try_from(" \t").unwrap_err();
        match error {
            UriError::Common(_) => {
                assert_eq!("'uri' is blank", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn uri_success_try_from() {
        let uri = Uri::try_from("file:///usr/lib").unwrap();
        assert_eq!("file", uri.scheme());
        assert_eq!(&Some(String::new()), uri.authority());
        assert_eq!(&None, uri.userinfo());
        assert_eq!(&None, uri.host());
        assert_eq!(&None, uri.port());
        assert_eq!("/usr/lib", uri.path());
        assert_eq!(&None, uri.query());
        assert_eq!(&None, uri.fragment());

        let uri = Uri::try_from("mailto:John.Doe@example.com").unwrap();
        assert_eq!("mailto", uri.scheme());
        assert_eq!(&None, uri.authority());
        assert_eq!(&None, uri.userinfo());
        assert_eq!(&None, uri.host());
        assert_eq!(&None, uri.port());
        assert_eq!("John.Doe@example.com", uri.path());
        assert_eq!(&None, uri.query());
        assert_eq!(&None, uri.fragment());
    }
}