use std::collections::HashMap;

#[derive(Debug)]
pub enum CommonError {
    /// An empty variable was given.
    IsEmpty(String),
    /// A blank variable was given.
    IsBlank(String),
}

fn error_message_is_empty(
    str: &str
) -> String {
    format!("{} is empty", str)
}

fn error_message_is_blank(
    str: &str
) -> String {
    format!("{} is blank", str)
}

/// Precondition which is used by [required](required) to perform checks on the
/// passed in types.
pub trait Precondition {
    fn if_empty(
        &self
    ) -> bool;

    fn if_blank(
        &self
    ) -> bool {
        false
    }
}

/// Perform [precondition](Precondition) checks.
///
/// * `t` is a supported type.
/// * `name` is the name of the variable.
///
/// # Errors
/// * [CommonError:isEmpty](CommonError#isEmpty) if `t` is empty.
/// * [CommonError:isBlank](CommonError#isBlank) if `t` is blank.
///
/// # Examples
/// ```
/// use foundation::common::required;
/// let s = "string value";
/// let s = required(s, "s");
///
/// // The following will emit an error since we are passing an empty Vec.
/// let v:Vec<i32> = Vec::new();
/// let v = required(&v, "v");
/// ```
///
pub fn required<T: Precondition>(
    t: T,
    name: &'static str,
) -> Result<T, CommonError> {
    if t.if_empty() {
        return Err(CommonError::IsEmpty(error_message_is_empty(name)));
    }
    if t.if_blank() {
        return Err(CommonError::IsBlank(error_message_is_blank(name)));
    }
    Ok(t)
}

impl Precondition for String {
    fn if_empty(
        &self
    ) -> bool {
        self.is_empty()
    }

    fn if_blank(
        &self
    ) -> bool {
        self.trim().is_empty()
    }
}

impl Precondition for &String {
    fn if_empty(
        &self
    ) -> bool {
        self.is_empty()
    }

    fn if_blank(
        &self
    ) -> bool {
        self.trim().is_empty()
    }
}

impl Precondition for &str {
    fn if_empty(
        &self
    ) -> bool {
        self.is_empty()
    }

    fn if_blank(
        &self
    ) -> bool {
        self.trim().is_empty()
    }
}

impl<T> Precondition for &Vec<T> {
    fn if_empty(
        &self
    ) -> bool {
        self.is_empty()
    }
}

impl<K, V> Precondition for &HashMap<K, V> {
    fn if_empty(
        &self
    ) -> bool {
        self.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use crate::common::{required, CommonError, error_message_is_empty, error_message_is_blank};
    use std::collections::HashMap;

    static ERROR_UNEXPECTED_ERROR: &str = "An unexpected error was returned";

    #[test]
    fn required_error_on_empty_str_reference() {
        let error = required("", "str").unwrap_err();
        match error {
            CommonError::IsEmpty(message) => {
                assert_eq!(message, error_message_is_empty("str"));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn required_error_on_blank_str_reference() {
        let error = required(" ", "str").unwrap_err();
        match error {
            CommonError::IsBlank(message) => {
                assert_eq!(message, error_message_is_blank("str"));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn required_succeed_on_str_reference() {
        assert_eq!(required("test", "str").unwrap(), "test");
    }

    #[test]
    fn required_error_on_empty_string() {
        let error = required(String::from(""), "string")
            .unwrap_err();
        match error {
            CommonError::IsEmpty(message) => {
                assert_eq!(message, error_message_is_empty("string"));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn required_error_on_blank_string() {
        let error = required(String::from("\r\n"), "string")
            .unwrap_err();
        match error {
            CommonError::IsBlank(message) => {
                assert_eq!(message, error_message_is_blank("string"));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn required_succeed_on_string() {
        assert_eq!(required(String::from("test"), "str").unwrap(),
                   String::from("test"));
    }

    #[test]
    fn required_error_on_empty_string_reference() {
        let s = String::from("");
        let error = required(&s, "s").unwrap_err();
        match error {
            CommonError::IsEmpty(message) => {
                assert_eq!(message, error_message_is_empty("s"));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn required_error_on_blank_string_reference() {
        let s = String::from("\r\n");
        let error = required(&s, "s").unwrap_err();
        match error {
            CommonError::IsBlank(message) => {
                assert_eq!(message, error_message_is_blank("s"));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn required_succeed_on_string_reference() {
        let s = String::from("test");
        assert_eq!(required(&s, "s").unwrap(), &String::from("test"));
    }

    #[test]
    fn required_error_on_empty_vec() {
        let v: Vec<isize> = Vec::new();
        let error = required(&v, "v").unwrap_err();
        match error {
            CommonError::IsEmpty(message) => {
                assert_eq!(message, error_message_is_empty("v"));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn required_succeed_on_vec() {
        let v = vec![1, 2];
        assert_eq!(required(&v, "v").unwrap(), &v);
    }

    #[test]
    fn required_error_on_empty_hashmap() {
        let m: HashMap<String, String> = HashMap::new();
        let error = required(&m, "m").unwrap_err();
        match error {
            CommonError::IsEmpty(message) => {
                assert_eq!(message, error_message_is_empty("m"));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn required_succeed_on_hashmap() {
        let m: HashMap<&str, usize> = [
            ("ten", 10),
            ("twenty", 20)
        ].iter().copied().collect();
        assert_eq!(required(&m, "m").unwrap(), &m);
    }
}
