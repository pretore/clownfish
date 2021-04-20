use std::collections::HashMap;

#[derive(Debug)]
pub enum CommonError {
    /// An empty variable was given.
    IsEmpty(String),
    /// Is invalid may mean that a string contains whitespace only characters.
    IsInvalid(String),
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

/// Domain which is used by [required](required) to perform checks on the
/// passed in types.
pub trait Domain {
    fn check_domain(
        &self,
        name: &'static str,
    ) -> Result<(), CommonError>;
}

/// Perform [domain](Domain) checks.
///
/// * `t` is a supported type.
/// * `name` is the name of the variable.
///
/// # Errors
/// * [CommonError::IsEmpty](CommonError::IsEmpty) if `t` is empty.
/// * [CommonError::IsInvalid](CommonError::IsInvalid) if `t` is invalid.
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
pub fn required<T: Domain>(
    t: T,
    name: &'static str,
) -> Result<(), CommonError> {
    t.check_domain(name)
}

impl Domain for &str {
    fn check_domain(
        &self,
        name: &'static str
    ) -> Result<(), CommonError> {
        if self.is_empty() {
            return Err(CommonError::IsEmpty(error_message_is_empty(name)));
        }
        if self.trim().is_empty() {
            return Err(CommonError::IsInvalid(error_message_is_blank(name)));
        }
        Ok(())
    }
}

impl Domain for &String {
    fn check_domain(
        &self,
        name: &'static str
    ) -> Result<(), CommonError> {
        required(self.as_str(), name)
    }
}

impl<T> Domain for &[T] {
    fn check_domain(
        &self,
        name: &'static str
    ) -> Result<(), CommonError> {
        if self.is_empty() {
            return Err(CommonError::IsEmpty(error_message_is_empty(name)));
        }
        Ok(())
    }
}

impl<T> Domain for &Vec<T> {
    fn check_domain(
        &self,
        name: &'static str
    ) -> Result<(), CommonError> {
        required(&self[..], name)
    }
}

impl<K, V> Domain for &HashMap<K, V> {
    fn check_domain(
        &self,
        name: &'static str
    ) -> Result<(), CommonError> {
        if self.is_empty() {
            return Err(CommonError::IsEmpty(error_message_is_empty(name)));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::common::{required, CommonError, error_message_is_empty, error_message_is_blank};
    use std::collections::HashMap;

    static ERROR_UNEXPECTED_ERROR: &str = "An unexpected error was returned";

    #[test]
    fn required_error_on_empty_str() {
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
    fn required_error_on_blank_str() {
        let error = required(" ", "str").unwrap_err();
        match error {
            CommonError::IsInvalid(message) => {
                assert_eq!(message, error_message_is_blank("str"));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn required_succeed_on_str() {
        required("test", "str").unwrap();
    }

    #[test]
    fn required_error_on_empty_string() {
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
    fn required_error_on_blank_string() {
        let s = String::from("\r\n");
        let error = required(&s, "s").unwrap_err();
        match error {
            CommonError::IsInvalid(message) => {
                assert_eq!(message, error_message_is_blank("s"));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn required_succeed_on_string() {
        let s = String::from("test");
        required(&s, "s").unwrap();
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
        required(&v, "v").unwrap();
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
        required(&m, "m").unwrap();
    }
}
