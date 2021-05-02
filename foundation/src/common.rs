use std::collections::HashMap;
use std::{error, fmt};
use std::fmt::Formatter;

type Result<T> = std::result::Result<T, CommonError>;

#[derive(Debug)]
pub enum CommonError {
    /// Variable whose contents was empty.
    Empty(&'static str),
    /// Variable that only consisted of whitespace characters.
    Blank(&'static str),
    /// Variable is not suitable for use.
    Invalid(&'static str),
}

impl fmt::Display for CommonError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> fmt::Result {
        match *self {
            CommonError::Empty(name) =>
                write!(f, "'{}' is empty", name),
            CommonError::Blank(name) =>
                write!(f, "'{}' is blank", name),
            CommonError::Invalid(name) =>
                write!(f, "'{}' is invalid", name)
        }
    }
}

impl error::Error for CommonError {

}

/// Domain which is used by [required](required) to perform checks on the
/// passed in types.
pub trait Domain {
    fn check_domain(
        &self,
        name: &'static str,
    ) -> Result<()>;
}

/// Perform [domain](Domain) checks.
///
/// * `t` is a supported type.
/// * `name` is the name of the variable.
///
/// # Errors
/// * [CommonError::Empty](CommonError::Empty) if `t` is empty.
/// * [CommonError::Blank](CommonError::Blank) if `t` only consists of
/// whitespace characters.
/// * [CommonError::Invalid](CommonError::Invalid) if `t` is invalid.
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
) -> Result<()> {
    t.check_domain(name)
}

impl Domain for &str {
    fn check_domain(
        &self,
        name: &'static str
    ) -> Result<()> {
        if self.is_empty() {
            return Err(CommonError::Empty(name));
        }
        if self.trim().is_empty() {
            return Err(CommonError::Blank(name));
        }
        Ok(())
    }
}

impl Domain for &String {
    fn check_domain(
        &self,
        name: &'static str
    ) -> Result<()> {
        required(self.as_str(), name)
    }
}

impl<T> Domain for &[T] {
    fn check_domain(
        &self,
        name: &'static str
    ) -> Result<()> {
        if self.is_empty() {
            return Err(CommonError::Empty(name));
        }
        Ok(())
    }
}

impl<T> Domain for &Vec<T> {
    fn check_domain(
        &self,
        name: &'static str
    ) -> Result<()> {
        required(&self[..], name)
    }
}

impl<K, V> Domain for &HashMap<K, V> {
    fn check_domain(
        &self,
        name: &'static str
    ) -> Result<()> {
        if self.is_empty() {
            return Err(CommonError::Empty(name));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::common::{required, CommonError};
    use std::collections::HashMap;

    static ERROR_UNEXPECTED_ERROR: &str = "An unexpected error was returned";

    #[test]
    fn required_error_on_empty_str() {
        let error = required("", "str").unwrap_err();
        match error {
            CommonError::Empty(_) => {
                assert_eq!("'str' is empty", format!("{}", error));
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
            CommonError::Blank(_) => {
                assert_eq!("'str' is blank", format!("{}", error));
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
            CommonError::Empty(_) => {
                assert_eq!("'s' is empty", format!("{}", error));
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
            CommonError::Blank(_) => {
                assert_eq!("'s' is blank", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn required_succeed_on_string() {
        required(&String::from("test"), "s").unwrap();
    }

    #[test]
    fn required_error_on_empty_vec() {
        let v: Vec<isize> = Vec::new();
        let error = required(&v, "v").unwrap_err();
        match error {
            CommonError::Empty(_) => {
                assert_eq!("'v' is empty", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn required_succeed_on_vec() {
        required(&vec![1, 2], "v").unwrap();
    }

    #[test]
    fn required_error_on_empty_hashmap() {
        let m: HashMap<String, String> = HashMap::new();
        let error = required(&m, "m").unwrap_err();
        match error {
            CommonError::Empty(_) => {
                assert_eq!("'m' is empty", format!("{}", error));
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
