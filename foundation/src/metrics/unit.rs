static BYTES: &str = "bytes";
static NANOSECONDS: &str = "nanoseconds";
static PERCENTAGE: &str = "percentage";
static QUANTITY: &str = "quantity";

/// Return the unit for `bytes`.
pub fn bytes() -> &'static str {
    BYTES
}

/// Return the unit for `nanoseconds`.
pub fn nanoseconds() -> &'static str {
    NANOSECONDS
}

/// Return the unit for `percentage`.
pub fn percentage() -> &'static str {
    PERCENTAGE
}

/// Return the unit for `quantity`.
pub fn quantity() -> &'static str {
    QUANTITY
}

#[cfg(test)]
mod tests {
    use crate::metrics::unit;
    use crate::metrics::unit::*;

    #[test]
    fn bytes() {
        assert_eq!(unit::bytes(), BYTES);
    }

    #[test]
    fn nanoseconds() {
        assert_eq!(unit::nanoseconds(), NANOSECONDS);
    }

    #[test]
    fn percentage() {
        assert_eq!(unit::percentage(), PERCENTAGE);
    }

    #[test]
    fn quantity() {
        assert_eq!(unit::quantity(), QUANTITY);
    }
}