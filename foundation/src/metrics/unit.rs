pub const BYTES: &str = "bytes";
pub const NANOSECONDS: &str = "nanoseconds";
pub const PERCENTAGE: &str = "percentage";
pub const QUANTITY: &str = "quantity";

#[cfg(test)]
mod tests {
    use crate::metrics::unit;

    #[test]
    fn bytes() {
        assert_eq!(unit::BYTES, "bytes");
    }

    #[test]
    fn nanoseconds() {
        assert_eq!(unit::NANOSECONDS, "nanoseconds");
    }

    #[test]
    fn percentage() {
        assert_eq!(unit::PERCENTAGE, "percentage");
    }

    #[test]
    fn quantity() {
        assert_eq!(unit::QUANTITY, "quantity");
    }
}