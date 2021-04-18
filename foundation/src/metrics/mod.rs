use std::collections::{BTreeMap, HashMap};
use std::fmt::{Display, Formatter};
use std::fmt;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::time::{SystemTime, Duration};

use crate::common::{CommonError, required};
use crate::foundation::executor;

/// Unit represents the type of a value in [measurements](Measurement)
/// and [metrics](Metric).
pub mod unit;

#[derive(Debug)]
enum MetricsError {
    /// When you are trying to aggregate [measurements](Measurement) or
    /// [distributions](Distribution) and their `label`s don't match.
    MismatchedLabel(String),
    /// When you are trying to aggregate [measurements](Measurement) or
    /// [distributions](Distribution) and their `unit`s don't match.
    MismatchedUnit(String),
    /// When you are trying to aggregate [measurements](Measurement) or
    /// [distributions](Distribution) and their `tags` don't match.
    MismatchedTags(String),
    /// When an integer overflow error has occurred.
    Overflow(String),
}

static MEASUREMENT: &str = "Measurement";
static DISTRIBUTION: &str = "Distribution";

fn error_message_mismatched_label(
    lhs: (&str /* label */, &str /* type */),
    rhs: (&str /* label */, &str /* type */),
) -> String {
    format!("LHS: {} with label of \"{}\" != RHS: {} with label of \"{}\"",
            lhs.1, lhs.0, rhs.1, rhs.0)
}

fn error_message_mismatched_unit(
    lhs: (&str /* unit */, &str /* type */),
    rhs: (&str /* unit */, &str /* type */),
) -> String {
    format!("LHS: {} with unit of \"{}\" != RHS: {} with unit of \"{}\"",
            lhs.1, lhs.0, rhs.1, rhs.0)
}

fn error_message_mismatched_tags(
    lhs: (&Tags, &str /* type */),
    rhs: (&Tags, &str /* type */),
) -> String {
    format!("LHS: {} with tags of \"{}\" != RHS: {} with tags of \"{}\"",
            lhs.1, lhs.0, rhs.1, rhs.0)
}

fn error_message_overflow(
    op: &str,
    key: &i128,
    label: &str,
    unit: &str,
    tags: &Tags,
) -> String {
    format!("Overflow {} key of \"{}\" for {} with label: {}, \
             unit: {} and tags: {}", op, key, DISTRIBUTION, label, unit, tags)
}

/// Tags are string key value pairs used to classify and quantify
/// [measurements](Measurement) and [metrics](Metric).
#[derive(Debug, Default)]
pub struct Tags {
    entries: BTreeMap<String, String>
}

impl Tags {
    /// Create an empty `Tags`.
    pub fn new() -> Self {
        Default::default()
    }

    /// Create a `Tags` using the given map.
    ///
    /// * tags whose contents will be used to populate the instance.
    ///
    /// # Errors
    ///
    /// * [CommonError::isEmpty](CommonError::IsEmpty) if any
    /// _key_ in `tags` is empty.
    /// * [CommonError::isBlank](CommonError::IsBlank) if any
    /// _key_ in `tags` is blank.
    ///
    /// # Examples
    /// ```
    /// use foundation::metrics::Tags;
    ///
    /// let tags = Tags::from([
    ///     ("service", "awesome"),
    ///     ("hostname", "localhost")
    /// ].iter().map(|e| {
    ///   (String::from(e.0), String::from(e.1))
    /// }).collect()).unwrap();
    /// ```
    pub fn from(
        tags: HashMap<String, String>
    ) -> Result<Self, CommonError> {
        required(&tags, "tags")?;
        let mut map = BTreeMap::new();
        for (key, value) in &tags {
            required(key, "key in tags")?;
            map.insert(key.clone(), value.clone());
        }
        Ok(Tags {
            entries: map
        })
    }

    /// Merge two `Tags` together.
    ///
    /// * `other` whose contents we will merge into this instance
    /// _but will not overwrite any existing entries_.
    ///
    /// # Examples
    /// ```
    /// use foundation::metrics::Tags;
    ///
    /// let mut tags = Tags::from([
    ///     ("service", "awesome"),
    ///     ("hostname", "localhost")
    /// ].iter().map(|e| {
    ///   (String::from(e.0), String::from(e.1))
    /// }).collect()).unwrap();
    ///
    /// let other = Tags::from([
    ///     ("service", "other"),
    ///     ("component", "application")
    /// ].iter().map(|e| {
    ///   (String::from(e.0), String::from(e.1))
    /// }).collect()).unwrap();
    /// // 'component' will be added and 'service' will not be overwritten.
    /// tags.merge(&other);
    /// ```
    pub fn merge(
        &mut self,
        other: &Self,
    ) {
        for (key, value) in &other.entries {
            if self.entries.contains_key(key) {
                continue;
            }
            self.entries.insert(key.clone(), value.clone());
        }
    }
}

impl Display for Tags {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> fmt::Result {
        f.debug_map()
            .entries(&self.entries)
            .finish()
    }
}

impl PartialEq for Tags {
    fn eq(
        &self,
        other: &Self,
    ) -> bool {
        self.entries.eq(&other.entries)
    }
}

impl Clone for Tags {
    fn clone(&self) -> Self {
        Tags {
            entries: self.entries.clone()
        }
    }
}

/// A measurement is what was observed and recorded.
#[derive(Debug)]
pub struct Measurement {
    label: &'static str,
    value: i128,
    unit: &'static str,
    tags: Tags,
    at: SystemTime,
}

impl Measurement {
    /// Create a `Measurement` with the given values.
    ///
    /// * `label` tells us _what_ we are measuring.
    /// * `value` is the _quantity_ of the measurement.
    /// * `unit` gives us the _type_ of quantity.
    /// * `tags` allows for classification and grouping of related
    /// measurements.
    ///
    /// # Errors
    /// * [CommonError::isEmpty](CommonError::IsEmpty) if
    /// `label` or `unit` is empty.
    /// * [CommonError::isBlank](CommonError::IsBlank) if
    /// `label` or `unit` is blank.
    ///
    /// # Examples
    /// ```
    /// use foundation::metrics::{Measurement, Tags};
    /// use foundation::metrics::unit;
    /// use std::collections::HashMap;
    ///
    /// let measurement = Measurement::new("cpu_usage", 12, unit::percentage(), None);
    /// ```
    pub fn new(
        label: &'static str,
        value: i128,
        unit: &'static str,
        tags: Option<&Tags>,
    ) -> Result<Self, CommonError> {
        required(label, "label")?;
        required(unit, "unit")?;
        Ok(Measurement {
            label,
            value,
            unit,
            tags: match tags {
                None => Tags::new(),
                Some(t) => t.clone()
            },
            at: SystemTime::now(),
        })
    }

    /// Get the `label` of this `Measurement`.
    pub fn label(
        &self
    ) -> &'static str {
        self.label
    }

    /// Get the `value` of this `Measurement`.
    pub fn value(
        &self
    ) -> i128 {
        self.value
    }

    /// Get the `unit` of this `Measurement`.
    pub fn unit(
        &self
    ) -> &'static str {
        self.unit
    }

    /// Get the `tags` of this `Measurement`.
    pub fn tags(
        &self
    ) -> &Tags {
        &self.tags
    }

    /// Get the [time](std::time::SystemTime) this `Measurement` was recorded
    /// `at`.
    pub fn at(
        &self
    ) -> &SystemTime {
        &self.at
    }
}

/// A distribution is the what you have when you group together related
/// [measurements](Measurement).
#[derive(Debug)]
struct Distribution {
    label: &'static str,
    unit: &'static str,
    tags: Tags,
    values: HashMap<i128, u128>,
    first: SystemTime,
    last: SystemTime,
}

impl Distribution {
    /// Create an empty `Distribution` with the given values.
    ///
    /// * `label` tells us _what_ we are distribution of.
    /// * `unit` gives us the _type_ of distribution values.
    /// * `tags` allows for classification and grouping of related
    /// distributions.
    ///
    /// # Errors
    /// * [CommonError::isEmpty](CommonError::IsEmpty) if
    /// `label` or `unit` is empty.
    /// * [CommonError::isBlank](CommonError::IsBlank) if
    /// `label` or `unit` is blank.
    ///
    pub fn new(
        label: &'static str,
        unit: &'static str,
        tags: &Tags,
    ) -> Result<Self, CommonError> {
        required(label, "label")?;
        required(unit, "unit")?;
        let now = SystemTime::now();
        Ok(Distribution {
            label,
            unit,
            tags: tags.clone(),
            values: HashMap::new(),
            first: now.clone(),
            last: now,
        })
    }

    /// Create an empty `Distribution` from the given measurement.
    ///
    /// * `measurement` which will be used to create the distribution.
    ///
    pub fn from(
        measurement: &Measurement
    ) -> Self {
        let now = SystemTime::now();
        Distribution {
            label: measurement.label,
            unit: measurement.unit,
            tags: measurement.tags.clone(),
            values: HashMap::new(),
            first: now.clone(),
            last: now,
        }
    }

    /// Add the given measurement to the `Distribution`.
    ///
    /// * `measurement` which will be added to the distribution.
    ///
    /// # Errors
    /// * [MetricsError::MismatchedLabel](MetricsError::MismatchedLabel) if
    /// `label` of the measurement does not match the `label` of the
    /// distribution.
    /// * [MetricsError::MismatchedUnit](MetricsError::MismatchedUnit) if
    /// `unit` of the measurement does not match the `unit` of the distribution.
    /// * [MetricsError::MismatchedTags](MetricsError::MismatchedTags) if
    /// `tags` of the measurement does not match the `tags` of the distribution.
    /// * [MetricsError::Overflow](MetricsError::Overflow) if the measurement
    /// value caused an integer overflow in the distribution.
    ///
    pub fn add(
        &mut self,
        measurement: &Measurement,
    ) -> Result<(), MetricsError> {
        Distribution::if_related(
            (self.label, self.unit, &self.tags, DISTRIBUTION),
            (measurement.label, measurement.unit, &measurement.tags, MEASUREMENT),
        )?;
        let count = self.values.entry(measurement.value)
            .or_insert(0);
        match count.checked_add(1) {
            None => {
                return Err(MetricsError::Overflow(
                    error_message_overflow(
                        "incrementing",
                        &measurement.value,
                        self.label,
                        self.unit,
                        &self.tags,
                    )));
            }
            Some(sum) => {
                *count = sum;
            }
        }
        self.update_duration(&measurement.at, &measurement.at);
        Ok(())
    }

    /// Merge two `Distribution`s together.
    ///
    /// * `other` the given distribution whose contents will be merged.
    ///
    /// # Errors
    /// * [MetricsError::MismatchedLabel](MetricsError::MismatchedLabel) if
    /// `label` of the given distribution does not match the `label` of this
    /// distribution.
    /// * [MetricsError::MismatchedUnit](MetricsError::MismatchedUnit) if
    /// `unit` of the given distribution does not match the `unit` of this
    /// distribution.
    /// * [MetricsError::MismatchedTags](MetricsError::MismatchedTags) if
    /// `tags` of the given distribution does not match the `tags` of this
    /// distribution.
    /// * [MetricsError::Overflow](MetricsError::Overflow) if the given
    /// distribution has a value that caused an integer overflow in this
    /// distribution.
    pub fn merge(
        &mut self,
        other: &Self,
    ) -> Result<(), MetricsError> {
        Distribution::if_related(
            (self.label, self.unit, &self.tags, DISTRIBUTION),
            (other.label, other.unit, &other.tags, DISTRIBUTION),
        )?;
        let mut values = self.values.clone();
        for (key, value) in &other.values {
            let count = values.entry(*key).or_insert(0);
            match count.checked_add(*value) {
                None => {
                    return Err(MetricsError::Overflow(
                        error_message_overflow(
                            "merging",
                            key,
                            self.label,
                            self.unit,
                            &self.tags,
                        )));
                }
                Some(sum) => {
                    *count = sum;
                }
            }
        }
        self.values = values;
        self.update_duration(&other.first, &other.last);
        Ok(())
    }

    fn if_related(
        lhs: (&str /*label*/, &str /*unit*/, &Tags, &str /*type*/),
        rhs: (&str /*label*/, &str /*unit*/, &Tags, &str /*type*/),
    ) -> Result<(), MetricsError> {
        if lhs.0 != rhs.0 {
            return Err(MetricsError::MismatchedLabel(
                error_message_mismatched_label(
                    (lhs.0, lhs.3),
                    (rhs.0, rhs.3),
                )));
        }
        if lhs.1 != rhs.1 {
            return Err(MetricsError::MismatchedUnit(
                error_message_mismatched_unit(
                    (lhs.1, lhs.3),
                    (rhs.1, rhs.3),
                )));
        }
        if lhs.2 != rhs.2 {
            return Err(MetricsError::MismatchedTags(
                error_message_mismatched_tags(
                    (lhs.2, lhs.3),
                    (rhs.2, rhs.3),
                )));
        }
        Ok(())
    }

    fn update_duration(
        &mut self,
        first: &SystemTime,
        last: &SystemTime,
    ) {
        if self.first.gt(first) {
            self.first = first.clone();
        }
        if self.last.lt(last) {
            self.last = last.clone();
        }
    }
}

#[derive(Debug)]
pub struct Metric {}

impl Metric {
    fn new(distribution: &Distribution) -> Self {
        Metric {}
    }
}

#[derive(Debug)]
pub struct Metrics {
    tx: Sender<Measurement>
}

impl Metrics {
    pub fn new<F>(
        interval: Duration,
        mut callback: Option<F>,
    ) -> Self
        where
            F: FnMut(&Metric) + Send + 'static
    {
        let (tx, rx): (Sender<Measurement>, Receiver<Measurement>) = mpsc::channel();
        executor().submit(move || {
            let mut distributions: HashMap<String, Distribution> = HashMap::new();
            loop {
                // Retrieve the measurement
                let measurement = match rx.recv() {
                    Ok(m) => m,
                    Err(_) => break
                };
                // Get the distribution for the given measurement
                let key = format!("{}{}{}", measurement.label, measurement.unit,
                                  measurement.tags);
                let mut distribution = match distributions.get_mut(&key) {
                    Some(d) => d,
                    None => {
                        distributions.insert(key.clone(),
                                             Distribution::from(&measurement));
                        distributions.get_mut(&key).unwrap()
                    }
                };
                if !distribution.values.is_empty() {
                    let time = match distribution.first.checked_add(interval) {
                        None => panic!("Failed to determine distribution duration"),
                        Some(s) => s
                    };
                    if time.lt(&measurement.at) {
                        Metrics::metric_from(distribution, &mut callback);
                        distributions.insert(key.clone(),
                                             Distribution::from(&measurement));
                        distribution = distributions.get_mut(&key).unwrap();
                    }
                }
                // Update the distribution with the given measurement
                match distribution.add(&measurement) {
                    Ok(_) => {}
                    Err(e) => match e {
                        MetricsError::Overflow(_) => continue,
                        MetricsError::MismatchedLabel(e)
                        | MetricsError::MismatchedUnit(e)
                        | MetricsError::MismatchedTags(e) => panic!("{}", e)
                    }
                }
            }
            for (_, distribution) in &distributions {
                Metrics::metric_from(distribution, &mut callback);
            }
        });
        Metrics {
            tx
        }
    }

    pub fn submit(
        &self,
        measurement: Measurement,
    ) {
        match self.tx.send(measurement) {
            _ => {} /* ignored */
        }
    }

    fn metric_from<F>(
        distribution: &Distribution,
        callback: &mut Option<F>
    )
        where
            F: FnMut(&Metric) + Send + 'static
    {
        let metric = Metric::new(distribution);
        // TODO: log metric...
        if callback.is_some() {
            let callback = callback.as_mut().unwrap();
            callback(&metric);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use crate::common::CommonError;
    use crate::metrics::*;

    static ERROR_UNEXPECTED_ERROR: &str = "An unexpected error was returned";

    #[test]
    fn tags_successfully_created_using_new() {
        let h: HashMap<String, String> = [
            ("service", "awesome"),
            ("hostname", "localhost")
        ].iter().map(|e| {
            (String::from(e.0), String::from(e.1))
        }).collect();
        let t = Tags::from(h.clone()).unwrap();
        let mut m = BTreeMap::new();
        for (key, value) in &h {
            m.insert(key.clone(), value.clone());
        }
        assert_eq!(m, t.entries);
        println!("{:#?}", t);
    }

    #[test]
    fn tags_error_on_creating_with_empty_key() {
        let error = Tags::from([
            ("", "awesome"),
            ("hostname", "localhost")
        ].iter().map(|e| {
            (String::from(e.0), String::from(e.1))
        }).collect()).unwrap_err();
        match error {
            CommonError::IsEmpty(message) => {
                assert_eq!(message, "key in tags is empty")
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn tags_error_on_creating_with_blank_key() {
        let error = Tags::from([
            (" \t\n", "awesome"),
            ("hostname", "localhost")
        ].iter().map(|e| {
            (String::from(e.0), String::from(e.1))
        }).collect()).unwrap_err();
        match error {
            CommonError::IsBlank(message) => {
                assert_eq!(message, "key in tags is blank")
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn tags_successfully_clone() {
        let t = Tags::new();
        let o = t.clone();
        assert_eq!(t, o);
    }

    #[test]
    fn tags_successfully_merge() {
        let mut t = Tags::from([
            ("service", "awesome"),
            ("hostname", "localhost")
        ].iter().map(|e| {
            (String::from(e.0), String::from(e.1))
        }).collect()).unwrap();
        let o = Tags::from([
            ("service", "other"),
            ("component", "application")
        ].iter().map(|e| {
            (String::from(e.0), String::from(e.1))
        }).collect()).unwrap();
        t.merge(&o);
        assert_eq!(t.entries, [
            ("service", "awesome"),
            ("component", "application"),
            ("hostname", "localhost")
        ].iter().map(|e| {
            (String::from(e.0), String::from(e.1))
        }).collect());
    }

    #[test]
    fn measurement_successfully_created_using_new() {
        let l = "label";
        let v = 20;
        let u = unit::nanoseconds();
        let i = SystemTime::now();
        let m = Measurement::new(
            l, v, u, None).unwrap();
        assert_eq!(l, m.label);
        assert_eq!(v, m.value);
        assert_eq!(u, m.unit);
        assert_eq!(Tags::new(), m.tags);
        // Could be "flaky" if the system clock is updated and skewed backwards
        assert!(m.at.duration_since(i).unwrap().as_nanos() > 0);
        println!("{:#?}", m);
    }

    #[test]
    fn measurement_error_on_creating_with_empty_label() {
        let error = Measurement::new(
            "", 0, unit::bytes(), None).unwrap_err();
        match error {
            CommonError::IsEmpty(message) => {
                assert_eq!(message, "label is empty")
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn measurement_error_on_creating_with_blank_label() {
        let error = Measurement::new(
            "\t", -10, unit::quantity(), None).unwrap_err();
        match error {
            CommonError::IsBlank(message) => {
                assert_eq!(message, "label is blank")
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn measurement_error_on_creating_with_empty_unit() {
        let error = Measurement::new(
            "label", 0, "", None).unwrap_err();
        match error {
            CommonError::IsEmpty(message) => {
                assert_eq!(message, "unit is empty")
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn measurement_error_on_creating_with_blank_unit() {
        let error = Measurement::new(
            "label", -10, "  \n  ", None).unwrap_err();
        match error {
            CommonError::IsBlank(message) => {
                assert_eq!(message, "unit is blank")
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn distribution_successfully_created_using_new() {
        let d = Distribution::new(
            "label", unit::quantity(), &Tags::new()).unwrap();
        println!("{:#?}", d);
    }

    #[test]
    fn distribution_error_on_creating_with_empty_label() {
        let error = Distribution::new(
            "", unit::percentage(), &Tags::new()).unwrap_err();
        match error {
            CommonError::IsEmpty(message) => {
                assert_eq!(message, "label is empty")
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn distribution_error_on_creating_with_blank_label() {
        let error = Distribution::new(
            "\t ", unit::quantity(), &Tags::new()).unwrap_err();
        match error {
            CommonError::IsBlank(message) => {
                assert_eq!(message, "label is blank")
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn distribution_error_on_creating_with_empty_unit() {
        let error = Distribution::new(
            "label", "", &Tags::new()).unwrap_err();
        match error {
            CommonError::IsEmpty(message) => {
                assert_eq!(message, "unit is empty")
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn distribution_error_on_creating_with_blank_unit() {
        let error = Distribution::new(
            "label", "\t\n", &Tags::new()).unwrap_err();
        match error {
            CommonError::IsBlank(message) => {
                assert_eq!(message, "unit is blank")
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn distribution_successfully_created_from_measurement() {
        let m = Measurement::new(
            "label", 33, unit::quantity(), None)
            .unwrap();
        let d = Distribution::from(&m);
        assert_eq!(m.label, d.label);
        assert_eq!(m.unit, d.unit);
        assert_eq!(m.tags, d.tags);
        assert_eq!(d.values, HashMap::new());
    }

    #[test]
    fn distribution_error_on_adding_measurement_with_mismatched_label() {
        let mut d = Distribution::new(
            "memory_used", unit::bytes(), &Tags::new())
            .unwrap();
        let m = Measurement::new(
            "memory_free", 20_000, unit::bytes(), None)
            .unwrap();
        let error = d.add(&m).unwrap_err();
        match error {
            MetricsError::MismatchedLabel(message) => {
                assert_eq!(message, error_message_mismatched_label(
                    (d.label, DISTRIBUTION),
                    (m.label, MEASUREMENT))
                );
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn distribution_error_on_adding_measurement_with_mismatched_unit() {
        let mut d = Distribution::new(
            "memory_used", unit::bytes(), &Tags::new())
            .unwrap();
        let m = Measurement::new(
            "memory_used", 20_000, unit::percentage(), None)
            .unwrap();
        let error = d.add(&m).unwrap_err();
        match error {
            MetricsError::MismatchedUnit(message) => {
                assert_eq!(message, error_message_mismatched_unit(
                    (d.unit, DISTRIBUTION),
                    (m.unit, MEASUREMENT))
                );
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn distribution_error_on_adding_measurement_with_mismatched_tags() {
        let mut d = Distribution::new(
            "memory_used", unit::bytes(), &Tags::new())
            .unwrap();
        let t = Tags::from([
            ("service", "me"),
            ("component", "application")
        ].iter().map(|e| {
            (String::from(e.0), String::from(e.1))
        }).collect()).unwrap();
        let m = Measurement::new(
            "memory_used", 20_000, unit::bytes(), Some(&t))
            .unwrap();
        let error = d.add(&m).unwrap_err();
        match error {
            MetricsError::MismatchedTags(message) => {
                assert_eq!(message, error_message_mismatched_tags(
                    (&d.tags, DISTRIBUTION),
                    (&t, MEASUREMENT))
                );
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn distribution_overflow_error_on_adding_measurement() {
        let mut d = Distribution::new(
            "memory_used", unit::bytes(), &Tags::new())
            .unwrap();
        let key: i128 = 20_000;
        d.values.insert(key, u128::MAX);
        let m = Measurement::new(
            "memory_used", key, unit::bytes(), None)
            .unwrap();
        let error = d.add(&m).unwrap_err();
        match error {
            MetricsError::Overflow(message) => {
                assert_eq!(message, error_message_overflow(
                    "incrementing",
                    &key,
                    d.label,
                    d.unit,
                    &d.tags)
                );
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
        assert_eq!(&u128::MAX, d.values.get(&key).unwrap())
    }

    #[test]
    fn distribution_successfully_add_measurement() {
        let mut d = Distribution::new(
            "memory_used", unit::bytes(), &Tags::new())
            .unwrap();
        let key: i128 = 20_000;
        let m = Measurement::new(
            "memory_used", key, unit::bytes(), None)
            .unwrap();
        assert!(!d.values.contains_key(&key));
        d.add(&m).unwrap();
        assert_eq!(&1, d.values.get(&key).unwrap())
    }

    #[test]
    fn distribution_error_on_merge_with_mismatched_label() {
        let mut d = Distribution::new(
            "memory_used", unit::bytes(), &Tags::new())
            .unwrap();
        let o = Distribution::new(
            "memory_free", unit::bytes(), &Tags::new())
            .unwrap();
        let error = d.merge(&o).unwrap_err();
        match error {
            MetricsError::MismatchedLabel(message) => {
                assert_eq!(message, error_message_mismatched_label(
                    (d.label, DISTRIBUTION),
                    (o.label, DISTRIBUTION))
                );
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn distribution_error_on_merge_with_mismatched_unit() {
        let mut d = Distribution::new(
            "memory_used", unit::bytes(), &Tags::new())
            .unwrap();
        let o = Distribution::new(
            "memory_used", unit::percentage(), &Tags::new())
            .unwrap();
        let error = d.merge(&o).unwrap_err();
        match error {
            MetricsError::MismatchedUnit(message) => {
                assert_eq!(message, error_message_mismatched_unit(
                    (d.unit, DISTRIBUTION),
                    (o.unit, DISTRIBUTION))
                );
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn distribution_error_on_merge_with_mismatched_tags() {
        let t = Tags::from([
            ("service", "another"),
            ("component", "application")
        ].iter().map(|e| {
            (String::from(e.0), String::from(e.1))
        }).collect()).unwrap();
        let mut d = Distribution::new(
            "memory_used", unit::bytes(), &t)
            .unwrap();
        let o = Distribution::new(
            "memory_used", unit::bytes(), &Tags::new())
            .unwrap();
        let error = d.merge(&o).unwrap_err();
        match error {
            MetricsError::MismatchedTags(message) => {
                assert_eq!(message, error_message_mismatched_tags(
                    (&d.tags, DISTRIBUTION),
                    (&o.tags, DISTRIBUTION))
                );
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }
}
