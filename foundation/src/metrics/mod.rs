use std::{error, fmt};
use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::fmt::{Debug, Display, Formatter};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::time::{Duration, SystemTime, Instant};

use crate::common::{CommonError, Domain, required};
use crate::executor;

/// Unit represents the _type_ of a value in [measurements](Measurement)
/// and [metrics](Metric).
pub mod unit;

#[derive(Debug)]
enum MetricsError {
    Common(CommonError),
    /// When an integer overflow error has occurred.
    Overflow(String /* message */),
    /// When [measurements](Measurement) or [distributions](Distribution)
    /// are being aggregated and their `label`s, `tag`s or `unit`s don't match.
    Unrelated(&'static str, /* measurement or distribution */
              &'static str, /* label, tags or unit */
              String, /* value */
              &'static str, /* measurement or distribution */
              String /* value */),
    /// When the duration is calculated for the [distributions](Distribution)
    /// and it is negative.
    NegativeDuration(String /* identity */),
    /// When a position is given that does not map to a value in the
    /// [distributions](Distribution)
    NotFound(u128, /* position */
             String /* identity */),
}

impl error::Error for MetricsError {
    fn source(
        &self
    ) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            MetricsError::Common(ref e) => Some(e),
            _ => None
        }
    }
}

impl Display for MetricsError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> fmt::Result {
        match *self {
            MetricsError::Common(ref error) =>
                write!(f, "{}", error),
            MetricsError::Unrelated(a1, a2, ref a3, b1, ref b2) =>
                write!(f, "{0} with {1} of {2} is unrelated to {3} with {1} of {4}",
                       a1, a2, a3, b1, b2),
            MetricsError::Overflow(ref message) =>
                write!(f, "{}", message),
            MetricsError::NegativeDuration(ref identity) =>
                write!(f, "Negative duration was recorded for {} of {}",
                       DISTRIBUTION, identity),
            MetricsError::NotFound(position, ref identity) =>
                write!(f, "Position of {} was not found for values in {} of {}",
                       position, DISTRIBUTION, identity)
        }
    }
}

impl From<CommonError> for MetricsError {
    fn from(
        error: CommonError
    ) -> Self {
        MetricsError::Common(error)
    }
}

const MEASUREMENT: &str = "measurement";
const DISTRIBUTION: &str = "distribution";

fn identity(
    label: &'static str,
    tags: &Tags,
    unit: &'static str,
) -> String {
    format!("label={} tags={} unit={}", label, tags.to_string(), unit)
}

/// Tags are string key value pairs used to classify and quantify
/// [measurements](Measurement) and [metrics](Metric).
#[derive(Default)]
pub struct Tags {
    entries: BTreeMap<String, String>,
}

impl Tags {
    /// Create an empty `Tags`.
    pub fn new() -> Self {
        Default::default()
    }

    /// Merge two `Tags` together.
    ///
    /// * `other` whose contents we will merge into this instance
    /// _but will not overwrite any existing entries_.
    ///
    /// # Examples
    /// ```
    /// use foundation::metrics::Tags;
    /// use std::convert::TryFrom;
    /// use std::collections::HashMap;
    ///
    /// let mut tags = Tags::try_from([
    ///     ("service", "awesome"),
    ///     ("hostname", "localhost")
    /// ].iter().map(|e| {
    ///   (String::from(e.0), String::from(e.1))
    /// }).collect::<HashMap<String, String>>()).unwrap();
    ///
    /// let other = Tags::try_from([
    ///     ("service", "other"),
    ///     ("component", "application")
    /// ].iter().map(|e| {
    ///   (String::from(e.0), String::from(e.1))
    /// }).collect::<HashMap<String, String>>()).unwrap();
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

impl TryFrom<HashMap<String, String>> for Tags {
    type Error = CommonError;

    /// Attempt to create a `Tags` from the given map.
    ///
    /// * tags whose contents will be used to populate the instance.
    ///
    /// # Errors
    ///
    /// * [CommonError::Empty](CommonError::Empty) if any
    /// _key_ in `tags` is empty.
    /// * [CommonError::Blank](CommonError::Blank) if any
    /// _key_ in `tags` is blank.
    ///
    /// # Examples
    /// ```
    /// use foundation::metrics::Tags;
    /// use std::convert::TryFrom;
    /// use std::collections::HashMap;
    ///
    /// let tags = Tags::try_from([
    ///     ("service", "awesome"),
    ///     ("hostname", "localhost")
    /// ].iter().map(|e| {
    ///   (String::from(e.0), String::from(e.1))
    /// }).collect::<HashMap<String, String>>()).unwrap();
    /// ```
    fn try_from(
        tags: HashMap<String, String>
    ) -> Result<Self, Self::Error> {
        required(&tags, "tags")?;
        let mut map = BTreeMap::new();
        for (key, value) in &tags {
            required(key, "key")?;
            map.insert(key.clone(), value.clone());
        }
        Ok(Tags {
            entries: map
        })
    }
}

impl Debug for Tags {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> fmt::Result {
        f.debug_map()
            .entries(&self.entries)
            .finish()
    }
}

impl Display for Tags {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> fmt::Result {
        let mut output = String::new();
        self.entries
            .iter()
            .map(|e| format!("{}={}", e.0, e.1))
            .fold(true, |first, e| {
                if !first {
                    output.push_str(", ");
                }
                output.push_str(&*e);
                false
            });
        write!(f, "{{{}}}", output)
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
    fn clone(
        &self
    ) -> Self {
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
    /// * [CommonError::Empty](CommonError::Empty) if
    /// `label` or `unit` is empty.
    /// * [CommonError::Blank](CommonError::Blank) if
    /// `label` or `unit` is blank.
    ///
    /// # Examples
    /// ```
    /// use foundation::metrics::{Measurement, Tags};
    /// use foundation::metrics::unit;
    ///
    /// let measurement = Measurement::new("cpu_usage", 12, unit::PERCENTAGE, None);
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

    /// Create a `Measurement` from the duration of executing the given closure.
    ///
    /// The closure's duration will be reported in
    /// [nanoseconds](unit::NANOSECONDS) with the given `label` and `tags`.
    ///
    /// * `label` tells us _what_ we are measuring.
    /// * `tags` allows for classification and grouping of related
    /// measurements.
    ///
    /// # Errors
    /// * [CommonError::Empty](CommonError::Empty) if `label` is empty.
    /// * [CommonError::Blank](CommonError::Blank) if `label` is blank.
    pub fn from_closure<F>(
        label: &'static str,
        tags: Option<&Tags>,
        mut callable: F,
    ) -> Result<Self, CommonError>
        where
            F: FnMut() + Send + 'static
    {
        let start = Instant::now();
        callable();
        let duration = Instant::now().duration_since(start);
        Measurement::new(label, duration.as_nanos() as i128, unit::NANOSECONDS, tags)
    }

    /// Get the `label`.
    pub fn label(
        &self
    ) -> &'static str {
        self.label
    }

    /// Get the `value`.
    pub fn value(
        &self
    ) -> i128 {
        self.value
    }

    /// Get the `unit`.
    pub fn unit(
        &self
    ) -> &'static str {
        self.unit
    }

    /// Get the `tags`.
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
    first: Option<SystemTime>,
    last: Option<SystemTime>,
}

impl Distribution {
    /// Create an empty `Distribution` with the given values.
    ///
    /// * `label` tells us _what_ we are a distribution of.
    /// * `unit` gives us the _type_ of distribution values.
    /// * `tags` allows for classification and grouping of related
    /// distributions.
    ///
    /// # Errors
    /// * [CommonError::Empty](CommonError::Empty) if
    /// `label` or `unit` is empty.
    /// * [CommonError::Blank](CommonError::Blank) if
    /// `label` or `unit` is blank.
    ///
    pub fn new(
        label: &'static str,
        unit: &'static str,
        tags: &Tags,
    ) -> Result<Self, CommonError> {
        required(label, "label")?;
        required(unit, "unit")?;
        Ok(Distribution {
            label,
            unit,
            tags: tags.clone(),
            values: HashMap::new(),
            first: None,
            last: None,
        })
    }

    /// Add the given measurement to the `Distribution`.
    ///
    /// * `measurement` which will be added to the distribution.
    ///
    /// # Errors
    /// * [MetricsError::Unrelated](MetricsError::Unrelated) if `label`, `tags`
    /// or `unit` of the measurement does not match the `label`, `tags` or
    /// `unit` of the distribution.
    /// * [MetricsError::Overflow](MetricsError::Overflow) if the measurement
    /// value caused an integer overflow in the distribution.
    ///
    pub fn add(
        &mut self,
        measurement: &Measurement,
    ) -> Result<(), MetricsError> {
        Distribution::error_if_unrelated(
            (self.label, self.unit, &self.tags, DISTRIBUTION),
            (measurement.label, measurement.unit, &measurement.tags, MEASUREMENT),
        )?;
        let count = self.values
            .entry(measurement.value)
            .or_insert(0);
        match count.checked_add(1) {
            Some(sum) => *count = sum,
            None => return Err(MetricsError::Overflow(
                Distribution::error_message_for_overflow(
                    "incrementing",
                    &measurement.value,
                    self.label,
                    &self.tags,
                    self.unit))),
        }
        let at = Some(measurement.at);
        self.update_duration(&at, &at);
        Ok(())
    }

    /// Merge two `Distribution`s together.
    ///
    /// * `other` the given distribution whose contents will be merged.
    ///
    /// # Errors
    /// * [MetricsError::Unrelated](MetricsError::Unrelated) if `label`, `tags`
    /// or `unit` of the given distribution does not match the `label`, `tags`
    /// or `unit` of this distribution.
    /// * [MetricsError::Overflow](MetricsError::Overflow) if the given
    /// distribution has a value that caused an integer overflow in this
    /// distribution.
    pub fn merge(
        &mut self,
        other: &Self,
    ) -> Result<(), MetricsError> {
        Distribution::error_if_unrelated(
            (self.label, self.unit, &self.tags, DISTRIBUTION),
            (other.label, other.unit, &other.tags, DISTRIBUTION),
        )?;
        let mut values = self.values.clone();
        for (key, value) in &other.values {
            let count = values
                .entry(*key)
                .or_insert(0);
            match count.checked_add(*value) {
                Some(sum) => *count = sum,
                None => return Err(MetricsError::Overflow(
                    Distribution::error_message_for_overflow(
                        "merging",
                        key,
                        self.label,
                        &self.tags,
                        self.unit)))
            }
        }
        self.values = values;
        self.update_duration(&other.first, &other.last);
        Ok(())
    }

    fn error_if_unrelated(
        lhs: (&'static str /*label*/, &'static str /*unit*/, &Tags, &'static str /*type*/),
        rhs: (&'static str /*label*/, &'static str /*unit*/, &Tags, &'static str /*type*/),
    ) -> Result<(), MetricsError> {
        if lhs.0 != rhs.0 {
            return Err(MetricsError::Unrelated(
                lhs.3, "label", lhs.0.to_string(), rhs.3, rhs.0.to_string()));
        }
        if lhs.1 != rhs.1 {
            return Err(MetricsError::Unrelated(
                lhs.3, "unit", lhs.1.to_string(), rhs.3, rhs.1.to_string()));
        }
        if lhs.2 != rhs.2 {
            return Err(MetricsError::Unrelated(
                lhs.3, "tags", lhs.2.to_string(), rhs.3, rhs.2.to_string()));
        }
        Ok(())
    }

    fn error_message_for_overflow(
        op: &'static str,
        key: &i128,
        label: &'static str,
        tags: &Tags,
        unit: &'static str,
    ) -> String {
        format!("Overflow occurred while {} key of {} for {} of {}",
                op, key, DISTRIBUTION, identity(label, tags, unit))
    }

    fn update_duration(
        &mut self,
        first: &Option<SystemTime>,
        last: &Option<SystemTime>,
    ) {
        if self.first.is_none() || self.first.gt(first) {
            self.first = *first;
        }
        if self.last.is_none() || self.last.lt(last) {
            self.last = *last;
        }
    }
}

impl From<&Measurement> for Distribution {
    /// Create an empty `Distribution` from the given measurement.
    ///
    /// * `measurement` which will be used to create the distribution.
    ///
    fn from(
        measurement: &Measurement
    ) -> Self {
        Distribution {
            label: measurement.label,
            unit: measurement.unit,
            tags: measurement.tags.clone(),
            values: HashMap::new(),
            first: None,
            last: None,
        }
    }
}

impl Domain for &Distribution {
    fn check_domain(
        &self,
        name: &'static str,
    ) -> Result<(), CommonError> {
        required(&self.values, name)
    }
}

/// A metric provides you with statistics from the distribution of
/// [measurements](Measurement) used to create it.
#[derive(Debug)]
pub struct Metric {
    label: &'static str,
    tags: BTreeMap<String, String>,
    unit: &'static str,
    count: u128,
    sum: i128,
    max: i128,
    min: i128,
    mode: Option<i128>,
    mean: i128,
    median: i128,
    distribution: BTreeMap<i128, u128>,
    duration: u128,
    at: SystemTime,
    percentiles: HashMap<&'static str, i128>,
}

impl Metric {
    const P05: &'static str = "p05";
    const P25: &'static str = "p25";
    const P50: &'static str = "p50";
    const P75: &'static str = "p75";
    const P90: &'static str = "p90";
    const P95: &'static str = "p95";
    const P99: &'static str = "p99";
    const P99_9: &'static str = "p99.9";
    const P99_99: &'static str = "p99.99";
    const P99_999: &'static str = "p99.999";
    const P99_9999: &'static str = "p99.9999";
    const P99_99999: &'static str = "p99.99999";

    fn new(
        distribution: &Distribution
    ) -> Result<Self, MetricsError> {
        required(distribution, "distribution")?;
        let label = distribution.label;
        let unit = distribution.unit;
        let at = distribution.last.unwrap();
        let identity = identity(label, &distribution.tags, unit);
        let tags = Metric::tags_from(&distribution.tags);
        let duration = Metric::duration_from(distribution, &identity)?;
        let distribution = Metric::distribution_from(distribution);
        let count = Metric::count_from(&distribution, &identity)?;
        let sum = Metric::sum_from(&distribution, &identity)?;
        let max = Metric::max_from(&distribution);
        let min = Metric::min_from(&distribution);
        let mode = Metric::mode_from(&distribution);
        let mean = Metric::mean_from(count, sum);
        let median = Metric::median_from(count, &distribution, &identity)?;
        let percentiles = Metric::percentiles_from(
            count, &distribution, [
                (Metric::P05, 5.0),
                (Metric::P25, 25.0),
                (Metric::P50, 50.0),
                (Metric::P75, 75.0),
                (Metric::P90, 90.0),
                (Metric::P95, 95.0),
                (Metric::P99, 99.0),
                (Metric::P99_9, 99.9),
                (Metric::P99_99, 99.99),
                (Metric::P99_999, 99.999),
                (Metric::P99_9999, 99.9999),
                (Metric::P99_99999, 99.99999)
            ].iter().copied().collect(), &identity)?;
        Ok(Metric {
            label,
            tags,
            unit,
            count,
            sum,
            max,
            min,
            mode,
            mean,
            median,
            distribution,
            duration,
            at,
            percentiles,
        })
    }

    /// Get the `label`.
    ///
    /// A label tells us _what_ we are a metric of.
    pub fn label(
        &self
    ) -> &'static str {
        self.label
    }

    /// Get the `tags`.
    ///
    /// The tags allows for classification and grouping of related metrics.
    pub fn tags(
        &self
    ) -> &BTreeMap<String, String> {
        &self.tags
    }

    /// Get the `unit`.
    ///
    /// A [unit](mod@unit) gives us the _type_ of metric values.
    pub fn unit(
        &self
    ) -> &'static str {
        self.unit
    }

    /// Get the `count`.
    ///
    /// The count represents the number of measurements recorded within the
    /// distribution of this metric.
    pub fn count(
        &self
    ) -> u128 {
        self.count
    }

    /// Get the `sum`.
    ///
    /// The sum is the summation of all the measurements within the
    /// distribution of this metric.
    pub fn sum(
        &self
    ) -> i128 {
        self.sum
    }

    /// Get the `max`.
    ///
    /// The [max](https://en.wikipedia.org/wiki/Sample_maximum_and_minimum) is
    /// the maximum measurement within the distribution of this metric.
    pub fn max(
        &self
    ) -> i128 {
        self.max
    }

    /// Get the `min`.
    ///
    /// The [min](https://en.wikipedia.org/wiki/Sample_maximum_and_minimum) is
    /// the minimum measurement within the distribution of this metric.
    pub fn min(
        &self
    ) -> i128 {
        self.min
    }

    /// Get the `mode`.
    ///
    /// The [mode](https://en.wikipedia.org/wiki/Mode_(statistics)) is the most
    /// frequently occurring measurement within the distribution of this metric.
    pub fn mode(
        &self
    ) -> &Option<i128> {
        &self.mode
    }

    /// Get the `mean`.
    ///
    /// The [mean](https://en.wikipedia.org/wiki/Arithmetic_mean) is the
    /// average value of all the measurement within the distribution of this
    /// metric.
    pub fn mean(
        &self
    ) -> i128 {
        self.mean
    }

    /// Get the `median`.
    ///
    /// The [median](https://en.wikipedia.org/wiki/Median) is the middle value
    /// of all the measurement within the distribution of this
    /// metric.
    pub fn median(
        &self
    ) -> i128 {
        self.median
    }

    /// Get the `distribution`.
    ///
    /// The distribution is in a map where the entry's key is the measurement
    /// and the entry's value is the the number of occurrences observed.
    pub fn distribution(
        &self
    ) -> &BTreeMap<i128, u128> {
        &self.distribution
    }

    /// Get the `duration`.
    ///
    /// The duration is the difference in nanoseconds between the first and
    /// last measurement in the distribution.
    pub fn duration(
        &self
    ) -> u128 {
        self.duration
    }

    /// Get the `at`.
    ///
    /// The at is the time at which this metric was formed. If you take the at
    /// and subtract the duration you will get the time that the first
    /// measurement was observed in the distribution.
    pub fn at(
        &self
    ) -> &SystemTime {
        &self.at
    }

    /// Get the `p05`.
    ///
    /// Return the 5th [percentile](https://en.wikipedia.org/wiki/Percentile)
    /// of the distribution.
    pub fn p05(
        &self
    ) -> i128 {
        *self.percentiles.get(Metric::P05).unwrap()
    }

    /// Get the `p25`.
    ///
    /// Return the 25th [percentile](https://en.wikipedia.org/wiki/Percentile)
    /// of the distribution.
    pub fn p25(
        &self
    ) -> i128 {
        *self.percentiles.get(Metric::P25).unwrap()
    }

    /// Get the `p50`.
    ///
    /// Return the 50th [percentile](https://en.wikipedia.org/wiki/Percentile)
    /// of the distribution.
    pub fn p50(
        &self
    ) -> i128 {
        *self.percentiles.get(Metric::P50).unwrap()
    }

    /// Get the `p75`.
    ///
    /// Return the 75th [percentile](https://en.wikipedia.org/wiki/Percentile)
    /// of the distribution.
    pub fn p75(
        &self
    ) -> i128 {
        *self.percentiles.get(Metric::P75).unwrap()
    }

    /// Get the `p90`.
    ///
    /// Return the 90th [percentile](https://en.wikipedia.org/wiki/Percentile)
    /// of the distribution.
    pub fn p90(
        &self
    ) -> i128 {
        *self.percentiles.get(Metric::P90).unwrap()
    }

    /// Get the `p95`.
    ///
    /// Return the 95th [percentile](https://en.wikipedia.org/wiki/Percentile)
    /// of the distribution.
    pub fn p95(
        &self
    ) -> i128 {
        *self.percentiles.get(Metric::P95).unwrap()
    }

    /// Get the `p99`.
    ///
    /// Return the 99th [percentile](https://en.wikipedia.org/wiki/Percentile)
    /// of the distribution.
    pub fn p99(
        &self
    ) -> i128 {
        *self.percentiles.get(Metric::P99).unwrap()
    }

    /// Get the `p99.9`.
    ///
    /// Return the 99.9th
    /// [percentile](https://en.wikipedia.org/wiki/Percentile) of the
    /// distribution.
    pub fn p99_9(
        &self
    ) -> i128 {
        *self.percentiles.get(Metric::P99_9).unwrap()
    }

    /// Get the `p99.99`.
    ///
    /// Return the 99.99th
    /// [percentile](https://en.wikipedia.org/wiki/Percentile) of the
    /// distribution.
    pub fn p99_99(
        &self
    ) -> i128 {
        *self.percentiles.get(Metric::P99_99).unwrap()
    }

    /// Get the `p99.999`.
    ///
    /// Return the 99.999th
    /// [percentile](https://en.wikipedia.org/wiki/Percentile) of the
    /// distribution.
    pub fn p99_999(
        &self
    ) -> i128 {
        *self.percentiles.get(Metric::P99_999).unwrap()
    }

    /// Get the `p99.9999`.
    ///
    /// Return the 99.9999th
    /// [percentile](https://en.wikipedia.org/wiki/Percentile) of the
    /// distribution.
    pub fn p99_9999(
        &self
    ) -> i128 {
        *self.percentiles.get(Metric::P99_9999).unwrap()
    }

    /// Get the `p99.99999`.
    ///
    /// Return the 99.99999th
    /// [percentile](https://en.wikipedia.org/wiki/Percentile) of the
    /// distribution.
    pub fn p99_99999(
        &self
    ) -> i128 {
        *self.percentiles.get(Metric::P99_99999).unwrap()
    }

    fn tags_from(
        tags: &Tags
    ) -> BTreeMap<String, String> {
        tags.entries.clone()
    }

    fn distribution_from(
        distribution: &Distribution
    ) -> BTreeMap<i128, u128> {
        let mut map = BTreeMap::new();
        for (key, value) in distribution.values.clone() {
            map.insert(key, value);
        }
        map
    }

    fn count_from(
        distribution: &BTreeMap<i128, u128>,
        identity: &str,
    ) -> Result<u128, MetricsError> {
        let mut count: u128 = 0;
        for value in distribution.values() {
            match count.checked_add(*value) {
                Some(r) => count = r,
                None => return Err(MetricsError::Overflow(
                    Metric::error_message_for_overflow("count", identity)))
            }
        }
        Ok(count)
    }

    fn sum_from(
        distribution: &BTreeMap<i128, u128>,
        identity: &str,
    ) -> Result<i128, MetricsError> {
        let mut sum: i128 = 0;
        for (key, value) in distribution {
            if *value > i128::MAX as u128 {
                return Err(Metric::error_for_overflow_in_sum(identity));
            }
            let x = match key.checked_mul(*value as i128) {
                Some(r) => r,
                None => return Err(Metric::error_for_overflow_in_sum(identity))
            };
            match sum.checked_add(x) {
                Some(r) => sum = r,
                None => return Err(Metric::error_for_overflow_in_sum(identity))
            }
        }
        Ok(sum)
    }

    fn max_from(
        distribution: &BTreeMap<i128, u128>
    ) -> i128 {
        return *distribution.keys().next_back().unwrap();
    }

    fn min_from(
        distribution: &BTreeMap<i128, u128>
    ) -> i128 {
        return *distribution.keys().next().unwrap();
    }

    fn mode_from(
        distribution: &BTreeMap<i128, u128>
    ) -> Option<i128> {
        let mut map = BTreeMap::new();
        for (key, value) in distribution {
            let vec = match map.get_mut(value) {
                Some(v) => v,
                None => {
                    map.insert(value, Vec::new());
                    map.get_mut(value).unwrap()
                }
            };
            vec.push(key);
        }
        if !map.is_empty() {
            let vec = map.values().next_back().unwrap();
            if vec.len() == 1 {
                return Some(**vec.first().unwrap());
            }
        }
        None
    }

    fn mean_from(
        count: u128,
        sum: i128,
    ) -> i128 {
        sum / count as i128
    }

    fn median_from(
        count: u128,
        distribution: &BTreeMap<i128, u128>,
        identity: &str,
    ) -> Result<i128, MetricsError> {
        let mut sum = 0;
        let position = count / 2;
        let limit = 1 + count % 2;
        for i in 0..limit {
            let x = Metric::nearest_rank(i + position, distribution, identity)?;
            sum = match x.checked_add(sum) {
                Some(r) => r,
                None => return Err(MetricsError::Overflow(
                    Metric::error_message_for_overflow("median", identity)))
            };
        }
        Ok(sum / limit as i128)
    }

    fn duration_from(
        distribution: &Distribution,
        identity: &str,
    ) -> Result<u128, MetricsError> {
        let last = distribution.last.unwrap();
        let first = distribution.first.unwrap();
        match last.duration_since(first) {
            Ok(d) => Ok(d.as_nanos()),
            Err(_) => Err(MetricsError::NegativeDuration(identity.to_string()))
        }
    }

    // https://en.wikipedia.org/wiki/Percentile#The_nearest-rank_method
    fn nearest_rank(
        position: u128,
        distribution: &BTreeMap<i128, u128>,
        identity: &str,
    ) -> Result<i128, MetricsError> {
        let mut lower = 0;
        for (key, value) in distribution {
            let upper = match value.checked_add(lower) {
                Some(r) => r,
                None => return Err(Metric::error_for_overflow_in_nearest_rank(position, identity))
            };
            if position <= upper && position >= lower {
                return Ok(*key);
            }
            lower = upper;
        }
        Err(MetricsError::NotFound(position, identity.to_string()))
    }

    fn percentiles_from(
        count: u128,
        distribution: &BTreeMap<i128, u128>,
        percentiles: BTreeMap<&'static str, f64>,
        identity: &str,
    ) -> Result<HashMap<&'static str, i128>, MetricsError> {
        let mut it = distribution.iter();
        let mut map = HashMap::new();
        let mut lower = 0;
        let mut option = it.next();
        if option.is_some() {
            for (p_key, p_value) in percentiles {
                let position = p_value / 100.0 * count as f64;
                let position = position.ceil() as u128;
                loop {
                    let (key, value) = option.unwrap();
                    let upper = match value.checked_add(lower) {
                        Some(r) => r,
                        None => return Err(Metric::error_for_overflow_in_percentiles(
                            p_key, identity))
                    };
                    if position <= upper && position > lower {
                        map.insert(p_key, *key);
                        break;
                    }
                    lower = upper;
                    option = it.next();
                    if option.is_none() {
                        panic!("Unable to determine all the requested percentiles");
                    }
                }
            }
        }
        Ok(map)
    }

    fn error_message_for_overflow(
        op: &str,
        identity: &str,
    ) -> String {
        format!("An overflow occurred while determining {} from {} of {}",
                op, DISTRIBUTION, identity)
    }

    fn error_for_overflow_in_sum(
        identity: &str
    ) -> MetricsError {
        MetricsError::Overflow(
            Metric::error_message_for_overflow("sum", identity))
    }

    fn error_for_overflow_in_nearest_rank(
        position: u128,
        identity: &str,
    ) -> MetricsError {
        MetricsError::Overflow(Metric::error_message_for_overflow(
            &*format!("nearest rank of {} ", position), identity)
        )
    }

    fn error_for_overflow_in_percentiles(
        percentile: &'static str,
        identity: &str,
    ) -> MetricsError {
        MetricsError::Overflow(Metric::error_message_for_overflow(
            &*format!("percentile of {} ", percentile), identity)
        )
    }
}

/// Metrics is the way by which you submit [measurements](Measurement) to
/// become [metrics](Metric).
#[derive(Debug)]
pub struct Metrics {
    tx: Sender<Measurement>,
}

impl Metrics {
    /// Create a new `Metrics` handle with the given interval and callback.
    ///
    /// * `interval` the maximum time that a metric can span.
    /// * `callback` that is invoked with the newly created metric passed in.
    pub fn new<F>(
        interval: Duration,
        mut callback: F,
    ) -> Self
        where
            F: FnMut(&Metric) + Send + 'static
    {
        let (tx, rx): (Sender<Measurement>, Receiver<Measurement>) = mpsc::channel();
        executor().submit(move || {
            let mut distributions: HashMap<String, Distribution> = HashMap::new();
            while let Ok(measurement) = rx.recv() {
                // Get the distribution for the given measurement
                let key = format!("{}{}{}", measurement.label, measurement.tags,
                                  measurement.unit);
                let mut distribution = distributions.entry(key.clone())
                    .or_insert_with(|| Distribution::from(&measurement));
                if !distribution.values.is_empty() {
                    let first = distribution.first.unwrap();
                    let time = match first.checked_add(interval) {
                        Some(s) => s,
                        None => panic!("Failed to determine distribution duration")
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
                        MetricsError::Unrelated(..) => panic!("{}", e),
                        _ => continue
                    }
                }
            }
            for distribution in distributions.values() {
                Metrics::metric_from(distribution, &mut callback);
            }
        });
        Metrics {
            tx
        }
    }

    /// Submit a [measurements](Measurement).
    pub fn submit(
        &self,
        measurement: Measurement,
    ) {
        self.tx.send(measurement).ok();
    }

    fn metric_from<F>(
        distribution: &Distribution,
        callback: &mut F,
    )
        where
            F: FnMut(&Metric) + Send + 'static
    {
        let metric = match Metric::new(distribution) {
            Ok(m) => m,
            Err(error) => {
                //TODO: log the error ....
                return;
            }
        };
        // TODO: output the metric
        callback(&metric);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use crate::common::CommonError;
    use crate::metrics::*;
    use crate::metrics::MetricsError;
    use std::array::IntoIter;
    use std::iter::FromIterator;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;

    static ERROR_UNEXPECTED_ERROR: &str = "An unexpected error was returned";

    #[test]
    fn tags_successfully_created_using_try_from() {
        let h: HashMap<String, String> = [
            ("service", "awesome"),
            ("hostname", "localhost")
        ].iter().map(|e| {
            (String::from(e.0), String::from(e.1))
        }).collect();
        let t = Tags::try_from(h.clone()).unwrap();
        let mut m = BTreeMap::new();
        for (key, value) in &h {
            m.insert(key.clone(), value.clone());
        }
        assert_eq!(m, t.entries);
        println!("{:#?}", t);
    }

    #[test]
    fn tags_error_on_creating_with_empty_key() {
        let error = Tags::try_from([
            ("", "awesome"),
            ("hostname", "localhost")
        ].iter().map(|e| {
            (String::from(e.0), String::from(e.1))
        }).collect::<HashMap<String, String>>()).unwrap_err();
        match error {
            CommonError::Empty(_) => {
                assert_eq!("'key' is empty", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn tags_error_on_creating_with_blank_key() {
        let error = Tags::try_from([
            (" \t\n", "awesome"),
            ("hostname", "localhost")
        ].iter().map(|e| {
            (String::from(e.0), String::from(e.1))
        }).collect::<HashMap<String, String>>()).unwrap_err();
        match error {
            CommonError::Blank(_) => {
                assert_eq!("'key' is blank", format!("{}", error));
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
        let mut t = Tags::try_from([
            ("service", "awesome"),
            ("hostname", "localhost")
        ].iter().map(|e| {
            (String::from(e.0), String::from(e.1))
        }).collect::<HashMap<String, String>>()).unwrap();
        let o = Tags::try_from([
            ("service", "other"),
            ("component", "application")
        ].iter().map(|e| {
            (String::from(e.0), String::from(e.1))
        }).collect::<HashMap<String, String>>()).unwrap();
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
        let u = unit::NANOSECONDS;
        let i = SystemTime::now();
        let m = Measurement::new(
            l, v, u, None).unwrap();
        assert_eq!(l, m.label());
        assert_eq!(v, m.value());
        assert_eq!(u, m.unit());
        assert_eq!(Tags::new(), m.tags);
        // Could be "flaky" if the system clock is updated and skewed backwards
        assert!(m.at.duration_since(i).unwrap().as_nanos() > 0);
        println!("{:#?}", m);
    }

    #[test]
    fn measurement_error_on_creating_with_empty_label() {
        let error = Measurement::new(
            "", 0, unit::BYTES, None).unwrap_err();
        match error {
            CommonError::Empty(_) => {
                assert_eq!("'label' is empty", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn measurement_error_on_creating_with_blank_label() {
        let error = Measurement::new(
            "\t", -10, unit::QUANTITY, None).unwrap_err();
        match error {
            CommonError::Blank(_) => {
                assert_eq!("'label' is blank", format!("{}", error));
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
            CommonError::Empty(_) => {
                assert_eq!("'unit' is empty", format!("{}", error));
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
            CommonError::Blank(_) => {
                assert_eq!("'unit' is blank", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn measurement_successfully_created_from_closure() {
        let m = Measurement::from_closure("test_timing", None, || {
            thread::sleep(Duration::from_millis(100));
        }).unwrap();
        assert_eq!("test_timing", m.label());
        assert_eq!(&Tags::new(), m.tags());
        assert_eq!(unit::NANOSECONDS, m.unit());
        let delay = 100_000_000 as i128;
        assert!(m.value().ge(&delay));
    }

    #[test]
    fn distribution_successfully_created_using_new() {
        let d = Distribution::new(
            "label", unit::QUANTITY, &Tags::new()).unwrap();
        println!("{:#?}", d);
    }

    #[test]
    fn distribution_error_on_creating_with_empty_label() {
        let error = Distribution::new(
            "", unit::PERCENTAGE, &Tags::new()).unwrap_err();
        match error {
            CommonError::Empty(_) => {
                assert_eq!("'label' is empty", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn distribution_error_on_creating_with_blank_label() {
        let error = Distribution::new(
            "\t ", unit::QUANTITY, &Tags::new()).unwrap_err();
        match error {
            CommonError::Blank(_) => {
                assert_eq!("'label' is blank", format!("{}", error));
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
            CommonError::Empty(_) => {
                assert_eq!("'unit' is empty", format!("{}", error));
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
            CommonError::Blank(_) => {
                assert_eq!("'unit' is blank", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn distribution_successfully_created_from_measurement() {
        let m = Measurement::new(
            "label", 33, unit::QUANTITY, None)
            .unwrap();
        let d = Distribution::from(&m);
        assert_eq!(m.label, d.label);
        assert_eq!(m.unit, d.unit);
        assert_eq!(m.tags, d.tags);
        assert_eq!(d.values, HashMap::new());
    }

    #[test]
    fn distribution_error_on_adding_measurement_with_unrelated_label() {
        let mut d = Distribution::new(
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        let m = Measurement::new(
            "memory_free", 20_000, unit::BYTES, None)
            .unwrap();
        let error = d.add(&m).unwrap_err();
        match error {
            MetricsError::Unrelated(..) => {
                assert_eq!("distribution with label of memory_used is unrelated to measurement \
                with label of memory_free", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn distribution_error_on_adding_measurement_with_unrelated_unit() {
        let mut d = Distribution::new(
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        let m = Measurement::new(
            "memory_used", 20_000, unit::PERCENTAGE, None)
            .unwrap();
        let error = d.add(&m).unwrap_err();
        match error {
            MetricsError::Unrelated(..) => {
                assert_eq!("distribution with unit of bytes is unrelated to measurement \
                with unit of percentage", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn distribution_error_on_adding_measurement_with_unrelated_tags() {
        let mut d = Distribution::new(
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        let t = Tags::try_from([
            ("service", "me"),
            ("component", "application")
        ].iter().map(|e| {
            (String::from(e.0), String::from(e.1))
        }).collect::<HashMap<String, String>>()).unwrap();
        let m = Measurement::new(
            "memory_used", 20_000, unit::BYTES, Some(&t))
            .unwrap();
        let error = d.add(&m).unwrap_err();
        match error {
            MetricsError::Unrelated(..) => {
                assert_eq!("distribution with tags of {} is unrelated to measurement with \
                tags of {component=application, service=me}", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn distribution_overflow_error_on_adding_measurement() {
        let mut d = Distribution::new(
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        let key: i128 = 20_000;
        d.values.insert(key, u128::MAX);
        let m = Measurement::new(
            "memory_used", key, unit::BYTES, None)
            .unwrap();
        let error = d.add(&m).unwrap_err();
        match error {
            MetricsError::Overflow(_) => {
                assert_eq!("Overflow occurred while incrementing key of 20000 for \
                distribution of label=memory_used tags={} unit=bytes", format!("{}", error));
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
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        let key: i128 = 20_000;
        let m = Measurement::new(
            "memory_used", key, unit::BYTES, None)
            .unwrap();
        assert!(!d.values.contains_key(&key));
        d.add(&m).unwrap();
        assert_eq!(&1, d.values.get(&key).unwrap())
    }

    #[test]
    fn distribution_error_on_merge_with_unrelated_label() {
        let mut d = Distribution::new(
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        let o = Distribution::new(
            "memory_free", unit::BYTES, &Tags::new())
            .unwrap();
        let error = d.merge(&o).unwrap_err();
        match error {
            MetricsError::Unrelated(..) => {
                assert_eq!("distribution with label of memory_used is unrelated to \
                distribution with label of memory_free", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn distribution_error_on_merge_with_unrelated_unit() {
        let mut d = Distribution::new(
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        let o = Distribution::new(
            "memory_used", unit::PERCENTAGE, &Tags::new())
            .unwrap();
        let error = d.merge(&o).unwrap_err();
        match error {
            MetricsError::Unrelated(..) => {
                assert_eq!("distribution with unit of bytes is unrelated to distribution \
                with unit of percentage", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn distribution_error_on_merge_with_unrelated_tags() {
        let t = Tags::try_from([
            ("service", "another"),
            ("component", "application")
        ].iter().map(|e| {
            (String::from(e.0), String::from(e.1))
        }).collect::<HashMap<String, String>>()).unwrap();
        let mut d = Distribution::new(
            "memory_used", unit::BYTES, &t)
            .unwrap();
        let o = Distribution::new(
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        let error = d.merge(&o).unwrap_err();
        match error {
            MetricsError::Unrelated(..) => {
                assert_eq!("distribution with tags of {component=application, service=another} \
                is unrelated to distribution with tags of {}", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn metric_error_on_empty_distribution() {
        let d = Distribution::new(
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        let error = Metric::new(&d).unwrap_err();
        match error {
            MetricsError::Common(_) => {
                assert_eq!("'distribution' is empty", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn metric_error_on_negative_duration() {
        let mut d = Distribution::new(
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        d.values.insert(0, 12);
        d.first = Some(SystemTime::now());
        d.last = Some(SystemTime::UNIX_EPOCH);
        let error = Metric::new(&d).unwrap_err();
        match error {
            MetricsError::NegativeDuration(_) => {
                assert_eq!("Negative duration was recorded for distribution of \
                label=memory_used tags={} unit=bytes", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn metric_error_on_overflow_count() {
        let mut d = Distribution::new(
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        d.values.insert(-1, 1);
        d.values.insert(0, u128::MAX);
        d.first = Some(SystemTime::UNIX_EPOCH);
        d.last = Some(SystemTime::now());
        let error = Metric::new(&d).unwrap_err();
        match error {
            MetricsError::Overflow(_) => {
                assert_eq!("An overflow occurred while determining count from \
                distribution of label=memory_used tags={} unit=bytes", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn metric_error_on_overflow_sum() {
        let mut d = Distribution::new(
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        // case that sum is a i128 type and can not hold a u128 value
        d.values.insert(2, u128::MAX);
        d.first = Some(SystemTime::UNIX_EPOCH);
        d.last = Some(SystemTime::now());
        let error = Metric::new(&d).unwrap_err();
        match error {
            MetricsError::Overflow(_) => {
                assert_eq!("An overflow occurred while determining sum from distribution \
                of label=memory_used tags={} unit=bytes", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
        d.values.clear();
        // case that key multiplied by value causes an overflow
        d.values.insert(2, i128::MAX as u128);
        let error = Metric::new(&d).unwrap_err();
        match error {
            MetricsError::Overflow(_) => {
                assert_eq!("An overflow occurred while determining sum from distribution \
                of label=memory_used tags={} unit=bytes", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
        d.values.clear();
        // case when we add all the distribution entries causes an overflow
        d.values.insert(1, i128::MAX as u128);
        d.values.insert(2, (i128::MAX / 2) as u128);
        d.values.insert(3, 1);
        let error = Metric::new(&d).unwrap_err();
        match error {
            MetricsError::Overflow(_) => {
                assert_eq!("An overflow occurred while determining sum from distribution \
                of label=memory_used tags={} unit=bytes", format!("{}", error));
            }
            _ => {
                panic!("{}", ERROR_UNEXPECTED_ERROR);
            }
        }
    }

    #[test]
    fn metric_successfully_created_using_new() {
        let mut d = Distribution::new(
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        d.values.insert(1, 7);
        d.values.insert(2, 17);
        d.values.insert(8, 5);
        let first = SystemTime::UNIX_EPOCH;
        let last = SystemTime::now();
        d.first = Some(first);
        d.last = Some(last);
        let m = Metric::new(&d).unwrap();
        assert_eq!(d.label, m.label());
        assert_eq!(d.unit, m.unit());
        assert!(m.tags().is_empty());
        assert_eq!(last.duration_since(first).unwrap().as_nanos(), m.duration());
        assert_eq!(&last, m.at());
        assert_eq!(&BTreeMap::from_iter(IntoIter::new([(1, 7), (2, 17), (8, 5)])),
                   m.distribution());
        assert_eq!(29, m.count());
        assert_eq!(81, m.sum());
        assert_eq!(8, m.max());
        assert_eq!(1, m.min());
        assert_eq!(2, m.mode().unwrap());
        assert_eq!(2, m.mean());
        assert_eq!(2, m.median());
        assert_eq!(1, m.p05());
        assert_eq!(2, m.p25());
        assert_eq!(2, m.p50());
        assert_eq!(2, m.p75());
        assert_eq!(8, m.p90());
        assert_eq!(8, m.p95());
        assert_eq!(8, m.p99());
        assert_eq!(8, m.p99_9());
        assert_eq!(8, m.p99_99());
        assert_eq!(8, m.p99_999());
        assert_eq!(8, m.p99_9999());
        assert_eq!(8, m.p99_99999());
    }

    #[test]
    fn metrics_successfully_created_with_trivial_example() {
        let callback_was_called = Arc::new(AtomicBool::new(false));
        let clone = callback_was_called.clone();
        // Setting interval to 0 will only allow measurements that were created at the same
        // nanosecond become part of the metric
        let m = Metrics::new(Duration::from_secs(0), move |metric| {
            clone.store(true, Ordering::SeqCst);
            assert_eq!("cpu", metric.label());
            assert_eq!(unit::PERCENTAGE, metric.unit());
            assert!(metric.tags().is_empty());
            assert_eq!(0, metric.duration);
            assert_eq!(&BTreeMap::from_iter(IntoIter::new([(1, 1)])),
                       metric.distribution());
            assert_eq!(1, metric.count());
            assert_eq!(1, metric.sum());
            assert_eq!(1, metric.max());
            assert_eq!(1, metric.min());
            assert_eq!(1, metric.mode().unwrap());
            assert_eq!(1, metric.mean());
            assert_eq!(1, metric.median());
            assert_eq!(1, metric.p05());
            assert_eq!(1, metric.p25());
            assert_eq!(1, metric.p50());
            assert_eq!(1, metric.p75());
            assert_eq!(1, metric.p90());
            assert_eq!(1, metric.p95());
            assert_eq!(1, metric.p99());
            assert_eq!(1, metric.p99_9());
            assert_eq!(1, metric.p99_99());
            assert_eq!(1, metric.p99_999());
            assert_eq!(1, metric.p99_9999());
            assert_eq!(1, metric.p99_99999());
        });
        // first measurement is recorded
        let measurement = Measurement::new(
            "cpu", 1, unit::PERCENTAGE, None).unwrap();
        m.submit(measurement);
        // second measurement causes the creation of the metric and use of the callback
        let measurement = Measurement::new(
            "cpu", 1, unit::PERCENTAGE, None).unwrap();
        m.submit(measurement);
        thread::sleep(Duration::from_secs(1));
        assert!(callback_was_called.load(Ordering::SeqCst));
    }

    #[test]
    fn metrics_successfully_created() {
        let callback_was_called = Arc::new(AtomicBool::new(false));
        let clone = callback_was_called.clone();
        let m = Metrics::new(Duration::from_secs(1), move |metric| {
            clone.store(true, Ordering::SeqCst);
            assert_eq!("cpu", metric.label());
            assert_eq!(unit::PERCENTAGE, metric.unit());
            assert!(metric.tags().is_empty());
            assert_eq!(100, metric.count());
            assert_eq!(4950, metric.sum());
            assert_eq!(99, metric.max());
            assert_eq!(0, metric.min());
            assert_eq!(&None, metric.mode());
            assert_eq!(49, metric.mean());
            assert_eq!(49, metric.median());
            assert_eq!(4, metric.p05());
            assert_eq!(24, metric.p25());
            assert_eq!(49, metric.p50());
            assert_eq!(74, metric.p75());
            assert_eq!(89, metric.p90());
            assert_eq!(94, metric.p95());
            assert_eq!(98, metric.p99());
            assert_eq!(99, metric.p99_9());
            assert_eq!(99, metric.p99_99());
            assert_eq!(99, metric.p99_999());
            assert_eq!(99, metric.p99_9999());
            assert_eq!(99, metric.p99_99999());
        });
        for i in 0..100 {
            let measurement = Measurement::new(
                "cpu", i, unit::PERCENTAGE, None).unwrap();
            m.submit(measurement);
        }
        thread::sleep(Duration::from_secs(1));
        let measurement = Measurement::new(
            "cpu", 0, unit::PERCENTAGE, None).unwrap();
        m.submit(measurement);
        thread::sleep(Duration::from_secs(1));
        assert!(callback_was_called.load(Ordering::SeqCst));
    }
}
