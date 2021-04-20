use std::collections::{BTreeMap, HashMap};
use std::fmt::{Display, Formatter};
use std::fmt;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::time::{Duration, SystemTime};

use crate::common::{CommonError, Domain, required};
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

const MEASUREMENT: &str = "Measurement";
const DISTRIBUTION: &str = "Distribution";

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
    /// * [CommonError::IsEmpty](CommonError::IsEmpty) if any
    /// _key_ in `tags` is empty.
    /// * [CommonError::IsInvalid](CommonError::IsInvalid) if any
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
    /// * [CommonError::IsEmpty](CommonError::IsEmpty) if
    /// `label` or `unit` is empty.
    /// * [CommonError::IsInvalid](CommonError::IsInvalid) if
    /// `label` or `unit` is blank.
    ///
    /// # Examples
    /// ```
    /// use foundation::metrics::{Measurement, Tags};
    /// use foundation::metrics::unit;
    /// use std::collections::HashMap;
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
    /// * [CommonError::IsEmpty](CommonError::IsEmpty) if
    /// `label` or `unit` is empty.
    /// * [CommonError::IsInvalid](CommonError::IsInvalid) if
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

impl Domain for &Distribution {
    fn check_domain(
        &self,
        name: &'static str,
    ) -> Result<(), CommonError> {
        required(&self.values, name)
    }
}

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
    ) -> Result<Self, CommonError> {
        required(distribution, "distribution")?;

        let label = distribution.label;
        let unit = distribution.unit;
        let at = distribution.last;
        let tags = Metric::tags_from(&distribution.tags);
        let duration = Metric::duration_from(distribution)?;
        let distribution = Metric::distribution_from(distribution);
        let count = Metric::count_from(&distribution)?;
        let sum = Metric::sum_from(&distribution)?;
        let max = Metric::max_from(&distribution);
        let min = Metric::min_from(&distribution);
        let mode = Metric::mode_from(&distribution);
        let mean = Metric::mean_from(count, sum);
        let median = Metric::median_from(count, &distribution)?;
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
            ].iter().copied().collect())?;

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

    pub fn label(
        &self
    ) -> &'static str {
        self.label
    }

    pub fn tags(
        &self
    ) -> &BTreeMap<String, String> {
        &self.tags
    }

    pub fn unit(
        &self
    ) -> &'static str {
        self.unit
    }

    pub fn count(
        &self
    ) -> u128 {
        self.count
    }

    pub fn sum(
        &self
    ) -> i128 {
        self.sum
    }

    pub fn max(
        &self
    ) -> i128 {
        self.max
    }

    pub fn min(
        &self
    ) -> i128 {
        self.min
    }

    pub fn mode(
        &self
    ) -> &Option<i128> {
        &self.mode
    }

    pub fn mean(
        &self
    ) -> i128 {
        self.mean
    }

    pub fn median(
        &self
    ) -> i128 {
        self.median
    }

    pub fn distribution(
        &self
    ) -> &BTreeMap<i128, u128> {
        &self.distribution
    }

    pub fn duration(
        &self
    ) -> u128 {
        self.duration
    }

    pub fn at(
        &self
    ) -> &SystemTime {
        &self.at
    }

    pub fn p05(
        &self
    ) -> &i128 {
        self.percentiles.get(Metric::P05).unwrap()
    }

    pub fn p25(
        &self
    ) -> &i128 {
        self.percentiles.get(Metric::P25).unwrap()
    }

    pub fn p50(
        &self
    ) -> &i128 {
        self.percentiles.get(Metric::P50).unwrap()
    }

    pub fn p75(
        &self
    ) -> &i128 {
        self.percentiles.get(Metric::P75).unwrap()
    }

    pub fn p90(
        &self
    ) -> &i128 {
        self.percentiles.get(Metric::P90).unwrap()
    }

    pub fn p95(
        &self
    ) -> &i128 {
        self.percentiles.get(Metric::P95).unwrap()
    }

    pub fn p99(
        &self
    ) -> &i128 {
        self.percentiles.get(Metric::P99).unwrap()
    }

    pub fn p99_9(
        &self
    ) -> &i128 {
        self.percentiles.get(Metric::P99_9).unwrap()
    }

    pub fn p99_99(
        &self
    ) -> &i128 {
        self.percentiles.get(Metric::P99_99).unwrap()
    }

    pub fn p99_999(
        &self
    ) -> &i128 {
        self.percentiles.get(Metric::P99_999).unwrap()
    }

    pub fn p99_9999(
        &self
    ) -> &i128 {
        self.percentiles.get(Metric::P99_9999).unwrap()
    }

    pub fn p99_99999(
        &self
    ) -> &i128 {
        self.percentiles.get(Metric::P99_99999).unwrap()
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
        distribution: &BTreeMap<i128, u128>
    ) -> Result<u128, CommonError> {
        let mut count: u128 = 0;
        for (_, value) in distribution {
            match count.checked_add(*value) {
                None => {
                    return Err(CommonError::IsInvalid(
                        format!("An overflow occurred while determining count")));
                }
                Some(r) => {
                    count = r;
                }
            }
        }
        Ok(count)
    }

    fn sum_from(
        distribution: &BTreeMap<i128, u128>
    ) -> Result<i128, CommonError> {
        let mut sum: i128 = 0;
        for (key, value) in distribution {
            let x = match key.checked_mul(*value as i128) {
                None => {
                    return Err(CommonError::IsInvalid(
                        format!("An overflow occurred while determining sum")));
                }
                Some(r) => r
            };
            match sum.checked_add(x) {
                None => {}
                Some(r) => {
                    sum = r;
                }
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
    ) -> Result<i128, CommonError> {
        let mut sum = 0;
        let position = count / 2;
        let limit = 1 + count % 2;
        for i in 0..limit {
            let x = match Metric::value_at(i + position, distribution) {
                None => {
                    return Err(CommonError::IsInvalid(format!("Failed to determine median")));
                }
                Some(r) => r
            };
            sum = match x.checked_add(sum) {
                None => {
                    return Err(CommonError::IsInvalid(
                        format!("Overflow occurred while determining median")));
                }
                Some(r) => r
            };
        }
        Ok(sum / limit as i128)
    }

    fn duration_from(
        distribution: &Distribution
    ) -> Result<u128, CommonError> {
        match distribution.last.duration_since(distribution.first) {
            Ok(d) => Ok(d.as_nanos()),
            Err(_) => {
                Err(CommonError::IsInvalid(
                    format!("Negative duration is not allowed")))
            }
        }
    }

    fn value_at(
        position: u128,
        distribution: &BTreeMap<i128, u128>,
    ) -> Option<i128> {
        let mut lower = 0;
        for (key, value) in distribution {
            let upper = match value.checked_add(lower) {
                None => return None,
                Some(r) => r
            };
            if position < upper && position >= lower {
                return Some(*key);
            }
            lower = upper;
        }
        None
    }

    // https://en.wikipedia.org/wiki/Percentile#The_nearest-rank_method
    fn percentiles_from(
        count: u128,
        distribution: &BTreeMap<i128, u128>,
        percentiles: BTreeMap<&'static str, f64>,
    ) -> Result<HashMap<&'static str, i128>, CommonError> {
        let mut it = distribution.iter();
        let mut map = HashMap::new();
        let mut lower = 0;
        for (p_key, p_value) in percentiles {
            let position = p_value / 100.0 * count as f64;
            let position = position.round() as u128;
            while let Some((key, value)) = it.next() {
                let upper = match value.checked_add(lower) {
                    None => {
                        return Err(CommonError::IsInvalid(
                            format!("Overflow occurred while determining percentiles")));
                    }
                    Some(r) => r
                };
                let found = position < upper && position >= lower;
                lower = upper;
                if found {
                    map.insert(p_key, *key);
                    break;
                }
            }
            break;
        }
        Ok(map)
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
                let mut distribution = distributions.entry(key.clone())
                    .or_insert(Distribution::from(&measurement));
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
        callback: &mut Option<F>,
    )
        where
            F: FnMut(&Metric) + Send + 'static
    {
        let metric = match Metric::new(distribution) {
            Ok(m) => m,
            Err(_) => return //TODO: log an error ....
        };
        // TODO: log metric...
        if callback.is_some() {
            callback.as_mut().unwrap()(&metric);
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
            CommonError::IsInvalid(message) => {
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
        let u = unit::NANOSECONDS;
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
            "", 0, unit::BYTES, None).unwrap_err();
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
            "\t", -10, unit::QUANTITY, None).unwrap_err();
        match error {
            CommonError::IsInvalid(message) => {
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
            CommonError::IsInvalid(message) => {
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
            "label", unit::QUANTITY, &Tags::new()).unwrap();
        println!("{:#?}", d);
    }

    #[test]
    fn distribution_error_on_creating_with_empty_label() {
        let error = Distribution::new(
            "", unit::PERCENTAGE, &Tags::new()).unwrap_err();
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
            "\t ", unit::QUANTITY, &Tags::new()).unwrap_err();
        match error {
            CommonError::IsInvalid(message) => {
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
            CommonError::IsInvalid(message) => {
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
            "label", 33, unit::QUANTITY, None)
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
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        let m = Measurement::new(
            "memory_free", 20_000, unit::BYTES, None)
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
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        let m = Measurement::new(
            "memory_used", 20_000, unit::PERCENTAGE, None)
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
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        let t = Tags::from([
            ("service", "me"),
            ("component", "application")
        ].iter().map(|e| {
            (String::from(e.0), String::from(e.1))
        }).collect()).unwrap();
        let m = Measurement::new(
            "memory_used", 20_000, unit::BYTES, Some(&t))
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
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        let key: i128 = 20_000;
        d.values.insert(key, u128::MAX);
        let m = Measurement::new(
            "memory_used", key, unit::BYTES, None)
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
    fn distribution_error_on_merge_with_mismatched_label() {
        let mut d = Distribution::new(
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        let o = Distribution::new(
            "memory_free", unit::BYTES, &Tags::new())
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
            "memory_used", unit::BYTES, &Tags::new())
            .unwrap();
        let o = Distribution::new(
            "memory_used", unit::PERCENTAGE, &Tags::new())
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
            "memory_used", unit::BYTES, &t)
            .unwrap();
        let o = Distribution::new(
            "memory_used", unit::BYTES, &Tags::new())
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
