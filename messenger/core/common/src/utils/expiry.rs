/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::utils::duration::MessengerDuration;
use humantime::Duration as HumanDuration;
use humantime::format_duration;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::fmt;
use std::fmt::Display;
use std::iter::Sum;
use std::ops::Add;
use std::str::FromStr;
use std::time::Duration;

/// Helper enum for various time-based expiry related functionalities
#[derive(Debug, Copy, Default, Clone, Eq, PartialEq)]
pub enum MessengerExpiry {
    #[default]
    /// Use the default expiry time from the server
    ServerDefault,
    /// Set expiry time to given value
    ExpireDuration(MessengerDuration),
    /// Never expire
    NeverExpire,
}

impl MessengerExpiry {
    pub fn new(values: Option<Vec<MessengerExpiry>>) -> Option<Self> {
        values.map(|items| items.iter().cloned().sum())
    }
}

impl From<&MessengerExpiry> for Option<u64> {
    fn from(value: &MessengerExpiry) -> Self {
        match value {
            MessengerExpiry::ExpireDuration(value) => Some(value.as_micros()),
            MessengerExpiry::NeverExpire => Some(u64::MAX),
            MessengerExpiry::ServerDefault => None,
        }
    }
}

impl Display for MessengerExpiry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NeverExpire => write!(f, "never_expire"),
            Self::ServerDefault => write!(f, "server_default"),
            Self::ExpireDuration(value) => write!(f, "{value}"),
        }
    }
}

impl Sum for MessengerExpiry {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.into_iter()
            .fold(MessengerExpiry::NeverExpire, |acc, x| acc + x)
    }
}

impl Add for MessengerExpiry {
    type Output = MessengerExpiry;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (MessengerExpiry::NeverExpire, MessengerExpiry::NeverExpire) => MessengerExpiry::NeverExpire,
            (MessengerExpiry::NeverExpire, expiry) => expiry,
            (expiry, MessengerExpiry::NeverExpire) => expiry,
            (
                MessengerExpiry::ExpireDuration(lhs_duration),
                MessengerExpiry::ExpireDuration(rhs_duration),
            ) => MessengerExpiry::ExpireDuration(lhs_duration + rhs_duration),
            (MessengerExpiry::ServerDefault, MessengerExpiry::ExpireDuration(_)) => MessengerExpiry::ServerDefault,
            (MessengerExpiry::ServerDefault, MessengerExpiry::ServerDefault) => MessengerExpiry::ServerDefault,
            (MessengerExpiry::ExpireDuration(_), MessengerExpiry::ServerDefault) => MessengerExpiry::ServerDefault,
        }
    }
}

impl FromStr for MessengerExpiry {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let result = match s {
            "unlimited" | "none" | "None" | "Unlimited" | "never_expire" => MessengerExpiry::NeverExpire,
            "default" | "server_default" | "Default" | "Server_default" => {
                MessengerExpiry::ServerDefault
            }
            value => {
                let duration = value.parse::<HumanDuration>().map_err(|e| format!("{e}"))?;
                if duration.as_secs() > u32::MAX as u64 {
                    return Err(format!(
                        "Value too big for expiry time, maximum value is {}",
                        format_duration(Duration::from_secs(u32::MAX as u64))
                    ));
                }

                MessengerExpiry::ExpireDuration(MessengerDuration::from(duration))
            }
        };

        Ok(result)
    }
}

impl From<MessengerExpiry> for Option<u64> {
    fn from(val: MessengerExpiry) -> Self {
        match val {
            MessengerExpiry::ExpireDuration(value) => Some(value.as_micros()),
            MessengerExpiry::ServerDefault => None,
            MessengerExpiry::NeverExpire => Some(u64::MAX),
        }
    }
}

impl From<MessengerExpiry> for u64 {
    fn from(val: MessengerExpiry) -> Self {
        match val {
            MessengerExpiry::ExpireDuration(value) => value.as_micros(),
            MessengerExpiry::ServerDefault => 0,
            MessengerExpiry::NeverExpire => u64::MAX,
        }
    }
}

impl From<Vec<MessengerExpiry>> for MessengerExpiry {
    fn from(values: Vec<MessengerExpiry>) -> Self {
        let mut result = MessengerExpiry::NeverExpire;
        for value in values {
            result = result + value;
        }
        result
    }
}

impl From<u64> for MessengerExpiry {
    fn from(value: u64) -> Self {
        match value {
            u64::MAX => MessengerExpiry::NeverExpire,
            0 => MessengerExpiry::ServerDefault,
            value => MessengerExpiry::ExpireDuration(MessengerDuration::from(value)),
        }
    }
}

impl From<Option<u64>> for MessengerExpiry {
    fn from(value: Option<u64>) -> Self {
        match value {
            Some(value) => match value {
                u64::MAX => MessengerExpiry::NeverExpire,
                0 => MessengerExpiry::ServerDefault,
                value => MessengerExpiry::ExpireDuration(MessengerDuration::from(value)),
            },
            None => MessengerExpiry::NeverExpire,
        }
    }
}

impl Serialize for MessengerExpiry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let expiry = match self {
            MessengerExpiry::ExpireDuration(value) => value.as_micros(),
            MessengerExpiry::ServerDefault => 0,
            MessengerExpiry::NeverExpire => u64::MAX,
        };
        serializer.serialize_u64(expiry)
    }
}

impl<'de> Deserialize<'de> for MessengerExpiry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_u64(MessengerExpiryVisitor)
    }
}

struct MessengerExpiryVisitor;

impl Visitor<'_> for MessengerExpiryVisitor {
    type Value = MessengerExpiry;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a microsecond expiry as a u64")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(MessengerExpiry::from(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::duration::SEC_IN_MICRO;

    #[test]
    fn should_parse_expiry() {
        assert_eq!(
            MessengerExpiry::from_str("none").unwrap(),
            MessengerExpiry::NeverExpire
        );
        assert_eq!(
            MessengerExpiry::from_str("15days").unwrap(),
            MessengerExpiry::ExpireDuration(MessengerDuration::from(SEC_IN_MICRO * 60 * 60 * 24 * 15))
        );
        assert_eq!(
            MessengerExpiry::from_str("2min").unwrap(),
            MessengerExpiry::ExpireDuration(MessengerDuration::from(SEC_IN_MICRO * 60 * 2))
        );
        assert_eq!(
            MessengerExpiry::from_str("1ms").unwrap(),
            MessengerExpiry::ExpireDuration(MessengerDuration::from(1000))
        );
        assert_eq!(
            MessengerExpiry::from_str("1s").unwrap(),
            MessengerExpiry::ExpireDuration(MessengerDuration::ONE_SECOND)
        );
        assert_eq!(
            MessengerExpiry::from_str("15days 2min 2s").unwrap(),
            MessengerExpiry::ExpireDuration(MessengerDuration::from(
                SEC_IN_MICRO * (60 * 60 * 24 * 15 + 60 * 2 + 2)
            ))
        );
    }

    #[test]
    fn should_fail_parsing_expiry() {
        let x = MessengerExpiry::from_str("15se");
        assert!(x.is_err());
        assert_eq!(
            x.unwrap_err(),
            "unknown time unit \"se\", supported units: ns, us, ms, sec, min, hours, days, weeks, months, years (and few variations)"
        );
    }

    #[test]
    fn should_sum_expiry() {
        assert_eq!(
            MessengerExpiry::NeverExpire + MessengerExpiry::NeverExpire,
            MessengerExpiry::NeverExpire
        );
        assert_eq!(
            MessengerExpiry::NeverExpire + MessengerExpiry::ExpireDuration(MessengerDuration::from(3)),
            MessengerExpiry::ExpireDuration(MessengerDuration::from(3))
        );
        assert_eq!(
            MessengerExpiry::ExpireDuration(MessengerDuration::from(5)) + MessengerExpiry::NeverExpire,
            MessengerExpiry::ExpireDuration(MessengerDuration::from(5))
        );
        assert_eq!(
            MessengerExpiry::ExpireDuration(MessengerDuration::from(5))
                + MessengerExpiry::ExpireDuration(MessengerDuration::from(3)),
            MessengerExpiry::ExpireDuration(MessengerDuration::from(8))
        );
    }

    #[test]
    fn should_sum_expiry_from_vec() {
        assert_eq!(
            vec![MessengerExpiry::NeverExpire]
                .into_iter()
                .sum::<MessengerExpiry>(),
            MessengerExpiry::NeverExpire
        );
        let x = vec![
            MessengerExpiry::NeverExpire,
            MessengerExpiry::ExpireDuration(MessengerDuration::from(333)),
            MessengerExpiry::NeverExpire,
            MessengerExpiry::ExpireDuration(MessengerDuration::from(123)),
        ];
        assert_eq!(
            x.into_iter().sum::<MessengerExpiry>(),
            MessengerExpiry::ExpireDuration(MessengerDuration::from(456))
        );
    }

    #[test]
    fn should_check_display_expiry() {
        assert_eq!(MessengerExpiry::NeverExpire.to_string(), "never_expire");
        assert_eq!(
            MessengerExpiry::ExpireDuration(MessengerDuration::from(333333000000)).to_string(),
            "3days 20h 35m 33s"
        );
    }

    #[test]
    fn should_calculate_none_from_server_default() {
        let expiry = MessengerExpiry::ServerDefault;
        let result: Option<u64> = From::from(&expiry);
        assert_eq!(result, None);
    }

    #[test]
    fn should_calculate_u64_max_from_never_expiry() {
        let expiry = MessengerExpiry::NeverExpire;
        let result: Option<u64> = From::from(&expiry);
        assert_eq!(result, Some(u64::MAX));
    }

    #[test]
    fn should_calculate_some_seconds_from_message_expire() {
        let duration = MessengerDuration::new(Duration::new(42, 0));
        let expiry = MessengerExpiry::ExpireDuration(duration);
        let result: Option<u64> = From::from(&expiry);
        assert_eq!(result, Some(42000000));
    }

    #[test]
    fn should_create_new_expiry_from_vec() {
        let some_values = vec![
            MessengerExpiry::NeverExpire,
            MessengerExpiry::ExpireDuration(MessengerDuration::from(3)),
            MessengerExpiry::ExpireDuration(MessengerDuration::from(2)),
            MessengerExpiry::ExpireDuration(MessengerDuration::from(1)),
        ];
        assert_eq!(
            MessengerExpiry::new(Some(some_values)),
            Some(MessengerExpiry::ExpireDuration(MessengerDuration::from(6)))
        );
        assert_eq!(MessengerExpiry::new(None), None);
        let none_values = vec![MessengerExpiry::ServerDefault; 10];

        assert_eq!(
            MessengerExpiry::new(Some(none_values)),
            Some(MessengerExpiry::ServerDefault)
        );
    }
}
