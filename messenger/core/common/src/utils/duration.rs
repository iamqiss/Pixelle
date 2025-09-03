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

use humantime::Duration as HumanDuration;
use humantime::format_duration;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    fmt::{Display, Formatter},
    ops::Add,
    str::FromStr,
    time::Duration,
};

pub const SEC_IN_MICRO: u64 = 1_000_000;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct MessengerDuration {
    duration: Duration,
}

impl MessengerDuration {
    pub const ONE_SECOND: MessengerDuration = MessengerDuration {
        duration: Duration::from_secs(1),
    };
}

impl MessengerDuration {
    pub fn new(duration: Duration) -> MessengerDuration {
        MessengerDuration { duration }
    }

    pub fn new_from_secs(secs: u64) -> MessengerDuration {
        MessengerDuration {
            duration: Duration::from_secs(secs),
        }
    }

    pub fn as_human_time_string(&self) -> String {
        format!("{}", format_duration(self.duration))
    }

    pub fn as_secs(&self) -> u32 {
        self.duration.as_secs() as u32
    }

    pub fn as_secs_f64(&self) -> f64 {
        self.duration.as_secs_f64()
    }

    pub fn as_micros(&self) -> u64 {
        self.duration.as_micros() as u64
    }

    pub fn get_duration(&self) -> Duration {
        self.duration
    }

    pub fn is_zero(&self) -> bool {
        self.duration.as_secs() == 0
    }

    pub fn abs_diff(&self, other: MessengerDuration) -> MessengerDuration {
        let diff = self.duration.abs_diff(other.duration);
        MessengerDuration { duration: diff }
    }
}

impl FromStr for MessengerDuration {
    type Err = humantime::DurationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = &s.to_lowercase();
        if s == "0" || s == "unlimited" || s == "disabled" || s == "none" {
            Ok(MessengerDuration {
                duration: Duration::new(0, 0),
            })
        } else {
            Ok(MessengerDuration {
                duration: humantime::parse_duration(s)?,
            })
        }
    }
}

impl From<Option<u64>> for MessengerDuration {
    fn from(duration_us: Option<u64>) -> Self {
        match duration_us {
            Some(value) => MessengerDuration {
                duration: Duration::from_micros(value),
            },
            None => MessengerDuration {
                duration: Duration::new(0, 0),
            },
        }
    }
}

impl From<u64> for MessengerDuration {
    fn from(value: u64) -> Self {
        MessengerDuration {
            duration: Duration::from_micros(value),
        }
    }
}

impl From<Duration> for MessengerDuration {
    fn from(duration: Duration) -> Self {
        MessengerDuration { duration }
    }
}

impl From<HumanDuration> for MessengerDuration {
    fn from(human_duration: HumanDuration) -> Self {
        Self {
            duration: human_duration.into(),
        }
    }
}

impl From<MessengerDuration> for u64 {
    fn from(messenger_duration: MessengerDuration) -> u64 {
        messenger_duration.duration.as_micros() as u64
    }
}

impl Default for MessengerDuration {
    fn default() -> Self {
        MessengerDuration {
            duration: Duration::new(0, 0),
        }
    }
}

impl Display for MessengerDuration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_human_time_string())
    }
}

impl Add for MessengerDuration {
    type Output = MessengerDuration;

    fn add(self, rhs: Self) -> Self::Output {
        MessengerDuration {
            duration: self.duration + rhs.duration,
        }
    }
}

impl Serialize for MessengerDuration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.as_micros())
    }
}

struct MessengerDurationVisitor;

impl<'de> Deserialize<'de> for MessengerDuration {
    fn deserialize<D>(deserializer: D) -> Result<MessengerDuration, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_u64(MessengerDurationVisitor)
    }
}

impl Visitor<'_> for MessengerDurationVisitor {
    type Value = MessengerDuration;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a duration in seconds")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(MessengerDuration::new(Duration::from_micros(value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_new() {
        let duration = Duration::new(60, 0); // 60 seconds
        let messenger_duration = MessengerDuration::new(duration);
        assert_eq!(messenger_duration.as_secs(), 60);
    }

    #[test]
    fn test_as_human_time_string() {
        let duration = Duration::new(3661, 0); // 1 hour, 1 minute and 1 second
        let messenger_duration = MessengerDuration::new(duration);
        assert_eq!(messenger_duration.as_human_time_string(), "1h 1m 1s");
    }

    #[test]
    fn test_long_duration_as_human_time_string() {
        let duration = Duration::new(36611233, 0); // 1year 1month 28days 1hour 13minutes 37seconds
        let messenger_duration = MessengerDuration::new(duration);
        assert_eq!(
            messenger_duration.as_human_time_string(),
            "1year 1month 28days 1h 13m 37s"
        );
    }

    #[test]
    fn test_from_str() {
        let messenger_duration: MessengerDuration = "1h 1m 1s".parse().unwrap();
        assert_eq!(messenger_duration.as_secs(), 3661);
    }

    #[test]
    fn test_display() {
        let duration = Duration::new(3661, 0);
        let messenger_duration = MessengerDuration::new(duration);
        let duration_string = format!("{messenger_duration}");
        assert_eq!(duration_string, "1h 1m 1s");
    }

    #[test]
    fn test_invalid_duration() {
        let result: Result<MessengerDuration, _> = "1 hour and 30 minutes".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_zero_seconds_duration() {
        let messenger_duration: MessengerDuration = "0s".parse().unwrap();
        assert_eq!(messenger_duration.as_secs(), 0);
    }

    #[test]
    fn test_zero_duration() {
        let messenger_duration: MessengerDuration = "0".parse().unwrap();
        assert_eq!(messenger_duration.as_secs(), 0);
    }

    #[test]
    fn test_unlimited() {
        let messenger_duration: MessengerDuration = "unlimited".parse().unwrap();
        assert_eq!(messenger_duration.as_secs(), 0);
    }

    #[test]
    fn test_disabled() {
        let messenger_duration: MessengerDuration = "disabled".parse().unwrap();
        assert_eq!(messenger_duration.as_secs(), 0);
    }

    #[test]
    fn test_add_duration() {
        let messenger_duration1: MessengerDuration = "6s".parse().unwrap();
        let messenger_duration2: MessengerDuration = "1m".parse().unwrap();
        let result: MessengerDuration = messenger_duration1 + messenger_duration2;
        assert_eq!(result.as_secs(), 66);
    }
}
