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

use chrono::{DateTime, Local, Utc};
use core::fmt;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, Visitor},
};
use std::{
    ops::{Add, Sub},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::MessengerDuration;

/// A struct that represents a timestamp.
///
/// This struct uses `SystemTime` from `std::time` crate.
///
/// # Example
///
/// ```
/// use messenger_common::MessengerTimestamp;
///
/// let timestamp = MessengerTimestamp::from(1694968446131680);
/// assert_eq!(timestamp.to_utc_string("%Y-%m-%d %H:%M:%S"), "2023-09-17 16:34:06");
/// assert_eq!(timestamp.as_micros(), 1694968446131680);
/// ```
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct MessengerTimestamp(SystemTime);

pub const UTC_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

impl MessengerTimestamp {
    pub fn now() -> Self {
        MessengerTimestamp::default()
    }

    pub fn zero() -> Self {
        MessengerTimestamp(UNIX_EPOCH)
    }

    pub fn to_secs(&self) -> u64 {
        self.0.duration_since(UNIX_EPOCH).unwrap().as_secs()
    }

    pub fn as_micros(&self) -> u64 {
        self.0.duration_since(UNIX_EPOCH).unwrap().as_micros() as u64
    }

    pub fn to_rfc3339_string(&self) -> String {
        DateTime::<Utc>::from(self.0).to_rfc3339()
    }

    pub fn to_utc_string(&self, format: &str) -> String {
        DateTime::<Utc>::from(self.0).format(format).to_string()
    }

    pub fn to_local_string(&self, format: &str) -> String {
        DateTime::<Local>::from(self.0).format(format).to_string()
    }
}

impl From<u64> for MessengerTimestamp {
    fn from(timestamp: u64) -> Self {
        MessengerTimestamp(UNIX_EPOCH + Duration::from_micros(timestamp))
    }
}

impl From<MessengerTimestamp> for u64 {
    fn from(timestamp: MessengerTimestamp) -> u64 {
        timestamp.as_micros()
    }
}

impl From<SystemTime> for MessengerTimestamp {
    fn from(timestamp: SystemTime) -> Self {
        MessengerTimestamp(timestamp)
    }
}

impl Add<SystemTime> for MessengerTimestamp {
    type Output = MessengerTimestamp;

    fn add(self, other: SystemTime) -> MessengerTimestamp {
        MessengerTimestamp(self.0 + other.duration_since(UNIX_EPOCH).unwrap())
    }
}

impl Sub<SystemTime> for MessengerTimestamp {
    type Output = MessengerTimestamp;

    fn sub(self, rhs: SystemTime) -> Self::Output {
        MessengerTimestamp(self.0 - rhs.duration_since(UNIX_EPOCH).unwrap())
    }
}

impl Add<MessengerDuration> for MessengerTimestamp {
    type Output = MessengerTimestamp;

    fn add(self, other: MessengerDuration) -> Self::Output {
        MessengerTimestamp(self.0 + other.get_duration())
    }
}

impl Sub for MessengerTimestamp {
    type Output = Duration;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0
            .duration_since(rhs.0)
            .expect("Failed to subtract timestamps rhs < self")
    }
}

impl Default for MessengerTimestamp {
    fn default() -> Self {
        Self(SystemTime::now())
    }
}

impl fmt::Display for MessengerTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_utc_string(UTC_TIME_FORMAT))
    }
}

impl Serialize for MessengerTimestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let timestamp = self.as_micros();
        serializer.serialize_u64(timestamp)
    }
}

impl<'de> Deserialize<'de> for MessengerTimestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_u64(MessengerTimestampVisitor)
    }
}
struct MessengerTimestampVisitor;

impl Visitor<'_> for MessengerTimestampVisitor {
    type Value = MessengerTimestamp;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a microsecond timestamp as a u64")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(MessengerTimestamp::from(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_get() {
        let timestamp = MessengerTimestamp::now();
        assert!(timestamp.as_micros() > 0);
    }

    #[test]
    fn test_timestamp_to_micros() {
        let timestamp = MessengerTimestamp::from(1663472051111);
        assert_eq!(timestamp.as_micros(), 1663472051111);
    }

    #[test]
    fn test_timestamp_to_string() {
        let timestamp = MessengerTimestamp::from(1694968446131680);
        assert_eq!(
            timestamp.to_utc_string("%Y-%m-%d %H:%M:%S"),
            "2023-09-17 16:34:06"
        );
    }

    #[test]
    fn test_timestamp_from_u64() {
        let timestamp = MessengerTimestamp::from(1663472051111);
        assert_eq!(timestamp.as_micros(), 1663472051111);
    }
}
