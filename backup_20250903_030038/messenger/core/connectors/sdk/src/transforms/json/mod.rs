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

use chrono::Utc;
use simd_json::OwnedValue;

use super::ComputedValue;

// JSON-specific implementations for transforms
pub mod add_fields;
pub mod delete_fields;
pub mod filter_fields;
pub mod update_fields;

/// Computes a JSON value based on the specified computed value type
pub fn compute_value(kind: &ComputedValue) -> OwnedValue {
    let now = Utc::now();
    match kind {
        ComputedValue::DateTime => now.to_rfc3339().into(),
        ComputedValue::TimestampNanos => now.timestamp_nanos_opt().unwrap().into(),
        ComputedValue::TimestampMicros => now.timestamp_micros().into(),
        ComputedValue::TimestampMillis => now.timestamp_millis().into(),
        ComputedValue::TimestampSeconds => now.timestamp().into(),
        ComputedValue::UuidV4 => uuid::Uuid::new_v4().to_string().into(),
        ComputedValue::UuidV7 => uuid::Uuid::now_v7().to_string().into(),
    }
}

#[cfg(test)]
pub mod test_utils;
