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

use crate::MessengerDuration;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct QuicClientReconnectionConfig {
    pub enabled: bool,
    pub max_retries: Option<u32>,
    pub interval: MessengerDuration,
    pub reestablish_after: MessengerDuration,
}

impl QuicClientReconnectionConfig {
    pub fn new(
        enabled: bool,
        max_retries: Option<u32>,
        interval: MessengerDuration,
        reestablish_after: MessengerDuration,
    ) -> Self {
        Self {
            enabled,
            max_retries,
            interval,
            reestablish_after,
        }
    }
}

impl Default for QuicClientReconnectionConfig {
    fn default() -> QuicClientReconnectionConfig {
        QuicClientReconnectionConfig {
            enabled: true,
            max_retries: None,
            interval: MessengerDuration::from_str("1s").unwrap(),
            reestablish_after: MessengerDuration::from_str("5s").unwrap(),
        }
    }
}
