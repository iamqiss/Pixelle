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

mod indexes;
mod messages;
mod messages_accumulator;
mod reading_messages;
mod segment;
mod types;
mod writing_messages;

pub use indexes::IggyIndexesMut;
pub use messages_accumulator::MessagesAccumulator;
pub use segment::Segment;
pub use types::IggyMessageHeaderViewMut;
pub use types::IggyMessageViewMut;
pub use types::IggyMessagesBatchMut;
pub use types::IggyMessagesBatchSet;

pub const LOG_EXTENSION: &str = "log";
pub const INDEX_EXTENSION: &str = "index";
pub const SEGMENT_MAX_SIZE_BYTES: u64 = 1024 * 1024 * 1024;
