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

use crate::{BytesSerializable, Sizeable, error::MessengerError, utils::byte_size::MessengerByteSize};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::ops::Range;

pub const MESSENGER_MESSAGE_HEADER_SIZE: usize = 8 + 16 + 8 + 8 + 8 + 4 + 4;
pub const MESSENGER_MESSAGE_HEADER_RANGE: Range<usize> = 0..MESSENGER_MESSAGE_HEADER_SIZE;

pub const MESSENGER_MESSAGE_CHECKSUM_OFFSET_RANGE: Range<usize> = 0..8;
pub const MESSENGER_MESSAGE_ID_OFFSET_RANGE: Range<usize> = 8..24;
pub const MESSENGER_MESSAGE_OFFSET_OFFSET_RANGE: Range<usize> = 24..32;
pub const MESSENGER_MESSAGE_TIMESTAMP_OFFSET_RANGE: Range<usize> = 32..40;
pub const MESSENGER_MESSAGE_ORIGIN_TIMESTAMP_OFFSET_RANGE: Range<usize> = 40..48;
pub const MESSENGER_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE: Range<usize> = 48..52;
pub const MESSENGER_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE: Range<usize> = 52..56;

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct MessengerMessageHeader {
    pub checksum: u64,
    pub id: u128,
    pub offset: u64,
    pub timestamp: u64,
    pub origin_timestamp: u64,
    pub user_headers_length: u32,
    pub payload_length: u32,
}

impl Sizeable for MessengerMessageHeader {
    fn get_size_bytes(&self) -> MessengerByteSize {
        MessengerByteSize::from(MESSENGER_MESSAGE_HEADER_SIZE as u64)
    }
}

impl MessengerMessageHeader {
    pub fn from_raw_bytes(bytes: &[u8]) -> Result<Self, MessengerError> {
        if bytes.len() != MESSENGER_MESSAGE_HEADER_SIZE {
            return Err(MessengerError::InvalidCommand);
        }

        Ok(MessengerMessageHeader {
            checksum: u64::from_le_bytes(
                bytes[MESSENGER_MESSAGE_CHECKSUM_OFFSET_RANGE]
                    .try_into()
                    .map_err(|_| MessengerError::InvalidNumberEncoding)?,
            ),
            id: u128::from_le_bytes(
                bytes[MESSENGER_MESSAGE_ID_OFFSET_RANGE]
                    .try_into()
                    .map_err(|_| MessengerError::InvalidNumberEncoding)?,
            ),
            offset: u64::from_le_bytes(
                bytes[MESSENGER_MESSAGE_OFFSET_OFFSET_RANGE]
                    .try_into()
                    .map_err(|_| MessengerError::InvalidNumberEncoding)?,
            ),
            timestamp: u64::from_le_bytes(
                bytes[MESSENGER_MESSAGE_TIMESTAMP_OFFSET_RANGE]
                    .try_into()
                    .map_err(|_| MessengerError::InvalidNumberEncoding)?,
            ),
            origin_timestamp: u64::from_le_bytes(
                bytes[MESSENGER_MESSAGE_ORIGIN_TIMESTAMP_OFFSET_RANGE]
                    .try_into()
                    .map_err(|_| MessengerError::InvalidNumberEncoding)?,
            ),
            user_headers_length: u32::from_le_bytes(
                bytes[MESSENGER_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE]
                    .try_into()
                    .map_err(|_| MessengerError::InvalidNumberEncoding)?,
            ),
            payload_length: u32::from_le_bytes(
                bytes[MESSENGER_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE]
                    .try_into()
                    .map_err(|_| MessengerError::InvalidNumberEncoding)?,
            ),
        })
    }
}

impl BytesSerializable for MessengerMessageHeader {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(self.get_size_bytes().as_bytes_usize());
        bytes.put_u64_le(self.checksum);
        bytes.put_u128_le(self.id);
        bytes.put_u64_le(self.offset);
        bytes.put_u64_le(self.timestamp);
        bytes.put_u64_le(self.origin_timestamp);
        bytes.put_u32_le(self.user_headers_length);
        bytes.put_u32_le(self.payload_length);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, MessengerError> {
        if bytes.len() != MESSENGER_MESSAGE_HEADER_SIZE {
            return Err(MessengerError::InvalidCommand);
        }

        let checksum = u64::from_le_bytes(
            bytes[MESSENGER_MESSAGE_CHECKSUM_OFFSET_RANGE]
                .try_into()
                .map_err(|_| MessengerError::InvalidNumberEncoding)?,
        );

        let id = u128::from_le_bytes(
            bytes[MESSENGER_MESSAGE_ID_OFFSET_RANGE]
                .try_into()
                .map_err(|_| MessengerError::InvalidNumberEncoding)?,
        );

        let offset = u64::from_le_bytes(
            bytes[MESSENGER_MESSAGE_OFFSET_OFFSET_RANGE]
                .try_into()
                .map_err(|_| MessengerError::InvalidNumberEncoding)?,
        );

        let timestamp = u64::from_le_bytes(
            bytes[MESSENGER_MESSAGE_TIMESTAMP_OFFSET_RANGE]
                .try_into()
                .map_err(|_| MessengerError::InvalidNumberEncoding)?,
        );

        let origin_timestamp = u64::from_le_bytes(
            bytes[MESSENGER_MESSAGE_ORIGIN_TIMESTAMP_OFFSET_RANGE]
                .try_into()
                .map_err(|_| MessengerError::InvalidNumberEncoding)?,
        );

        let headers_length = u32::from_le_bytes(
            bytes[MESSENGER_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE]
                .try_into()
                .map_err(|_| MessengerError::InvalidNumberEncoding)?,
        );

        let payload_length = u32::from_le_bytes(
            bytes[MESSENGER_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE]
                .try_into()
                .map_err(|_| MessengerError::InvalidNumberEncoding)?,
        );

        Ok(MessengerMessageHeader {
            checksum,
            id,
            offset,
            timestamp,
            origin_timestamp,
            user_headers_length: headers_length,
            payload_length,
        })
    }
}
