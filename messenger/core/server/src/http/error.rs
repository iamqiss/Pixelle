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

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use messenger_common::MessengerError;
use serde::Serialize;
use thiserror::Error;
use tracing::error;

#[derive(Debug, Error)]
pub enum CustomError {
    #[error(transparent)]
    Error(#[from] MessengerError),
    #[error("Resource not found")]
    ResourceNotFound,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub id: u32,
    pub code: String,
    pub reason: String,
    pub field: Option<String>,
}

impl IntoResponse for CustomError {
    fn into_response(self) -> Response {
        match self {
            CustomError::Error(error) => {
                error!("There was an error: {error}");
                let status_code = match error {
                    MessengerError::StreamIdNotFound(_) => StatusCode::NOT_FOUND,
                    MessengerError::TopicIdNotFound(_, _) => StatusCode::NOT_FOUND,
                    MessengerError::PartitionNotFound(_, _, _) => StatusCode::NOT_FOUND,
                    MessengerError::SegmentNotFound => StatusCode::NOT_FOUND,
                    MessengerError::ClientNotFound(_) => StatusCode::NOT_FOUND,
                    MessengerError::ConsumerGroupIdNotFound(_, _) => StatusCode::NOT_FOUND,
                    MessengerError::ConsumerGroupNameNotFound(_, _) => StatusCode::NOT_FOUND,
                    MessengerError::ConsumerGroupMemberNotFound(_, _, _) => StatusCode::NOT_FOUND,
                    MessengerError::ConsumerOffsetNotFound(_) => StatusCode::NOT_FOUND,
                    MessengerError::ResourceNotFound(_) => StatusCode::NOT_FOUND,
                    MessengerError::Unauthenticated => StatusCode::UNAUTHORIZED,
                    MessengerError::AccessTokenMissing => StatusCode::UNAUTHORIZED,
                    MessengerError::InvalidAccessToken => StatusCode::UNAUTHORIZED,
                    MessengerError::InvalidPersonalAccessToken => StatusCode::UNAUTHORIZED,
                    MessengerError::Unauthorized => StatusCode::FORBIDDEN,
                    _ => StatusCode::BAD_REQUEST,
                };
                (status_code, Json(ErrorResponse::from_error(error)))
            }
            CustomError::ResourceNotFound => (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    id: 404,
                    code: "not_found".to_string(),
                    reason: "Resource not found".to_string(),
                    field: None,
                }),
            ),
        }
        .into_response()
    }
}

impl ErrorResponse {
    pub fn from_error(error: MessengerError) -> Self {
        ErrorResponse {
            id: error.as_code(),
            code: error.as_string().to_string(),
            reason: error.to_string(),
            field: match error {
                MessengerError::StreamIdNotFound(_) => Some("stream_id".to_string()),
                MessengerError::TopicIdNotFound(_, _) => Some("topic_id".to_string()),
                MessengerError::PartitionNotFound(_, _, _) => Some("partition_id".to_string()),
                MessengerError::SegmentNotFound => Some("segment_id".to_string()),
                MessengerError::ClientNotFound(_) => Some("client_id".to_string()),
                MessengerError::InvalidStreamName => Some("name".to_string()),
                MessengerError::StreamNameAlreadyExists(_) => Some("name".to_string()),
                MessengerError::InvalidTopicName => Some("name".to_string()),
                MessengerError::TopicNameAlreadyExists(_, _) => Some("name".to_string()),
                MessengerError::InvalidStreamId => Some("stream_id".to_string()),
                MessengerError::StreamIdAlreadyExists(_) => Some("stream_id".to_string()),
                MessengerError::InvalidTopicId => Some("topic_id".to_string()),
                MessengerError::TopicIdAlreadyExists(_, _) => Some("topic_id".to_string()),
                MessengerError::InvalidOffset(_) => Some("offset".to_string()),
                MessengerError::InvalidConsumerGroupId => Some("consumer_group_id".to_string()),
                MessengerError::ConsumerGroupIdAlreadyExists(_, _) => {
                    Some("consumer_group_id".to_string())
                }
                MessengerError::ConsumerGroupNameAlreadyExists(_, _) => Some("name".to_string()),
                MessengerError::UserAlreadyExists => Some("username".to_string()),
                MessengerError::PersonalAccessTokenAlreadyExists(_, _) => Some("name".to_string()),
                _ => None,
            },
        }
    }
}
