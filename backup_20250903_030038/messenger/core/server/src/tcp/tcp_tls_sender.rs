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

use crate::binary::sender::Sender;
use crate::tcp::COMPONENT;
use crate::{server_error::ServerError, tcp::sender};
use error_set::ErrContext;
use iggy_common::IggyError;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;

#[derive(Debug)]
pub struct TcpTlsSender {
    pub(crate) stream: TlsStream<TcpStream>,
}

impl Sender for TcpTlsSender {
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, IggyError> {
        sender::read(&mut self.stream, buffer).await
    }

    async fn send_empty_ok_response(&mut self) -> Result<(), IggyError> {
        sender::send_empty_ok_response(&mut self.stream).await
    }

    async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), IggyError> {
        sender::send_ok_response(&mut self.stream, payload).await
    }

    async fn send_error_response(&mut self, error: IggyError) -> Result<(), IggyError> {
        sender::send_error_response(&mut self.stream, error).await
    }

    async fn shutdown(&mut self) -> Result<(), ServerError> {
        self.stream
            .shutdown()
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to shutdown TCP TLS stream")
            })
            .map_err(ServerError::IoError)
    }

    async fn send_ok_response_vectored(
        &mut self,
        length: &[u8],
        slices: Vec<std::io::IoSlice<'_>>,
    ) -> Result<(), IggyError> {
        sender::send_ok_response_vectored(&mut self.stream, length, slices).await
    }
}
