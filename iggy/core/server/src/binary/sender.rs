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

use std::future::Future;
use std::io::IoSlice;

use crate::tcp::tcp_sender::TcpSender;
use crate::tcp::tcp_tls_sender::TcpTlsSender;
use crate::{quic::quic_sender::QuicSender, server_error::ServerError};
use iggy_common::IggyError;
use quinn::{RecvStream, SendStream};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;

macro_rules! forward_async_methods {
    (
        $(
            async fn $method_name:ident(
                &mut self $(, $arg:ident : $arg_ty:ty )*
            ) -> $ret:ty ;
        )*
    ) => {
        $(
            pub async fn $method_name(&mut self, $( $arg: $arg_ty ),* ) -> $ret {
                match self {
                    Self::Tcp(d) => d.$method_name($( $arg ),*).await,
                    Self::TcpTls(s) => s.$method_name($( $arg ),*).await,
                    Self::Quic(s) => s.$method_name($( $arg ),*).await,
                }
            }
        )*
    }
}

pub trait Sender {
    fn read(&mut self, buffer: &mut [u8]) -> impl Future<Output = Result<usize, IggyError>> + Send;
    fn send_empty_ok_response(&mut self) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn send_ok_response(
        &mut self,
        payload: &[u8],
    ) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn send_ok_response_vectored(
        &mut self,
        length: &[u8],
        slices: Vec<IoSlice<'_>>,
    ) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn send_error_response(
        &mut self,
        error: IggyError,
    ) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn shutdown(&mut self) -> impl Future<Output = Result<(), ServerError>> + Send;
}

#[allow(clippy::large_enum_variant)]
pub enum SenderKind {
    Tcp(TcpSender),
    TcpTls(TcpTlsSender),
    Quic(QuicSender),
}

impl SenderKind {
    pub fn get_tcp_sender(stream: TcpStream) -> Self {
        Self::Tcp(TcpSender { stream })
    }

    pub fn get_tcp_tls_sender(stream: TlsStream<TcpStream>) -> Self {
        Self::TcpTls(TcpTlsSender { stream })
    }

    pub fn get_quic_sender(send_stream: SendStream, recv_stream: RecvStream) -> Self {
        Self::Quic(QuicSender {
            send: send_stream,
            recv: recv_stream,
        })
    }

    forward_async_methods! {
        async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, IggyError>;
        async fn send_empty_ok_response(&mut self) -> Result<(), IggyError>;
        async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), IggyError>;
        async fn send_ok_response_vectored(&mut self, length: &[u8], slices: Vec<IoSlice<'_>>) -> Result<(), IggyError>;
        async fn send_error_response(&mut self, error: IggyError) -> Result<(), IggyError>;
        async fn shutdown(&mut self) -> Result<(), ServerError>;
    }
}
