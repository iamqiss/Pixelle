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

use super::PersisterTask;
use crate::streaming::segments::{IggyMessagesBatchSet, messages::write_batch};
use error_set::ErrContext;
use iggy_common::{Confirmation, IggyByteSize, IggyError};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::fs::{File, OpenOptions};
use tracing::{error, trace};

/// A dedicated struct for writing to the messages file.
#[derive(Debug)]
pub struct MessagesWriter {
    file_path: String,
    /// Holds the file for synchronous writes; when asynchronous persistence is enabled, this will be None.
    file: Option<File>,
    /// When set, asynchronous writes are handled by this persister task.
    persister_task: Option<PersisterTask>,
    messages_size_bytes: Arc<AtomicU64>,
    fsync: bool,
}

impl MessagesWriter {
    /// Opens the messages file in write mode.
    ///
    /// If the server confirmation is set to `NoWait`, the file handle is transferred to the
    /// persister task (and stored in `persister_task`) so that writes are done asynchronously.
    /// Otherwise, the file is retained in `self.file` for synchronous writes.
    pub async fn new(
        file_path: &str,
        messages_size_bytes: Arc<AtomicU64>,
        fsync: bool,
        server_confirmation: Confirmation,
        file_exists: bool,
    ) -> Result<Self, IggyError> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(file_path)
            .await
            .map_err(|_| IggyError::CannotReadFile)?;

        if file_exists {
            let _ = file.sync_all().await.with_error_context(|error| {
                format!("Failed to fsync messages file after creation: {file_path}, error: {error}")
            });

            let actual_messages_size = file
                .metadata()
                .await
                .with_error_context(|error| {
                    format!("Failed to get metadata of messages file: {file_path}, error: {error}")
                })
                .map_err(|_| IggyError::CannotReadFileMetadata)?
                .len();

            messages_size_bytes.store(actual_messages_size, Ordering::Release);
        }

        trace!(
            "Opened messages file for writing: {file_path}, size: {}",
            messages_size_bytes.load(Ordering::Acquire)
        );

        let (file, persister_task) = match server_confirmation {
            Confirmation::NoWait => {
                let persister = PersisterTask::new(
                    file,
                    file_path.to_string(),
                    fsync,
                    messages_size_bytes.clone(),
                );
                (None, Some(persister))
            }
            Confirmation::Wait => (Some(file), None),
        };

        Ok(Self {
            file_path: file_path.to_string(),
            file,
            persister_task,
            messages_size_bytes,
            fsync,
        })
    }

    /// Append a batch of messages to the messages file.
    pub async fn save_batch_set(
        &mut self,
        batch_set: IggyMessagesBatchSet,
        confirmation: Confirmation,
    ) -> Result<IggyByteSize, IggyError> {
        let messages_size = batch_set.size();
        let messages_count = batch_set.count();
        let containers_count = batch_set.containers_count();
        trace!(
            "Saving batch set of size {messages_size} bytes ({containers_count} containers, {messages_count} messages) to messages file: {}",
            self.file_path
        );
        match confirmation {
            Confirmation::Wait => {
                if let Some(ref mut file) = self.file {
                    write_batch(file, &self.file_path, batch_set)
                        .await
                        .with_error_context(|error| {
                            format!(
                                "Failed to write batch to messages file: {}. {error}",
                                self.file_path
                            )
                        })
                        .map_err(|_| IggyError::CannotWriteToFile)?;
                } else {
                    error!("File handle is not available for synchronous write.");
                    return Err(IggyError::CannotWriteToFile);
                }

                if self.fsync {
                    let _ = self.fsync().await;
                }

                self.messages_size_bytes
                    .fetch_add(messages_size as u64, Ordering::Release);
                trace!(
                    "Written batch set of size {messages_size} bytes ({containers_count} containers, {messages_count} messages) to disk messages file: {}",
                    self.file_path
                );
            }
            Confirmation::NoWait => {
                if let Some(task) = &self.persister_task {
                    task.persist(batch_set).await;
                } else {
                    panic!(
                        "Confirmation::NoWait is used, but MessagesPersisterTask is not set for messages file: {}",
                        self.file_path
                    );
                }
            }
        }

        Ok(IggyByteSize::from(messages_size as u64))
    }

    pub async fn fsync(&self) -> Result<(), IggyError> {
        if let Some(file) = self.file.as_ref() {
            file.sync_all()
                .await
                .with_error_context(|error| {
                    format!("Failed to fsync messages file: {}. {error}", self.file_path)
                })
                .map_err(|_| IggyError::CannotWriteToFile)?;
        }

        Ok(())
    }

    pub async fn shutdown_persister_task(self) {
        if let Some(task) = self.persister_task {
            task.shutdown().await;
        }
    }
}
