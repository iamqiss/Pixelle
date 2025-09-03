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

use crate::streaming::persistence::COMPONENT;
use crate::streaming::utils::file;
use error_set::ErrContext;
use iggy_common::IggyError;
use std::fmt::Debug;
use std::future::Future;
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[cfg(test)]
use mockall::automock;

#[derive(Debug)]
pub enum PersisterKind {
    File(FilePersister),
    FileWithSync(FileWithSyncPersister),
    #[cfg(test)]
    Mock(MockPersister),
}

impl PersisterKind {
    pub async fn append(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError> {
        match self {
            PersisterKind::File(p) => p.append(path, bytes).await,
            PersisterKind::FileWithSync(p) => p.append(path, bytes).await,
            #[cfg(test)]
            PersisterKind::Mock(p) => p.append(path, bytes).await,
        }
    }

    pub async fn overwrite(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError> {
        match self {
            PersisterKind::File(p) => p.overwrite(path, bytes).await,
            PersisterKind::FileWithSync(p) => p.overwrite(path, bytes).await,
            #[cfg(test)]
            PersisterKind::Mock(p) => p.overwrite(path, bytes).await,
        }
    }

    pub async fn delete(&self, path: &str) -> Result<(), IggyError> {
        match self {
            PersisterKind::File(p) => p.delete(path).await,
            PersisterKind::FileWithSync(p) => p.delete(path).await,
            #[cfg(test)]
            PersisterKind::Mock(p) => p.delete(path).await,
        }
    }
}

#[cfg_attr(test, automock)]
pub trait Persister: Send {
    fn append(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn overwrite(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> impl Future<Output = Result<(), IggyError>> + Send;
    fn delete(&self, path: &str) -> impl Future<Output = Result<(), IggyError>> + Send;
}

#[derive(Debug)]
pub struct FilePersister;

#[derive(Debug)]
pub struct FileWithSyncPersister;

impl Persister for FilePersister {
    async fn append(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError> {
        let mut file = file::append(path)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to append to file: {path}")
            })
            .map_err(|_| IggyError::CannotAppendToFile)?;
        file.write_all(bytes)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to write data to file: {path}")
            })
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }

    async fn overwrite(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError> {
        let mut file = file::overwrite(path)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to overwrite file: {path}")
            })
            .map_err(|_| IggyError::CannotOverwriteFile)?;
        file.write_all(bytes)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to write data to file: {path}")
            })
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), IggyError> {
        fs::remove_file(path)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to delete file: {path}")
            })
            .map_err(|_| IggyError::CannotDeleteFile)?;
        Ok(())
    }
}

impl Persister for FileWithSyncPersister {
    async fn append(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError> {
        let mut file = file::append(path)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to append to file: {path}")
            })
            .map_err(|_| IggyError::CannotAppendToFile)?;
        file.write_all(bytes)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to write data to file: {path}")
            })
            .map_err(|_| IggyError::CannotWriteToFile)?;
        file.sync_all()
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to sync file after appending: {path}"
                )
            })
            .map_err(|_| IggyError::CannotSyncFile)?;
        Ok(())
    }

    async fn overwrite(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError> {
        let mut file = file::overwrite(path)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to overwrite file: {path}")
            })
            .map_err(|_| IggyError::CannotOverwriteFile)?;
        file.write_all(bytes)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to write data to file: {path}")
            })
            .map_err(|_| IggyError::CannotWriteToFile)?;
        file.sync_all()
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to sync file after overwriting: {path}"
                )
            })
            .map_err(|_| IggyError::CannotSyncFile)?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), IggyError> {
        fs::remove_file(path)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to delete file: {path}")
            })
            .map_err(|_| IggyError::CannotDeleteFile)?;
        Ok(())
    }
}
