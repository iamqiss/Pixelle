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

use std::path::PathBuf;

use messenger_binary_protocol::cli::binary_context::common::{ContextReaderWriter, ContextsConfigMap};
use tempfile::{TempDir, tempdir};

pub struct TestMessengerContext {
    maybe_contexts: Option<ContextsConfigMap>,
    maybe_active_context_key: Option<String>,
    messenger_home: TempDir,
    context_manager: ContextReaderWriter,
}

impl TestMessengerContext {
    pub fn new(
        maybe_contexts: Option<ContextsConfigMap>,
        maybe_active_context_key: Option<String>,
    ) -> Self {
        let messenger_home = tempdir().unwrap();
        let context_manager = ContextReaderWriter::new(Some(messenger_home.path().to_path_buf()));

        Self {
            messenger_home,
            context_manager,
            maybe_contexts,
            maybe_active_context_key,
        }
    }

    pub async fn prepare(&self) {
        if let Some(contexts) = &self.maybe_contexts {
            self.context_manager
                .write_contexts(contexts.clone())
                .await
                .unwrap();
        }

        if let Some(active_context_key) = &self.maybe_active_context_key {
            self.context_manager
                .write_active_context(active_context_key)
                .await
                .unwrap();
        }
    }

    pub async fn read_saved_context_key(&self) -> Option<String> {
        self.context_manager.read_active_context().await.unwrap()
    }

    pub fn get_contexts(&self) -> Option<ContextsConfigMap> {
        self.maybe_contexts.clone()
    }

    pub fn get_active_context_key(&self) -> Option<String> {
        self.maybe_active_context_key.clone()
    }

    pub fn get_messenger_home(&self) -> PathBuf {
        self.messenger_home.path().to_path_buf()
    }
}
