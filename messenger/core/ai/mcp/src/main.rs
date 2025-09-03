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

use config::{Config, Environment, File};
use configs::{McpServerConfig, McpTransport};
use dotenvy::dotenv;
use error::McpRuntimeError;
use figlet_rs::FIGfont;
use messenger::prelude::{Client, Identifier};
use rmcp::{ServiceExt, model::ErrorData, transport::stdio};
use service::MessengerService;
use std::{env, sync::Arc};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt, util::SubscriberInitExt};

mod api;
mod configs;
mod error;
mod service;
mod stream;

#[tokio::main]
async fn main() -> Result<(), McpRuntimeError> {
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Messenger MCP Server");
    eprintln!("{}", figure.unwrap());

    if let Ok(env_path) = std::env::var("MESSENGER_MCP_ENV_PATH") {
        if dotenvy::from_path(&env_path).is_ok() {
            eprintln!("Loaded environment variables from path: {env_path}");
        }
    } else if let Ok(path) = dotenv() {
        eprintln!(
            "Loaded environment variables from .env file at path: {}",
            path.display()
        );
    }

    let config_path = env::var("MESSENGER_MCP_CONFIG_PATH").unwrap_or_else(|_| "config".to_string());
    eprintln!("Configuration file path: {config_path}");
    let config: McpServerConfig = Config::builder()
        .add_source(Config::try_from(&McpServerConfig::default()).expect("Failed to init config"))
        .add_source(File::with_name(&config_path).required(false))
        .add_source(Environment::with_prefix("MESSENGER_MCP").separator("_"))
        .build()
        .expect("Failed to build runtime config")
        .try_deserialize()
        .expect("Failed to deserialize runtime config");

    let transport = config.transport;
    if transport == McpTransport::Stdio {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("DEBUG")))
            .with_writer(std::io::stderr)
            .with_ansi(false)
            .init();
    } else {
        Registry::default()
            .with(tracing_subscriber::fmt::layer())
            .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
            .init();
    }

    info!("Starting Messenger MCP Server, transport: {transport}...");

    let consumer_id = Identifier::from_str_value(
        config.messenger.consumer.as_deref().unwrap_or("messenger-mcp"),
    )
    .map_err(|error| {
        error!("Failed to create Messenger consumer ID: {:?}", error);
        McpRuntimeError::FailedToCreateConsumerId
    })?;
    let messenger_consumer = Arc::new(messenger::prelude::Consumer::new(consumer_id));
    let messenger_client = Arc::new(stream::init(config.messenger).await?);
    let client_to_shutdown = messenger_client.clone();
    let permissions = Permissions {
        create: config.permissions.create,
        read: config.permissions.read,
        update: config.permissions.update,
        delete: config.permissions.delete,
    };

    if transport == McpTransport::Stdio {
        let Ok(service) = MessengerService::new(messenger_client, messenger_consumer, permissions)
            .serve(stdio())
            .await
            .inspect_err(|e| {
                error!("Serving error: {:?}", e);
            })
        else {
            error!("Failed to create service");
            return Err(McpRuntimeError::FailedToCreateService);
        };

        if let Err(error) = service.waiting().await {
            error!("waiting error: {:?}", error);
        }
    } else {
        let Some(http_config) = config.http else {
            error!("HTTP API configuration not found");
            return Err(McpRuntimeError::MissingConfig);
        };

        api::init(http_config, messenger_client, messenger_consumer, permissions).await?;
    }

    #[cfg(unix)]
    let (mut ctrl_c, mut sigterm) = {
        use tokio::signal::unix::{SignalKind, signal};
        (
            signal(SignalKind::interrupt()).expect("Failed to create SIGINT signal"),
            signal(SignalKind::terminate()).expect("Failed to create SIGTERM signal"),
        )
    };

    #[cfg(unix)]
    tokio::select! {
        _ = ctrl_c.recv() => {
            info!("Received SIGINT. Shutting down Messenger MCP Server...");
        },
        _ = sigterm.recv() => {
            info!("Received SIGTERM. Shutting down Messenger MCP Server...");
        }
    }

    client_to_shutdown.shutdown().await?;
    info!("Messenger MCP Server stopped successfully");
    Ok(())
}

#[derive(Debug, Copy, Clone)]
pub struct Permissions {
    create: bool,
    read: bool,
    update: bool,
    delete: bool,
}

impl Permissions {
    pub fn ensure_read(&self) -> Result<(), ErrorData> {
        if self.read {
            Ok(())
        } else {
            Err(ErrorData::invalid_request(
                "Insufficient 'read' permissions",
                None,
            ))
        }
    }

    pub fn ensure_create(&self) -> Result<(), ErrorData> {
        if self.create {
            Ok(())
        } else {
            Err(ErrorData::invalid_request(
                "Insufficient 'create' permissions",
                None,
            ))
        }
    }

    pub fn ensure_update(&self) -> Result<(), ErrorData> {
        if self.update {
            Ok(())
        } else {
            Err(ErrorData::invalid_request(
                "Insufficient 'update' permissions",
                None,
            ))
        }
    }

    pub fn ensure_delete(&self) -> Result<(), ErrorData> {
        if self.delete {
            Ok(())
        } else {
            Err(ErrorData::invalid_request(
                "Insufficient 'delete' permissions",
                None,
            ))
        }
    }
}
