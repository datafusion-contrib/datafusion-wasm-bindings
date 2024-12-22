// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use datafusion::arrow::util::display::FormatOptions;
use datafusion::arrow::util::pretty::pretty_format_batches_with_options;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::collect;
use datafusion::sql::parser::DFParser;
use wasm_bindgen::prelude::*;

use crate::console;
use crate::error::Result;
use crate::object_store::{OpendalRegistry, S3Config};

#[wasm_bindgen]
pub struct DataFusionContext {
    session_context: Arc<SessionContext>,
    store_registry: OpendalRegistry,
}

#[wasm_bindgen]
impl DataFusionContext {
    pub fn greet() -> String {
        "hello from datafusion-wasm".to_string()
    }

    pub fn new() -> Self {
        crate::set_panic_hook();

        // build opendal registry
        let store_registry = OpendalRegistry::new();

        let rt = Arc::new(
            RuntimeEnvBuilder::new()
                .with_disk_manager(DiskManagerConfig::Disabled)
                .with_object_store_registry(Arc::new(store_registry.clone()))
                .build()
                .unwrap(),
        );
        let session_config = SessionConfig::new()
            .with_target_partitions(1)
            .with_information_schema(true);
        let session_context = Arc::new(SessionContext::new_with_config_rt(session_config, rt));

        console::log("datafusion context is initialized");

        Self {
            session_context,
            store_registry,
        }
    }

    pub async fn execute_sql(&self, sql: String) -> Result<String> {
        self.execute_inner(sql).await
    }

    pub fn set_s3_config(
        &mut self,
        root: String,
        bucket: String,
        region: String,
        access_key_id: String,
        secret_access_key: String,
    ) {
        let s3_config = S3Config {
            root,
            bucket,
            region,
            access_key_id,
            secret_access_key,
        };
        self.store_registry.set_s3_config(s3_config);
    }
}

impl DataFusionContext {
    async fn execute_inner(&self, sql: String) -> Result<String> {
        let statements = DFParser::parse_sql(&sql)?;
        let mut results = Vec::with_capacity(statements.len());

        for statement in statements {
            let logical_plan = self
                .session_context
                .state()
                .statement_to_plan(statement)
                .await?;
            let data_frame = self
                .session_context
                .execute_logical_plan(logical_plan)
                .await?;
            let physical_plan = data_frame.create_physical_plan().await?;

            let task_ctx = self.session_context.task_ctx();
            let record_batches = collect(physical_plan, task_ctx).await?;
            let formatted =
                pretty_format_batches_with_options(&record_batches, &FormatOptions::default())?
                    .to_string();

            results.push(formatted)
        }

        Ok(format!("{}", results.join("\n")))
    }
}
