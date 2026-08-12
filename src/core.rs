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
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::collect;
use datafusion::sql::parser::DFParser;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStoreExt, PutPayload};
use url::Url;
use wasm_bindgen::prelude::*;

use crate::console;
use crate::error::Result;
use crate::ResultFormat;

/// URL under which the in-memory object store is registered.
const MEMORY_STORE_URL: &str = "memory:///";

#[wasm_bindgen]
pub struct DataFusionContext {
    session_context: Arc<SessionContext>,
    memory_store: Arc<InMemory>,
    result_format: ResultFormat,
}

#[wasm_bindgen]
impl DataFusionContext {
    pub fn greet() -> String {
        "hello from datafusion-wasm".to_string()
    }

    pub fn new() -> Self {
        crate::set_panic_hook();

        let rt_builder = RuntimeEnvBuilder::new().with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        );

        let rt = Arc::new(rt_builder.build().unwrap());
        let session_config = SessionConfig::new()
            .with_target_partitions(1)
            .with_information_schema(true);
        let session_context = Arc::new(SessionContext::new_with_config_rt(session_config, rt));

        let memory_store = Arc::new(InMemory::new());
        session_context
            .register_object_store(&Url::parse(MEMORY_STORE_URL).unwrap(), memory_store.clone());

        console::log("datafusion context is initialized");

        Self {
            session_context,
            memory_store,
            result_format: ResultFormat::Table,
        }
    }

    /// Insert `bytes` at `path` into the in-memory object store.
    pub async fn put_bytes(&self, path: String, bytes: Vec<u8>) -> Result<()> {
        self.memory_store
            .put(&ObjectPath::from(path), PutPayload::from(bytes))
            .await
            .map_err(|e| crate::error::WasmError::Other(format!("memory store put failed: {e}")))?;
        Ok(())
    }

    pub async fn execute_sql(&self, sql: String) -> Result<String> {
        self.execute_inner(sql).await
    }

    pub fn set_result_format(&mut self, result_format: ResultFormat) {
        self.result_format = result_format;
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
