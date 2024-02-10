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

use arrow::util::{display::FormatOptions, pretty::pretty_format_batches_with_options};
use datafusion::{
    execution::{
        cache::cache_manager::{self, CacheManager},
        context::{SessionConfig, SessionContext},
        disk_manager::DiskManagerConfig,
        memory_pool::UnboundedMemoryPool,
        object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry},
        runtime_env::{RuntimeConfig, RuntimeEnv},
        DiskManager,
    },
    physical_plan::collect,
    sql::parser::DFParser,
};
// use datafusion_cli::helper::unescape_input;
use crate::console;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct DataFusionContext {
    session_context: Arc<SessionContext>,
    // runtime: Runtime,
}

#[wasm_bindgen]
impl DataFusionContext {
    pub fn greet() -> String {
        "hello from datafusion-wasm".to_string()
    }

    pub fn new() -> Self {
        crate::set_panic_hook();
        // let rt = Arc::new(
        //     RuntimeEnv::new(RuntimeConfig::new().with_disk_manager(DiskManagerConfig::Disabled))
        //         .unwrap(),
        // );

        console::log("DataFusionContext::new");
        let session_config = SessionConfig::new().with_target_partitions(1);
        console::log(format!("session_config: {:?}", session_config).as_str());

        // build rt separately
        let memory_pool = Arc::new(UnboundedMemoryPool::default());
        console::log(format!("memory pool: {:?}", memory_pool).as_str());
        let disk_manager = DiskManager::try_new(DiskManagerConfig::Disabled).unwrap();
        console::log(format!("disk manager: {:?}", disk_manager).as_str());
        let cache_manager = Arc::new(CacheManager::default());
        console::log(format!("cache manager: {:?}", cache_manager).as_str());
        let object_store_registry = Arc::new(DefaultObjectStoreRegistry::default());
        console::log(format!("object store registry: {:?}", object_store_registry).as_str());

        let rt = Arc::new(RuntimeEnv {
            memory_pool,
            disk_manager,
            cache_manager,
            object_store_registry,
        });
        console::log(format!("runtime env: {:?}", rt).as_str());

        let session_context = Arc::new(SessionContext::new_with_config_rt(session_config, rt));

        console::log("DataFusionContext::new done");

        Self { session_context }
    }

    // pub fn execute_sql(&self, sql: String) -> String {
    //     self.runtime.block_on(self.execute_inner(sql))
    // }

    pub async fn execute_sql(&self, sql: String) -> String {
        self.execute_inner(sql).await
    }
}

impl DataFusionContext {
    async fn execute_inner(&self, sql: String) -> String {
        let statements = DFParser::parse_sql(&sql).unwrap();
        let mut results = Vec::with_capacity(statements.len());

        for statement in statements {
            // let session_context = self.session_context.lock().await;
            let logical_plan = self
                .session_context
                .state()
                .statement_to_plan(statement)
                .await
                .unwrap();
            let data_frame = self
                .session_context
                .execute_logical_plan(logical_plan)
                .await
                .unwrap();
            let physical_plan = data_frame.create_physical_plan().await.unwrap();

            let task_ctx = self.session_context.task_ctx();
            let record_batches = collect(physical_plan, task_ctx).await.unwrap();
            let formatted =
                pretty_format_batches_with_options(&record_batches, &FormatOptions::default())
                    .unwrap()
                    .to_string();

            results.push(formatted)
        }

        format!("{}", results.join("\n"))
    }
}
