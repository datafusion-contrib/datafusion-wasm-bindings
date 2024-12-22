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

use thiserror::Error;
use wasm_bindgen::JsValue;

pub type Result<T> = std::result::Result<T, WasmError>;

#[derive(Error, Debug)]
pub enum WasmError {
    #[error("failed to parse: {0}")]
    ParserError(#[from] datafusion::sql::sqlparser::parser::ParserError),
    #[error("datafusion error: {0}")]
    DataFusionError(#[from] datafusion::error::DataFusionError),
    #[error("arrow error: {0}")]
    ArrowError(#[from] datafusion::arrow::error::ArrowError),
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("utf8 error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),
    #[error("other error: {0}")]
    Other(String),
}

impl Into<JsValue> for WasmError {
    fn into(self) -> JsValue {
        JsValue::from_str(&self.to_string())
    }
}
