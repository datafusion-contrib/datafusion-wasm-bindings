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

use crate::error::Result;
use arrow::array::RecordBatch;
use arrow::util::display::FormatOptions;
use arrow::util::pretty::pretty_format_batches_with_options;
use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen]
pub enum ResultFormat {
    Table,
    Json,
}

impl ResultFormat {
    pub fn format_record_batch(&self, record_batches: &[RecordBatch]) -> Result<String> {
        match self {
            ResultFormat::Table => {
                let result =
                    pretty_format_batches_with_options(&record_batches, &FormatOptions::default())?
                        .to_string();
                Ok(result)
            }
            ResultFormat::Json => {
                let buf = Vec::new();
                let mut writer = arrow::json::ArrayWriter::new(buf);
                let record_batch_refs: Vec<&RecordBatch> = record_batches.iter().collect();
                writer.write_batches(&record_batch_refs)?;
                writer.finish()?;

                Ok(String::from_utf8(writer.into_inner())?)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_record_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap()
    }

    #[test]
    fn test_format_record_batch_table() {
        let batch = create_test_record_batch();
        let result = ResultFormat::Table.format_record_batch(&[batch]).unwrap();

        assert!(result.contains("id"));
        assert!(result.contains("name"));
        assert!(result.contains("Alice"));
        assert!(result.contains("Bob"));
        assert!(result.contains("Charlie"));
    }

    #[test]
    fn test_format_record_batch_json() {
        let batch = create_test_record_batch();
        let result = ResultFormat::Json.format_record_batch(&[batch]).unwrap();

        assert!(result.contains(r#""id":"#));
        assert!(result.contains(r#""name":"#));
        assert!(result.contains("Alice"));
        assert!(result.contains("Bob"));
        assert!(result.contains("Charlie"));
    }
}
