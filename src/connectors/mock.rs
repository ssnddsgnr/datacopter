use crate::connectors::{ConnectorError, Sink, Source};
use crate::core::types::{Column, DataType, Row, Schema};
use async_trait::async_trait;

pub struct MockSource;
pub struct MockSink;

#[async_trait]
impl Source for MockSource {
    async fn fetch_schema(&self) -> Result<Schema, ConnectorError> {
        Ok(Schema {
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    is_nullable: false,
                },
                Column {
                    name: "payload".to_string(),
                    data_type: DataType::String,
                    is_nullable: true,
                },
            ],
        })
    }

    async fn next_row(&self, row: &mut Row) -> Result<(), ConnectorError> {
        row.reset(); // Обязательный сброс Арены (INV-006)
        row.values.push(crate::core::types::DbValue::Int64(42));
        let slice = row.push_string("synthetic_data_line");
        row.values
            .push(crate::core::types::DbValue::StringRef(slice));
        Ok(())
    }
}

#[async_trait]
impl Sink for MockSink {
    async fn prepare_target(&self, _schema: &Schema) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&self, _batch: &[Row]) -> Result<(), ConnectorError> {
        // Симулирует моментальную идеальную вставку
        Ok(())
    }
}
