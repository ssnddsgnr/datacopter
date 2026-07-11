use crate::connectors::{ConnectorError, Sink};
use crate::core::types::{Row, Schema};
use async_trait::async_trait;

pub struct DuckdbSink {
    pub connection_string: String,
}

impl DuckdbSink {
    pub fn new(connection_string: String) -> Self {
        Self { connection_string }
    }
}

#[async_trait]
impl Sink for DuckdbSink {
    async fn prepare_target(&self, _schema: &Schema) -> Result<(), ConnectorError> {
        // Продакшен DDL-генератор (CREATE TABLE IF NOT EXISTS) будет здесь
        Err(ConnectorError::NotImplemented)
    }

    async fn write_batch(&self, _batch: &[Row]) -> Result<(), ConnectorError> {
        // Продакшен-реализация атомарной Bulk-вставки транзакции будет здесь
        Err(ConnectorError::NotImplemented)
    }
}
