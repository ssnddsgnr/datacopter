use crate::connectors::{ConnectorError, Source};
use crate::core::types::{Row, Schema};
use async_trait::async_trait;

pub struct FileStorageSource {
    pub connection_string: String,
}

impl FileStorageSource {
    pub fn new(connection_string: String) -> Self {
        Self { connection_string }
    }
}

#[async_trait]
impl Source for FileStorageSource {
    async fn fetch_schema(&self) -> Result<Schema, ConnectorError> {
        // Продакшен-реализация вычитки каталогов будет здесь
        Err(ConnectorError::NotImplemented)
    }

    async fn next_row(&self, _row: &mut Row) -> Result<(), ConnectorError> {
        // Продакшен-реализация вычитки CDC / репликационного слота будет здесь
        Err(ConnectorError::NotImplemented)
    }
}
