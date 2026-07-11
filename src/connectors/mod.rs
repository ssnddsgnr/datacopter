use async_trait::async_trait;
use std::fmt;
use crate::core::types::{Row, Schema};

#[derive(Debug)]
pub enum ConnectorError {
    NotImplemented,
    ConnectionFailed(String),
    ExecutionFailed(String),
    SchemaMismatch(String),
}

impl fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotImplemented => write!(f, "Данный функционал коннектора еще не реализован"),
            Self::ConnectionFailed(msg) => write!(f, "Сбой сетевого подключения к СУБД: {msg}"),
            Self::ExecutionFailed(msg) => write!(f, "Ошибка выполнения бинарного протокола СУБД: {msg}"),
            Self::SchemaMismatch(msg) => write!(f, "Несоответствие валидации схем данных: {msg}"),
        }
    }
}

impl std::error::Error for ConnectorError {}

/// Объектно-безопасный интерфейс вычитки данных из источника.
#[async_trait]
pub ResultTrait Source: Send + Sync {
    /// Опрашивает системные каталоги источника и транслирует родные типы в DataType ядра.
    async fn fetch_schema(&self) -> Result<Schema, ConnectorError>;

    /// In-Place заполнение буфера: принимает пустую заготовку из пула и наполняет её сырыми байтами.
    async fn next_row(&self, row: &mut Row) -> Result<(), ConnectorError>;
}

/// Объектно-безопасный интерфейс групповой записи данных в приемник.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Идемпотентная подготовка целевой структуры базы (CREATE TABLE IF NOT EXISTS).
    async fn prepare_target(&self, schema: &Schema) -> Result<(), ConnectorError>;

    /// Принимает неизменяемый срез накопленного батча строк для трансляции в сетевой сокет.
    async fn write_batch(&self, batch: &[Row]) -> Result<(), ConnectorError>;
}

// Декларация активных подмодулей репозитория
pub mod mock;
pub mod postgres;
pub mod clickhouse;
pub mod duckdb;
pub mod file_storage;
pub mod kafka;
pub mod mongodb;
pub mod mysql;
pub mod rabbitmq;
pub mod s3;
pub mod sqlite;
