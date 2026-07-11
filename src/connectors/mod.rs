use async_trait::async_trait;
use std::fmt;

use crate::core::types::{Row, Schema};

/// Универсальный тип ошибки, стирающий границы между драйверами различных СУБД.
/// Позволяет ядру однообразно обрабатывать сетевые сбои, десинхронизацию схем и IO-паники.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectorError {
    NotImplemented,
    ConnectionFailed(String),
    ExecutionFailed(String),
    SchemaMismatch(String),
    IoError(String),
}

impl fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotImplemented => write!(f, "Данный коннектор еще не реализован"),
            Self::ConnectionFailed(msg) => {
                write!(f, "Ошибка подключения к источнику/приемнику: {}", msg)
            }
            Self::ExecutionFailed(msg) => {
                write!(f, "Сбой выполнения запроса (Query/Execute): {}", msg)
            }
            Self::SchemaMismatch(msg) => {
                write!(f, "Критическое несовпадение метаданных (схемы): {}", msg)
            }
            Self::IoError(msg) => write!(f, "Дисковый или сетевой сбой ввода-вывода (IO): {}", msg),
        }
    }
}

// Реализация стандартного трейта Error для интеграции с Box<dyn std::error::Error> в main.rs
impl std::error::Error for ConnectorError {}

/// Объектно-безопасный контракт источника данных (Reader).
/// Исполняет INV-004 (Backpressure) за счет асинхронности и INV-006 (Zero-Alloc) в методе next_row.
#[async_trait]
pub trait Source: Send + Sync {
    /// Извлекает метаданные из источника и конвертирует их в единый язык ядра (core::types::DataType).
    async fn fetch_schema(&self) -> Result<Schema, ConnectorError>;

    /// Читает ровно одну запись из курсора СУБД прямо в переданную Арену (In-Place).
    /// При достижении конца потока (EOF) возвращает ошибку или специфичный сигнал (зависит от реализации).
    async fn next_row(&self, row: &mut Row) -> Result<(), ConnectorError>;
}

/// Объектно-безопасный контракт целевой системы (Writer).
#[async_trait]
pub trait Sink: Send + Sync {
    /// Выполняет DDL-сверку или создает целевую таблицу/топик на основе схемы, полученной от Source.
    async fn prepare_target(&self, schema: &Schema) -> Result<(), ConnectorError>;

    /// Выполняет атомарный сброс пакета данных (Batch) в сеть.
    /// Блокирует выполнение (await) до получения полного ACK от целевой системы (INV-002).
    async fn write_batch(&self, batch: &[Row]) -> Result<(), ConnectorError>;
}

// ==============================================================================
// Реестр коннекторов (Матрица расширения)
// ==============================================================================

pub mod clickhouse;
pub mod duckdb;
pub mod file_storage;
pub mod kafka;
pub mod mock;
pub mod mongodb;
pub mod mysql;
pub mod postgres;
pub mod rabbitmq;
pub mod s3;
pub mod sqlite;
