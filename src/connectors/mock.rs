use async_trait::async_trait;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::connectors::{ConnectorError, Sink, Source};
use crate::core::types::{Column, DataType, DbValue, Row, Schema};

/// Высокопроизводительный имитатор источника данных.
/// Генерирует бесконечный поток синтетических данных для стресс-тестирования ядра.
pub struct MockSource {
    pub counter: AtomicI64,
}

impl MockSource {
    pub fn new() -> Self {
        Self {
            // Используем атомарный счетчик для потокобезопасной генерации уникальных ID
            counter: AtomicI64::new(0),
        }
    }
}

#[async_trait]
impl Source for MockSource {
    /// Формирует эталонную схему данных для компилятора трансформаций.
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
                    is_nullable: false,
                },
                Column {
                    name: "metric".to_string(),
                    data_type: DataType::Float64,
                    is_nullable: false,
                },
            ],
        })
    }

    /// Генерирует строку In-Place.
    /// Строго соблюдается INV-006: никаких `String::new()` или макросов `format!()` с выделением памяти.
    async fn next_row(&self, row: &mut Row) -> Result<(), ConnectorError> {
        let current_id = self.counter.fetch_add(1, Ordering::Relaxed);

        // 1. Прямая запись строкового среза в физическую Арену
        let slice = row.push_string("mock_synthetic_payload");

        // 2. Упаковка примитивов (DbValue аппаратно выровнены до 32 байт)
        row.values.push(DbValue::Int64(current_id));
        row.values.push(DbValue::StringRef(slice));
        row.values
            .push(DbValue::Float64(current_id as f64 * 3.1415));

        // Эмуляция микрозадержки вычитки с диска/сети (опционально для бенчмарков)
        // tokio::task::yield_now().await;

        Ok(())
    }
}

impl Default for MockSource {
    fn default() -> Self {
        Self::new()
    }
}

/// Имитатор целевого хранилища (Sink).
/// Абсорбирует батчи и эмулирует сетевое сопротивление (RTT).
pub struct MockSink;

impl MockSink {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Sink for MockSink {
    /// Принимает и верифицирует скомпилированную схему приемника.
    async fn prepare_target(&self, schema: &Schema) -> Result<(), ConnectorError> {
        tracing::info!(
            "MockSink: Целевая система подготовлена. Ожидается колонок: {}",
            schema.columns.len()
        );
        Ok(())
    }

    /// Поглощает пакет данных и симулирует сетевую задержку записи для Autotuner.
    async fn write_batch(&self, batch: &[Row]) -> Result<(), ConnectorError> {
        // Симуляция RTT (Round Trip Time) — 50 миллисекунд.
        // Autotuner (AIMD) будет использовать это время для расчета коэффициента деградации или роста.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        tracing::debug!(
            "MockSink: Успешно поглощен и подтвержден пакет из {} строк.",
            batch.len()
        );

        Ok(())
    }
}

impl Default for MockSink {
    fn default() -> Self {
        Self::new()
    }
}
