pub mod cli;
pub mod config;
pub mod core;
pub mod wal;
pub mod connectors;

use tracing_subscriber::EnvFilter;

use crate::cli::{parse_arguments, parse_memory_limit};
use crate::config::PipelineConfig;
use crate::core::Engine;
use crate::wal::Recovery;

/// Императивный оркестратор холодного старта.
/// Последовательно инициализирует ресурсы, разворачивает Recovery Mode и порождает Engine.
#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // =========================================================================
    // Фаза 1 & 2: Парсинг CLI и инициализация системы логирования
    // =========================================================================
    let args = parse_arguments();
    
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(args.log_level.clone()))
        .init();
    
    tracing::info!("Запуск DataCopter. Лимит памяти: {}", args.max_memory);

    let max_memory_bytes = parse_memory_limit(&args.max_memory);

    // =========================================================================
    // Фаза 3: Валидация YAML и Холодный старт
    // =========================================================================
    let config = PipelineConfig::load_yaml(&args.config)?;
    config.validate(&args.wal_dir)?;
    tracing::info!("YAML конфигурация успешно прошла валидацию (Pre-flight Quality Gate).");

    // =========================================================================
    // Фаза 4: Фабрика коннекторов (Пока используем заглушки для связывания)
    // =========================================================================
    // TODO: В будущем здесь будет полноценный match по config.source_connector.type
    let source: Box<dyn crate::connectors::Source> = Box::new(crate::connectors::mock::MockSource::new());
    let sink: Box<dyn crate::connectors::Sink> = Box::new(crate::connectors::mock::MockSink::new());

    // =========================================================================
    // Фаза 5: Извлечение схемы и компиляция Transform Engine
    // =========================================================================
    let mut schema = source.fetch_schema().await?;
    let transform_engine = config.compile_engine(&mut schema)?;
    sink.prepare_target(&schema).await?;

    let columns_count = schema.columns.len();

    // =========================================================================
    // Фаза 6: Аварийная эвакуация WAL (Recovery Mode)
    // =========================================================================
    let mut recovery = Recovery::new(args.wal_dir.clone());
    let mut recovered_batch = recovery.run_evacuation()?;
    
    if !recovered_batch.is_empty() {
        tracing::warn!("Recovery Mode: Обнаружено {} строк в WAL. Начинаем эвакуацию...", recovered_batch.len());
        
        // Трансформируем восстановленные "сырые" строки перед отправкой
        for row in &mut recovered_batch {
            transform_engine.execute(row);
        }
        
        // Синхронный сетевой сброс до старта асинхронного движка
        sink.write_batch(&recovered_batch).await?;
        
        // Очищаем директорию логов, так как эвакуация прошла успешно
        std::fs::remove_file(args.wal_dir.join("active.wal")).ok();
        std::fs::remove_file(args.wal_dir.join("shadow.wal")).ok();
        tracing::info!("Recovery Mode: Эвакуация успешно завершена. WAL очищен.");
    }

    // =========================================================================
    // Фаза 7: Запуск горячего рантайма
    // =========================================================================
    tracing::info!("Запуск асинхронного ядра (Engine) конвейера...");
    
    let mut engine = Engine::new(config, args.wal_dir);
    engine.run(source, sink, max_memory_bytes, columns_count, transform_engine).await?;

    tracing::info!("DataCopter завершил работу без ошибок.");
    Ok(())
}
