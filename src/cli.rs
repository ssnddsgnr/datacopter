use clap::Parser;
use std::path::PathBuf;

/// Декларативный парсер системных ресурсов и ограничений.
#[derive(Parser, Debug)]
#[command(
    name = "datacopter",
    author = "Chyngalgan – Solo Architect & Engineer",
    version = "1.0.0",
    about = "Autonomous, resource-predictable real-time ETL engine built in Rust"
)]
pub struct CliArguments {
    /// Путь к YAML-файлу конфигурации пайплайна.
    #[arg(short, long, env = "DATACOPTER_CONFIG")]
    pub config: String,

    /// Лимит RAM под асинхронный буфер. Поддерживает суффиксы KB, MB, GB.
    #[arg(short, long, default_value = "512MB", env = "DATACOPTER_MAX_MEMORY")]
    pub max_memory: String,

    /// Директория для хранения логов предзаписи (active.wal, shadow.wal).
    #[arg(short, long, default_value = "./wal", env = "DATACOPTER_WAL_DIR")]
    pub wal_dir: PathBuf,

    /// Флаг отключения адаптивного батчинга. Если установлен, размер батча фиксирован.
    #[arg(long, env = "DATACOPTER_NO_AUTOTUNE")]
    pub no_autotune: bool,

    /// Фиксированный размер пачки (используется только при --no-autotune).
    #[arg(long, default_value_t = 25000, env = "DATACOPTER_BATCH_SIZE")]
    pub fixed_batch_size: usize,

    /// Уровень детализации логов: error, warn, info, debug, trace.
    #[arg(short, long, default_value = "info", env = "RUST_LOG")]
    pub log_level: String,
}

/// Инициализирует разбор аргументов ОС с принудительной валидацией clap.
#[inline]
pub fn parse_arguments() -> CliArguments {
    CliArguments::parse()
}

/// Детерминированно конвертирует строковые лимиты (KB, MB, GB) в абсолютное байтовое число.
/// Исполняет закон абсолютного лимита оперативной памяти (INV-003).
pub fn parse_memory_limit(raw_limit: &str) -> usize {
    let raw = raw_limit.trim().to_uppercase();

    let (num_str, multiplier) = if let Some(stripped) = raw.strip_suffix("GB") {
        (stripped, 1024 * 1024 * 1024)
    } else if let Some(stripped) = raw.strip_suffix("MB") {
        (stripped, 1024 * 1024)
    } else if let Some(stripped) = raw.strip_suffix("KB") {
        (stripped, 1024)
    } else if let Some(stripped) = raw.strip_suffix("B") {
        (stripped, 1)
    } else {
        // Если суффикс отсутствует, по умолчанию считаем байтами
        (raw.as_str(), 1)
    };

    let val: f64 = num_str.trim().parse().unwrap_or_else(|_| {
        eprintln!(
            "CRITICAL: Некорректный формат лимита памяти '--max-memory': {}. Ожидается формат числа с суффиксом (например, 512MB).",
            raw_limit
        );
        std::process::exit(1); // Fail-Fast протокол защиты до запуска рантайма
    });

    (val * (multiplier as f64)) as usize
}
