use serde::de::Error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

use crate::core::types::{DataType, DbValue, Row, Schema};

/// Перечисление всех потенциальных ошибок инициализации и семантического анализа.
/// Реализует Fail-Fast протокол до старта асинхронных воркеров.
#[derive(Debug)]
pub enum ConfigurationError {
    Io(std::io::Error),
    Yaml(serde_yaml::Error),
    InvalidConnectionString { connector: String, details: String },
    PermissionDenied { path: PathBuf, mode: &'static str },
    CyclicRename { field: String },
    InvalidCastType { field: String, target: String },
    ColumnNotFound { field: String },
}

impl fmt::Display for ConfigurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "Дисковый сбой ввода-вывода (IO): {err}"),
            Self::Yaml(err) => write!(f, "Ошибка синтаксиса YAML-конфигурации: {err}"),
            Self::InvalidConnectionString { connector, details } => {
                write!(
                    f,
                    "Некорректный URL подключения для коннектора '{connector}': {details}"
                )
            }
            Self::PermissionDenied { path, mode } => {
                write!(
                    f,
                    "Недостаточно прав доступа ОС ({mode}) к объекту: {}",
                    path.display()
                )
            }
            Self::CyclicRename { field } => {
                write!(f, "Обнаружена циклическая зависимость в цепочке трансформаций: поле '{field}' рекурсивно переименовывается")
            }
            Self::InvalidCastType { field, target } => {
                write!(
                    f,
                    "Неподдерживаемый тип для кастинга поля '{field}': '{target}'"
                )
            }
            Self::ColumnNotFound { field } => {
                write!(f, "Конфигурация трансформации ссылается на отсутствующее поле в исходной схеме: '{field}'")
            }
        }
    }
}

impl std::error::Error for ConfigurationError {}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ConnectorConfig {
    pub r#type: String,
    pub connection_string: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TransformStep {
    pub operation: String, // "rename", "cast", "mask"
    pub field: String,
    pub to: Option<String>,
    pub cast_type: Option<String>,
    pub mask_type: Option<String>, // "ip", "email"
}

/// Метамодель декларативного конфигурационного файла pipeline.yaml
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PipelineConfig {
    pub source_connector: ConnectorConfig,
    pub sink_connector: ConnectorConfig,
    pub state_file: String,
    pub transform_chain: Vec<TransformStep>,
}

/// Скомпилированный шаг маскирования для горячего рантайма.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeMaskType {
    Ip,
    Email,
}

/// Скомпилированная инструкция трансформации, оперирующая прямыми индексами вектора.
#[derive(Clone, Debug)]
pub enum RuntimeTransformStep {
    Cast {
        index: usize,
        target_type: DataType,
    },
    Mask {
        index: usize,
        mask_type: RuntimeMaskType,
    },
}

/// Исполнительный план трансформаций, очищенный от строкового оверхеда.
#[derive(Clone, Debug)]
pub struct CompiledTransformEngine {
    pub steps: Vec<RuntimeTransformStep>,
}

impl PipelineConfig {
    /// Загружает и производит первичный синтаксический разбор YAML-манифеста.
    pub fn load_yaml<P: AsRef<Path>>(path: P) -> Result<Self, ConfigurationError> {
        let content = fs::read_to_string(path).map_err(ConfigurationError::Io)?;
        let config: Self = serde_yaml::from_str(&content).map_err(ConfigurationError::Yaml)?;
        Ok(config)
    }

    /// Контур «Холодного валидатора» (Pre-flight Quality Gate 1).
    pub fn validate(&self, wal_dir: &Path) -> Result<(), ConfigurationError> {
        // Шаг 1: Проверка строк сетевых подключений схемы URL
        self.validate_connection_string(&self.source_connector, "source")?;
        self.validate_connection_string(&self.sink_connector, "sink")?;

        // Шаг 2: Верификация прав файловой системы ОС (защита WAL от паник)
        self.validate_filesystem_rights(wal_dir)?;

        // Шаг 3: Семантический анализ графа трансформаций на цикличность
        self.validate_transform_graph()?;

        Ok(())
    }

    fn validate_connection_string(
        &self,
        conn: &ConnectorConfig,
        role: &'static str,
    ) -> Result<(), ConfigurationError> {
        let url = &conn.connection_string;
        match conn.r#type.as_str() {
            "postgres" => {
                if !url.starts_with("postgres://") && !url.starts_with("postgresql://") {
                    return Err(ConfigurationError::InvalidConnectionString {
                        connector: role.to_string(),
                        details: "Строка подключения Postgres должна начинаться с 'postgres://' или 'postgresql://'".to_string(),
                    });
                }
            }
            "clickhouse" => {
                if !url.starts_with("clickhouse://")
                    && !url.starts_with("http://")
                    && !url.starts_with("https://")
                {
                    return Err(ConfigurationError::InvalidConnectionString {
                        connector: role.to_string(),
                        details: "Строка подключения ClickHouse должна начинаться с 'clickhouse://', 'http://' или 'https://'".to_string(),
                    });
                }
            }
            "mock" => {} // Имитационный полигон валиден всегда
            _ => {}      // Заглушки пула расширения пропускаются до их физической имплементации
        }
        Ok(())
    }

    fn validate_filesystem_rights(&self, wal_dir: &Path) -> Result<(), ConfigurationError> {
        // Проверка директории WAL логов
        if !wal_dir.exists() {
            fs::create_dir_all(wal_dir).map_err(ConfigurationError::Io)?;
        }
        let test_wal_file = wal_dir.join(".datacopter_gate");
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&test_wal_file)
            .map_err(|_| ConfigurationError::PermissionDenied {
                path: wal_dir.to_path_buf(),
                mode: "WRITE",
            })?;
        fs::remove_file(test_wal_file).map_err(ConfigurationError::Io)?;

        // Проверка доступности state_file прогресса
        let state_path = PathBuf::from(&self.state_file);
        if let Some(parent) = state_path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                fs::create_dir_all(parent).map_err(ConfigurationError::Io)?;
            }
        }
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&state_path)
            .map_err(|_| ConfigurationError::PermissionDenied {
                path: state_path.clone(),
                mode: "READ/WRITE APPEND",
            })?;

        Ok(())
    }

    fn validate_transform_graph(&self) -> Result<(), ConfigurationError> {
        let mut current_names: HashMap<String, String> = HashMap::new(); // Поле -> Исходное поле

        for step in &self.transform_chain {
            match step.operation.as_str() {
                "rename" => {
                    let to_name = step.to.as_ref().ok_or_else(|| {
                        ConfigurationError::Yaml(serde_yaml::Error::custom(
                            "Операция 'rename' требует обязательного указания поля 'to'",
                        ))
                    })?;

                    // Алгоритм поиска циклов renames (A -> B -> A)
                    if to_name == &step.field {
                        return Err(ConfigurationError::CyclicRename {
                            field: step.field.clone(),
                        });
                    }
                    if let Some(origin) = current_names.get(to_name) {
                        if origin == &step.field {
                            return Err(ConfigurationError::CyclicRename {
                                field: step.field.clone(),
                            });
                        }
                    }
                    current_names.insert(to_name.clone(), step.field.clone());
                }
                "cast" => {
                    let target = step.cast_type.as_ref().ok_or_else(|| {
                        ConfigurationError::Yaml(serde_yaml::Error::custom(
                            "Операция 'cast' требует обязательного указания 'cast_type'",
                        ))
                    })?;
                    match target.as_str() {
                        "Int8" | "Int16" | "Int32" | "Int64" | "UInt8" | "UInt16" | "UInt32"
                        | "UInt64" | "Float32" | "Float64" | "Bool" | "String" | "Bytes"
                        | "DateTime" => {}
                        _ => {
                            return Err(ConfigurationError::InvalidCastType {
                                field: step.field.clone(),
                                target: target.clone(),
                            })
                        }
                    }
                }
                "mask" => {
                    let mask = step.mask_type.as_ref().ok_or_else(|| {
                        ConfigurationError::Yaml(serde_yaml::Error::custom(
                            "Операция 'mask' требует обязательного указания 'mask_type'",
                        ))
                    })?;
                    if mask != "ip" && mask != "email" {
                        return Err(ConfigurationError::Yaml(serde_yaml::Error::custom(
                            "Поле 'mask_type' должно принимать значения 'ip' или 'email'",
                        )));
                    }
                }
                op => {
                    return Err(ConfigurationError::Yaml(serde_yaml::Error::custom(
                        format!("Неподдерживаемая операция трансформации: '{op}'"),
                    )))
                }
            }
        }
        Ok(())
    }

    /// Компиляция декларативного графа в плоский Runtime-план.
    /// Модифицирует Schema приемника ОДИН РАЗ, исключая накладные расходы из горячего цикла.
    pub fn compile_engine(
        &self,
        schema: &mut Schema,
    ) -> Result<CompiledTransformEngine, ConfigurationError> {
        let mut runtime_steps = Vec::new();

        for step in &self.transform_chain {
            // Находим физический индекс колонки по ее текущему имени в схеме metadata
            let col_idx = schema
                .columns
                .iter()
                .position(|c| c.name == step.field)
                .ok_or_else(|| ConfigurationError::ColumnNotFound {
                    field: step.field.clone(),
                })?;

            match step.operation.as_str() {
                "rename" => {
                    let new_name = step.to.as_ref().unwrap().clone();
                    schema.columns[col_idx].name = new_name;
                    // Данные в Row не двигаются, runtime_step сюда не пишется (Zero runtime cost)
                }
                "cast" => {
                    let target_str = step.cast_type.as_ref().unwrap();
                    let target_type = match target_str.as_str() {
                        "Int8" => DataType::Int8,
                        "Int16" => DataType::Int16,
                        "Int32" => DataType::Int32,
                        "Int64" => DataType::Int64,
                        "UInt8" => DataType::UInt8,
                        "UInt16" => DataType::UInt16,
                        "UInt32" => DataType::UInt32,
                        "UInt64" => DataType::UInt64,
                        "Float32" => DataType::Float32,
                        "Float64" => DataType::Float64,
                        "Bool" => DataType::Bool,
                        "String" => DataType::String,
                        "Bytes" => DataType::Bytes,
                        "DateTime" => DataType::DateTime,
                        _ => unreachable!(),
                    };
                    schema.columns[col_idx].data_type = target_type;
                    runtime_steps.push(RuntimeTransformStep::Cast {
                        index: col_idx,
                        target_type,
                    });
                }
                "mask" => {
                    let mask_str = step.mask_type.as_ref().unwrap();
                    let mask_type = if mask_str == "ip" {
                        RuntimeMaskType::Ip
                    } else {
                        RuntimeMaskType::Email
                    };
                    runtime_steps.push(RuntimeTransformStep::Mask {
                        index: col_idx,
                        mask_type,
                    });
                }
                _ => unreachable!(),
            }
        }

        Ok(CompiledTransformEngine {
            steps: runtime_steps,
        })
    }
}

impl CompiledTransformEngine {
    /// Главная исполняемая точка горячего цикла конвейера (INV-006).
    /// Выполняет In-Place модификацию структур данных со скоростью memcpy.
    #[inline]
    pub fn execute(&self, row: &mut Row) {
        for step in &self.steps {
            match step {
                RuntimeTransformStep::Cast { index, target_type } => {
                    if *index >= row.values.len() {
                        continue;
                    }

                    // Выполняем точечное аппаратное понижение разрядности (INV-005)
                    let current_val = &row.values[*index];
                    if let Some(downscaled) = self.cast_value(current_val, target_type) {
                        row.values[*index] = downscaled;
                    }
                }
                RuntimeTransformStep::Mask { index, mask_type } => {
                    if *index >= row.values.len() {
                        continue;
                    }

                    if let DbValue::StringRef(slice) = row.values[*index] {
                        if let Some(raw_str) = row.get_string(&slice) {
                            match mask_type {
                                RuntimeMaskType::Ip => {
                                    // Маскирование IPv4 октетов непосредственно в Арене
                                    if let Some(last_dot_idx) = raw_str.rfind('.') {
                                        let mut masked_ip = String::with_capacity(last_dot_idx + 2);
                                        masked_ip.push_str(&raw_str[..=last_dot_idx]);
                                        masked_ip.push('0');

                                        // Пишем новую строку в конец Арены In-place
                                        let new_slice = row.push_string(&masked_ip);
                                        row.values[*index] = DbValue::StringRef(new_slice);
                                    }
                                }
                                RuntimeMaskType::Email => {
                                    // Сверхбыстрое некриптографическое FNV-1a хэширование email
                                    let hash = self.fnv1a_hash(raw_str.as_bytes());
                                    let mut hex_buf = [0u8; 16];
                                    let hex_str = self.bytes_to_hex_fallback(hash, &mut hex_buf);

                                    let new_slice = row.push_string(hex_str);
                                    row.values[*index] = DbValue::StringRef(new_slice);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[inline]
    fn cast_value(&self, val: &DbValue, target: &DataType) -> Option<DbValue> {
        // Реализация точечной downscale-мутации примитивов из Int64 во всё вплоть до Int8
        match (val, target) {
            (DbValue::Int64(v), DataType::Int8) => Some(DbValue::Int8(*v as i8)),
            (DbValue::Int64(v), DataType::Int16) => Some(DbValue::Int16(*v as i16)),
            (DbValue::Int64(v), DataType::Int32) => Some(DbValue::Int32(*v as i32)),
            (DbValue::Int32(v), DataType::Int8) => Some(DbValue::Int8(*v as i8)),
            (DbValue::Int32(v), DataType::Int16) => Some(DbValue::Int16(*v as i16)),
            (DbValue::UInt64(v), DataType::UInt8) => Some(DbValue::UInt8(*v as u8)),
            (DbValue::UInt64(v), DataType::UInt16) => Some(DbValue::UInt16(*v as u16)),
            (DbValue::UInt64(v), DataType::UInt32) => Some(DbValue::UInt32(*v as u32)),
            _ => None, // Если типы аппаратно не кастятся или кастинг не описан, значение сохраняется
        }
    }

    #[inline]
    fn fnv1a_hash(&self, bytes: &[u8]) -> u64 {
        let mut hash = 0xcbf29ce484222325u64;
        for &byte in bytes {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3u64);
        }
        hash
    }

    #[inline]
    fn bytes_to_hex_fallback<'a>(&self, num: u64, buf: &'a mut [u8; 16]) -> &'a str {
        const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
        for i in (0..16).rev() {
            let nibble = ((num >> (i * 4)) & 0xf) as usize;
            buf[15 - i] = HEX_CHARS[nibble];
        }
        std::str::from_utf8(buf).unwrap()
    }
}
