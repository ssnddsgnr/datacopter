use serde::{Deserialize, Serialize};
use static_assertions::assert_eq_size;

/// Легковесный срез, указывающий на расположение динамических данных внутри Арены строки.
/// Заменяет собой тяжелые аллокации String и Vec в горячем цикле.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct OffsetLen {
    pub offset: u32,
    pub len: u32,
}

/// Строго типизированное перечисление универсальных примитивов ядра (Tagged Union).
/// Все Copy-варианты аппаратно выровнены. Использование зарезервированного поля
/// гарантирует размер ровно в 32 байта для точного преаллоцирования очередей (INV-005).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum DbValue {
    Null,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    StringRef(OffsetLen),
    BytesRef(OffsetLen),
    DateTime(i64),      // UNIX-timestamp в микросекундах
    Reserved([u8; 24]), // Заградительный барьер для сохранения стабильного Layout на x86_64 и ARM64
}

// Контрольная точка качества на этапе компиляции: проверяем физический размер структуры
assert_eq_size!(DbValue, [u8; 32]);

/// Перечисление типов данных, выступающее единым «языком-посредником» между СУБД (O(N) маппинг).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Bool,
    String,
    Bytes,
    DateTime,
}

/// Спецификация метаданных отдельной колонки.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub is_nullable: bool,
}

/// Универсальная структура схемы данных реплицируемой таблицы.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    pub columns: Vec<Column>,
}

/// Гибридный Layout строки, реализующий концепцию Contiguous Arena-Backed Rows.
/// Память под вектора выделяется один раз и циркулирует по замкнутому кругу (Row Recycler).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Row {
    pub values: Vec<DbValue>,
    // Арена сериализуется (нет #[serde(skip)]) для исключения паник Out of Bounds в wal::Recovery.
    pub arena: Vec<u8>,
}

impl Row {
    /// Создает пустую структуру строки.
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            arena: Vec::new(),
        }
    }

    /// Инициализирует строку с преаллокацией емкости для полного исключения реаллокаций.
    pub fn with_capacity(values_capacity: usize, arena_capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(values_capacity),
            arena: Vec::with_capacity(arena_capacity),
        }
    }

    /// Сбрасывает логическую длину векторов до 0, сохраняя выделенную емкость (capacity) (INV-006).
    #[inline]
    pub fn reset(&mut self) {
        self.values.clear();
        self.arena.clear();
    }

    /// Записывает текстовый срез In-Place прямо в Арену строки и возвращает дескриптор смещения.
    #[inline]
    pub fn push_string(&mut self, val: &str) -> OffsetLen {
        let offset = self.arena.len() as u32;
        let len = val.len() as u32;
        self.arena.extend_from_slice(val.as_bytes());
        OffsetLen { offset, len }
    }

    /// Записывает бинарный блоб In-Place прямо в Арену строки и возвращает дескриптор смещения.
    #[inline]
    pub fn push_bytes(&mut self, val: &[u8]) -> OffsetLen {
        let offset = self.arena.len() as u32;
        let len = val.len() as u32;
        self.arena.extend_from_slice(val);
        OffsetLen { offset, len }
    }

    /// Безопасно извлекает строковое представление из Арены строки по дескриптору смещения.
    #[inline]
    pub fn get_string(&self, slice: &OffsetLen) -> Option<&str> {
        let start = slice.offset as usize;
        let end = (slice.offset + slice.len) as usize;
        if end <= self.arena.len() {
            std::str::from_utf8(&self.arena[start..end]).ok()
        } else {
            None
        }
    }

    /// Безопасно извлекает бинарный срез из Арены строки по дескриптору смещения.
    #[inline]
    pub fn get_bytes(&self, slice: &OffsetLen) -> Option<&[u8]> {
        let start = slice.offset as usize;
        let end = (slice.offset + slice.len) as usize;
        if end <= self.arena.len() {
            Some(&self.arena[start..end])
        } else {
            None
        }
    }
}

impl Default for Row {
    fn default() -> Self {
        Self::new()
    }
}
