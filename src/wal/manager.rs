use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::core::types::Row;
use crc32fast::Hasher;

/// Диспетчер горячего цикла упреждающей записи.
/// Управляет дескрипторами ОС и бинарной сериализацией логов.
pub struct Manager {
    pub wal_dir: PathBuf,
    active_file: File,
    entry_counter: u64,
    /// Переиспользуемый буфер для сериализации bincode. Исключает аллокации в куче (INV-006).
    buffer: Vec<u8>,
}

impl Manager {
    /// Инициализирует WAL-менеджер, гарантирует наличие директории и открывает active.wal.
    pub fn new(wal_dir: PathBuf) -> Self {
        if !wal_dir.exists() {
            fs::create_dir_all(&wal_dir).expect("CRITICAL: Не удалось создать директорию WAL");
        }

        let active_path = wal_dir.join("active.wal");
        let active_file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true) // Исключение вызовов lseek, строго линейный ввод-вывод
            .open(&active_path)
            .unwrap_or_else(|e| {
                tracing::error!("CRITICAL: Ошибка открытия active.wal: {}", e);
                std::process::exit(1);
            });

        Self {
            wal_dir,
            active_file,
            entry_counter: 0,
            buffer: Vec::with_capacity(4096), // Преаллокация буфера сериализации
        }
    }

    /// Сериализует Row через bincode, рассчитывает CRC32 и выполняет линейный append.
    /// Исполняет закон INV-001 (Сначала диск).
    pub fn append(&mut self, row: &Row) -> Result<(), io::Error> {
        self.buffer.clear();

        // 1. Бинарная сериализация полезной нагрузки (включая arena, согласно ADR-004)
        if let Err(e) = bincode::serialize_into(&mut self.buffer, row) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Bincode error: {}", e),
            ));
        }

        let payload_len = self.buffer.len() as u32;
        self.entry_counter += 1;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // 2. Формируем первые 20 байт заголовка для хеширования
        let mut header_base = [0u8; 20];
        header_base[0..8].copy_from_slice(&self.entry_counter.to_le_bytes());
        header_base[8..16].copy_from_slice(&timestamp.to_le_bytes());
        header_base[16..20].copy_from_slice(&payload_len.to_le_bytes());

        // 3. SIMD-расчет CRC32 от заголовка и payload
        let mut hasher = Hasher::new();
        hasher.update(&header_base);
        hasher.update(&self.buffer);
        let crc32 = hasher.finalize();

        // 4. Запись блока на диск: 24 байта заголовка + payload
        let mut header_full = [0u8; 24];
        header_full[0..20].copy_from_slice(&header_base);
        header_full[20..24].copy_from_slice(&crc32.to_le_bytes());

        self.active_file.write_all(&header_full)?;
        self.active_file.write_all(&self.buffer)?;

        // Strict Mode Fsync: гарантирует физическую персистентность на диске до возврата управления (INV-001)
        self.active_file.sync_all()?;

        Ok(())
    }

    /// Принудительный сброс буферов (вызывается при Graceful Shutdown).
    pub fn flush(&mut self) -> Result<(), io::Error> {
        self.active_file.sync_all()
    }

    /// Атомарно ротирует active.wal в shadow.wal на уровне дескрипторов ОС.
    pub fn rotate_active_to_shadow(&mut self) -> Result<(), io::Error> {
        self.active_file.sync_all()?; // Фиксируем остатки перед закрытием

        let active_path = self.wal_dir.join("active.wal");
        let shadow_path = self.wal_dir.join("shadow.wal");

        // Атомарное переименование на уровне inode ОС
        fs::rename(&active_path, &shadow_path)?;

        // Мгновенное открытие нового чистого active.wal
        self.active_file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&active_path)?;

        // Сбрасываем счетчик для нового файла
        self.entry_counter = 0;

        Ok(())
    }

    /// Удаляет shadow.wal строго после успешного сетевого Ack от приемника.
    /// Исполняет закон Ack/Commit Isolation (INV-002).
    pub fn clear_shadow(&mut self) -> Result<(), io::Error> {
        let shadow_path = self.wal_dir.join("shadow.wal");
        if shadow_path.exists() {
            fs::remove_file(&shadow_path)?;
        }
        Ok(())
    }
}
