use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::path::PathBuf;

use crate::core::types::Row;
use crc32fast::Hasher;

/// Контур аварийной эвакуации и восстановления (Recovery Mode).
pub struct Recovery {
    pub wal_dir: PathBuf,
}

impl Recovery {
    pub fn new(wal_dir: PathBuf) -> Self {
        Self { wal_dir }
    }

    /// Сканирует директорию, верифицирует блоки и возвращает восстановленный пакет строк.
    /// Обрабатывает сначала shadow.wal, затем active.wal в строгом хронологическом порядке.
    pub fn run_evacuation(&mut self) -> Result<Vec<Row>, io::Error> {
        let mut recovery_batch = Vec::new();

        let shadow_path = self.wal_dir.join("shadow.wal");
        if shadow_path.exists() {
            tracing::info!("Recovery Mode: Обнаружен shadow.wal. Запуск эвакуации.");
            self.process_file(&shadow_path, &mut recovery_batch)?;
        }

        let active_path = self.wal_dir.join("active.wal");
        if active_path.exists() {
            tracing::info!("Recovery Mode: Обнаружен active.wal. Запуск эвакуации.");
            self.process_file(&active_path, &mut recovery_batch)?;
        }

        Ok(recovery_batch)
    }

    fn process_file(&self, path: &PathBuf, batch: &mut Vec<Row>) -> Result<(), io::Error> {
        // Открываем файл с правами на запись для возможности усечения битых хвостов (Torn Writes)
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        let mut iterator = WalIterator::new(&mut file);

        while let Some(result) = iterator.next() {
            match result {
                Ok(row) => batch.push(row),
                Err(e) => {
                    tracing::warn!(
                        "Recovery Mode: Чтение файла {} остановлено: {}",
                        path.display(),
                        e
                    );
                    break; // Итератор сам выполняет усечение файла, мы просто прерываем цикл
                }
            }
        }

        Ok(())
    }
}

/// Ленивый потоковый итератор для безопасного чтения WAL файлов без OOM.
pub struct WalIterator<'a> {
    file: &'a mut File,
    valid_bytes_pos: u64, // Точная байтовая граница последнего успешного блока
}

impl<'a> WalIterator<'a> {
    pub fn new(file: &'a mut File) -> Self {
        Self {
            file,
            valid_bytes_pos: 0,
        }
    }
}

impl<'a> Iterator for WalIterator<'a> {
    type Item = Result<Row, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut header_buf = [0u8; 24];

        // 1. Двухэтапная вычитка: чтение строго 24 байт заголовка
        let mut bytes_read = 0;
        while bytes_read < 24 {
            match self.file.read(&mut header_buf[bytes_read..]) {
                Ok(0) => {
                    // Конец файла
                    if bytes_read == 0 {
                        return None;
                    } else {
                        // Обрыв файла прямо на заголовке (Torn Write)
                        let _ = self.file.set_len(self.valid_bytes_pos);
                        return Some(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "Обрыв файла на чтении заголовка",
                        )));
                    }
                }
                Ok(n) => bytes_read += n,
                Err(e) => return Some(Err(e)),
            }
        }

        // Извлечение метаданных
        let mut payload_len_bytes = [0u8; 4];
        payload_len_bytes.copy_from_slice(&header_buf[16..20]);
        let payload_len = u32::from_le_bytes(payload_len_bytes) as usize;

        let mut crc_bytes = [0u8; 4];
        crc_bytes.copy_from_slice(&header_buf[20..24]);
        let expected_crc32 = u32::from_le_bytes(crc_bytes);

        // 2. Точечная довычитка тела полезной нагрузки (payload)
        let mut payload_buf = vec![0u8; payload_len];
        if let Err(e) = self.file.read_exact(&mut payload_buf) {
            // Файл оборвался на записи полезной нагрузки (Torn Write)
            let _ = self.file.set_len(self.valid_bytes_pos);
            return Some(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Обрыв файла на чтении payload: {}", e),
            )));
        }

        // 3. Аппаратная SIMD-верификация (Torn Writes Mitigation)
        let mut hasher = Hasher::new();
        hasher.update(&header_buf[0..20]); // Первые 20 байт: entry_id, timestamp, payload_len
        hasher.update(&payload_buf);
        let calculated_crc32 = hasher.finalize();

        if calculated_crc32 != expected_crc32 {
            // Критическое повреждение блока. Физически отсекаем дефектный мусор.
            tracing::error!("CRITICAL: Обнаружена разорванная запись (Torn Write). Ожидался CRC32: {}, расчетный: {}. Выполняется усечение файла.", expected_crc32, calculated_crc32);
            let _ = self.file.set_len(self.valid_bytes_pos);
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "CRC32 не совпадает",
            )));
        }

        // 4. Десериализация (bincode честно восстановит и values, и arena)
        match bincode::deserialize::<Row>(&payload_buf) {
            Ok(row) => {
                // Обновляем границу безопасного отсечения
                self.valid_bytes_pos += 24 + payload_len as u64;
                Some(Ok(row))
            }
            Err(e) => {
                let _ = self.file.set_len(self.valid_bytes_pos);
                Some(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Ошибка десериализации bincode: {}", e),
                )))
            }
        }
    }
}
