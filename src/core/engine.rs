use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration, Instant};
use tokio_util::sync::CancellationToken;

use crate::config::{CompiledTransformEngine, PipelineConfig};
use crate::connectors::{Sink, Source};
use crate::core::autotune::Autotuner;
use crate::core::types::Row;
use crate::wal::Manager as WalManager;

/// Атомарное состояние конвейера. Учитывает слабую модель памяти RISC-процессоров (ARM64).
pub struct EngineState {
    pub is_initialized: AtomicBool,
    pub is_recovery_mode: AtomicBool,
    pub total_processed_rows: AtomicUsize,
    pub last_batch_rtt_ms: AtomicUsize,
}

pub struct Engine {
    pub state: Arc<EngineState>,
    pub config: PipelineConfig,
    pub wal_dir: PathBuf,
}

impl Engine {
    pub fn new(config: PipelineConfig, wal_dir: PathBuf) -> Self {
        Self {
            state: Arc::new(EngineState {
                // Использование семантики Relaxed здесь безопасно только при старте
                is_initialized: AtomicBool::new(false),
                is_recovery_mode: AtomicBool::new(false),
                total_processed_rows: AtomicUsize::new(0),
                last_batch_rtt_ms: AtomicUsize::new(0),
            }),
            config,
            wal_dir,
        }
    }

    /// Главный оркестратор асинхронных воркеров.
    pub async fn run(
        &mut self,
        source: Box<dyn Source>,
        sink: Box<dyn Sink>,
        max_memory_bytes: usize,
        columns_count: usize,
        transform_engine: CompiledTransformEngine,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let cancel_token = CancellationToken::new();

        // Перехват системного сигнала SIGINT для мягкой остановки (Greceful Shutdowm).
        let token_for_signal = cancel_token.clone();
        tokio::spawn(async move {
            if let Ok(_) = tokio::signal::ctrl_c().await {
                tracing::warn!(
                    "ВНИМАНИЕ: Получен сигнал прерывания (Ctrl+C). Начинаем Graceful Shutdown..."
                );
                token_for_signal.cancel();
            }
        });

        // Математический расчет Double Buffering RAM Layout (INV-003)
        let row_size_bytes = columns_count * 32;
        let channel_capacity = std::cmp::max(1000, max_memory_bytes / (row_size_bytes * 2));

        // Двухканальный контур циркуляции памяти (ADR-001)
        let (tx_data, rx_data) = mpsc::channel::<Row>(channel_capacity);
        let (tx_recycle, rx_recycle) = mpsc::channel::<Row>(channel_capacity);

        // Экземпляр WAL Менеджера для Writer (очистка) и Reader (запись)
        let wal_manager_reader = WalManager::new(self.wal_dir.clone());
        let wal_manager_writer = WalManager::new(self.wal_dir.clone());

        let autotuner = Autotuner::new(max_memory_bytes, row_size_bytes);

        // Фиксация старта с жестким упорядочиванием Release для архитектуры ARM64 (ADR-005)
        self.state.is_initialized.store(true, Ordering::Release);

        let reader_handle = spawn_reader_task(
            source,
            tx_data.clone(),
            rx_recycle,
            wal_manager_reader,
            cancel_token.clone(),
            columns_count,
        );

        let writer_handle = spawn_writer_task(
            sink,
            rx_data,
            tx_recycle,
            tx_data, // Передаем клон для замера метрик экстренного триггера
            wal_manager_writer,
            autotuner,
            transform_engine,
            self.state.clone(),
            cancel_token,
            channel_capacity,
        );

        let (reader_res, writer_res) = tokio::join!(reader_handle, writer_handle);
        reader_res??;
        writer_res??;

        Ok(())
    }
}

/// Поток вычитки (Reader Task)
pub fn spawn_reader_task(
    source: Box<dyn Source>,
    tx_data: mpsc::Sender<Row>,
    mut rx_recycle: mpsc::Receiver<Row>,
    mut wal_manager: WalManager,
    cancel_token: CancellationToken,
    columns_count: usize,
) -> JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("Reader Task: Получен сигнал отмены. Завершение вычитки.");
                    break;
                }
                else => {
                    // Ленивый разогрев пула строк (ADR-001)
                    let mut row = rx_recycle.try_recv().unwrap_or_else(|_| {
                        Row::with_capacity(columns_count, 1024)
                    });

                    // 1. Вычитка (Zero-Copy In-Place)
                    if let Err(e) = source.next_row(&mut row).await {
                        tracing::error!("Ошибка вычитки из источника: {}", e);
                        return Err(e.into());
                    }

                    // 2. Упреждающая персистентность (Исполнение INV-001)
                    if let Err(e) = wal_manager.append(&row) {
                        tracing::error!("КРИТИЧЕСКИЙ СБОЙ WAL: {}", e);
                        return Err(e.into());
                    }

                    // 3. Отправка в память (Обратное давление INV-004 срабатывает здесь)
                    if tx_data.send(row).await.is_err() {
                        tracing::warn!("Reader Task: Канал передачи закрыт. Остановка конвейера.");
                        break;
                    }
                }
            }
        }
        Ok(())
    })
}

/// Поток записи (Writer Task)
pub fn spawn_writer_task(
    sink: Box<dyn Sink>,
    mut rx_data: mpsc::Receiver<Row>,
    tx_recycle: mpsc::Sender<Row>,
    tx_monitor: mpsc::Sender<Row>, // Для детекции Emergency Trigger
    mut wal_manager: WalManager,
    mut autotuner: Autotuner,
    transform_engine: CompiledTransformEngine,
    state: Arc<EngineState>,
    cancel_token: CancellationToken,
    channel_capacity: usize,
) -> JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
    tokio::spawn(async move {
        let mut batch = Vec::with_capacity(autotuner.get_batch_size());
        let flush_timeout = Duration::from_secs(5); // Time Trigger
        let mut sleep_timer = Box::pin(sleep(flush_timeout));

        loop {
            let mut do_flush = false;

            tokio::select! {
                _ = &mut sleep_timer => {
                    // Time Trigger: сброс по таймеру (защита от зависания при низком трафике)
                    if !batch.is_empty() {
                        do_flush = true;
                    }
                    sleep_timer.as_mut().reset(Instant::now() + flush_timeout);
                }
                _ = cancel_token.cancelled() => {
                    tracing::info!("Writer Task: Сигнал отмены. Режим Drain активирован.");
                    if !batch.is_empty() {
                        do_flush = true;
                    } else {
                        break;
                    }
                }
                opt_row = rx_data.recv() => {
                    match opt_row {
                        Some(mut row) => {
                            // Трансформация строки In-Place (Zero-Allocation)
                            transform_engine.execute(&mut row);
                            batch.push(row);

                            // Оценка триггеров
                            let current_size = batch.len();
                            let target_size = autotuner.get_batch_size();

                            // Size Trigger
                            if current_size >= target_size {
                                do_flush = true;
                            } else {
                                // Emergency Trigger (INV-004 защита)
                                let available_capacity = tx_monitor.capacity();
                                let fill_ratio = 1.0 - (available_capacity as f64 / channel_capacity as f64);
                                if fill_ratio >= 0.90 {
                                    tracing::warn!("Emergency Flush: MPSC-канал заполнен на {:.0}%.", fill_ratio * 100.0);
                                    do_flush = true;
                                }
                            }
                        }
                        None => {
                            // Канал от Reader закрыт (Паника Reader или штатный shutdown)
                            tracing::info!("Writer Task: Канал данных закрыт. Вымывание остатков.");
                            if !batch.is_empty() {
                                do_flush = true;
                            } else {
                                break;
                            }
                        }
                    }
                }
            }

            if do_flush {
                // 1. Атомарная дисковая ротация перед отправкой в сеть
                wal_manager.rotate_active_to_shadow()?;

                // 2. Сетевой инсерт и замер RTT
                let start_rtt = Instant::now();
                sink.write_batch(&batch).await.map_err(|e| {
                    tracing::error!(
                        "Ошибка сетевой записи: {}. Очистка WAL заблокирована (INV-002).",
                        e
                    );
                    e // Запуск Retry Loop делегируется вовне/оболочке
                })?;
                let rtt_ms = start_rtt.elapsed().as_millis() as u64;

                // 3. Исполнение INV-002: Очистка теневого лога СТРОГО ПОСЛЕ сетевого Ack
                wal_manager.clear_shadow()?;

                // Обновление метрик (Relaxed ordering безопасно для счетчиков)
                autotuner.update_bounds(rtt_ms);
                state
                    .last_batch_rtt_ms
                    .store(rtt_ms as usize, Ordering::Relaxed);
                state
                    .total_processed_rows
                    .fetch_add(batch.len(), Ordering::Relaxed);

                // 4. Ресайклинг пула строк
                for mut row in batch.drain(..) {
                    row.reset(); // Сброс значений, сохранение capacity
                    let _ = tx_recycle.try_send(row); // Не блокируем Writer при переполнении пула
                }

                sleep_timer.as_mut().reset(Instant::now() + flush_timeout);
            }
        }
        Ok(())
    })
}
