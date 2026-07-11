use std::cmp;

const B_MIN: usize = 10_000; // Нижний заградительный барьер для ClickHouse
const B_MAX_HARD_CAP: usize = 250_000; // Абсолютный верхний хард-кап
const TARGET_TIME_MS: u64 = 300; // Целевое (эталонное) время инсерта T_target
const ALPHA: f64 = 0.25; // Коэффициент агрессивности роста
const B_MAX_STEP: usize = 5000; // Жесткий ограничитель прироста за итерацию
const BETA: f64 = 0.5; // Базовый множитель сжатия

pub struct Autotuner {
    pub current_batch_size: usize,
    pub target_time_ms: u64,
    max_batch_bounds: usize,
}

impl Autotuner {
    /// Инициализация и преаллокация границ на основе лимитов ОС (INV-003).
    pub fn new(max_memory_bytes: usize, row_size_bytes: usize) -> Self {
        // Расчет B_max по закону двойной буферизации
        let calculated_b_max = max_memory_bytes / (row_size_bytes * 2);

        // Валидация хард-капа
        let mut max_batch_bounds = cmp::min(calculated_b_max, B_MAX_HARD_CAP);

        // Защита от экстремального OOM, когда памяти выделено меньше, чем нужно для B_MIN
        max_batch_bounds = cmp::max(max_batch_bounds, B_MIN);

        Self {
            current_batch_size: B_MIN, // Оптимистичный старт с нижней границы
            target_time_ms: TARGET_TIME_MS,
            max_batch_bounds,
        }
    }

    /// Главный автомат фазового перехода AIMD.
    pub fn update_bounds(&mut self, rtt_ms: u64) {
        let tn = rtt_ms as f64;
        let target = self.target_time_ms as f64;
        let current = self.current_batch_size as f64;

        if rtt_ms <= self.target_time_ms {
            // Режим линейного ускорения (Additive Increase)
            let ratio = (target - tn) / target;
            let step_f64 = current * ALPHA * ratio;
            let step = cmp::min(step_f64 as usize, B_MAX_STEP);

            let next_batch = cmp::min(self.current_batch_size + step, self.max_batch_bounds);

            tracing::debug!(
                mode = "Additive Increase",
                rtt_ms = rtt_ms,
                target_ms = self.target_time_ms,
                previous_batch = self.current_batch_size,
                next_batch = next_batch,
                step = step
            );

            self.current_batch_size = next_batch;
        } else {
            // Режим экстренного торможения (Multiplicative Decrease)
            let degradation_ratio = target / tn;
            let next_batch_f64 = current * BETA * degradation_ratio;

            let next_batch = cmp::max(next_batch_f64 as usize, B_MIN);

            tracing::debug!(
                mode = "Multiplicative Decrease",
                rtt_ms = rtt_ms,
                target_ms = self.target_time_ms,
                previous_batch = self.current_batch_size,
                next_batch = next_batch,
                degradation_ratio = degradation_ratio
            );

            self.current_batch_size = next_batch;
        }
    }

    /// Безопасный неблокирующий интерфейс чтения.
    #[inline]
    pub fn get_batch_size(&self) -> usize {
        self.current_batch_size
    }
}
