/// Диспетчер горячего цикла упреждающей записи.
pub mod manager;

/// Контур аварийной эвакуации и борьбы с разорванными записями (Torn Writes).
pub mod recovery;

// Экспортируем исключительно безопасные высокоуровневые абстракции
pub use manager::Manager;
pub use recovery::Recovery;
