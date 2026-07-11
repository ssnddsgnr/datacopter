pub mod autotune;
pub mod engine;
pub mod types;

// Экспортируем главный оркестратор конвейера для вызова в main.rs
pub use engine::Engine;
