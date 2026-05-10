pub mod media;

// Миграция для таблицы task_locks — только если активна фича task-locks
#[cfg(feature = "task-locks")]
pub mod task_locks;

use std::sync::Arc;
use crate::migrations::media::CreateMediaTable;

#[cfg(feature = "task-locks")]
use crate::migrations::task_locks::CreateTaskLocksTable;

// use crate::migrations::users::CreateUsersTable; // когда появится

/// Добавляет все системные миграции в переданный Migrator.
pub fn add_builtin_migrations(migrator: &mut Migrator) {
    // Миграция 002 – таблица media
    migrator.add_migration(Arc::new(CreateMediaTable));

    // Миграция 003 – таблица task_locks (только при включённой фиче)
    #[cfg(feature = "task-locks")]
    migrator.add_migration(Arc::new(CreateTaskLocksTable));

    // Миграция 001 – таблица пользователей (будет добавлена позже)
    // migrator.add_migration(Arc::new(CreateUsersTable));
}