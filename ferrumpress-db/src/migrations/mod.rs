pub mod media;   // <-- новый модуль
use std::sync::Arc;

use crate::migrations::media::CreateMediaTable;
// use crate::migrations::users::CreateUsersTable; // если будет миграция пользователей

/// Добавляет все системные миграции в переданный Migrator
pub fn add_builtin_migrations(migrator: &mut Migrator) {
    // Миграция 001 – таблица пользователей (добавьте, если уже есть)
    // migrator.add_migration(Arc::new(CreateUsersTable));

    // Миграция 002 – таблица media
    migrator.add_migration(Arc::new(CreateMediaTable));
}