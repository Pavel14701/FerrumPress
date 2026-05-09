use barrel::types::Type;
use ferrumpress_core::model::ColumnKind;

pub fn column_kind_to_barrel(kind: &ColumnKind) -> Type {
    match kind {
        ColumnKind::Text => Type::Text,
        ColumnKind::Varchar(n) => Type::Varchar(*n),
        ColumnKind::Char(n) => Type::Char(*n),
        ColumnKind::SmallInt => Type::SmallInt,
        ColumnKind::Integer => Type::Integer,
        ColumnKind::BigInt => Type::BigInt,
        ColumnKind::Serial => Type::Custom("SERIAL".into()),
        ColumnKind::Float => Type::Float,
        ColumnKind::Double => Type::Double,
        ColumnKind::Boolean => Type::Boolean,
        ColumnKind::Timestamp => Type::Timestamp,
        ColumnKind::Date => Type::Date,
        ColumnKind::Time => Type::Time,
        ColumnKind::DateTime => Type::DateTime,
        ColumnKind::Blob => Type::Blob,
        ColumnKind::VarBinary(n) => Type::Custom(format!("VARBINARY({})", n)),
        ColumnKind::Uuid => Type::Uuid,
        ColumnKind::Json => Type::Json,
        ColumnKind::Xml => Type::Custom("XML".into()),
        ColumnKind::Decimal(p, s) => Type::Custom(format!("DECIMAL({},{})", p, s)),
        ColumnKind::Money => Type::Money,
        ColumnKind::Array(inner) => Type::Array(Box::new(column_kind_to_barrel(inner))),
        ColumnKind::Enum(vars) => Type::Custom(format!("ENUM({})", vars.join(", "))),
        ColumnKind::Custom(sql) => Type::Custom(sql.clone()),
    }
}