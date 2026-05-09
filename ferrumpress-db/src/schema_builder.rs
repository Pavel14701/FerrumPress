use barrel::Table;
use ferrumpress_core::model::ColumnKind;
use ferrumpress_core::schema::TableBuilder;
use crate::type_mapping::column_kind_to_barrel;

pub struct TableBuilderImpl {
    table: Table,
}

impl TableBuilderImpl {
    pub fn new(name: &str) -> Self { Self { table: Table::new(name) } }
    pub fn into_table(self) -> Table { self.table }
    pub fn get_table_name(&self) -> &str { self.table.name() }
}

impl TableBuilder for TableBuilderImpl {
    fn add_column(&mut self, name: &str, kind: ColumnKind, nullable: bool, unique: bool, default: Option<&str>) {
        let mut t = column_kind_to_barrel(&kind);
        if nullable { t = t.nullable(); } else { t = t.not_null(); }
        if unique { t = t.unique(); }
        if let Some(def) = default { t = t.default(def); }
        self.table.add_column(name, t);
    }
    fn set_primary_key(&mut self, columns: Vec<&str>) {
        self.table.set_primary_key(columns);
    }
}