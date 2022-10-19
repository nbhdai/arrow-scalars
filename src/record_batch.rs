use crate::{TableRow, ScalarValuable};
use arrow::record_batch::RecordBatch;

pub trait RowValuable {
    fn row_value(&self) -> TableRow;
}

impl RowValuable for RecordBatch {
    fn row_value(&self) -> TableRow {
        let mut row = TableRow::default();

        let schema = self.schema();
        for i in 0..self.num_columns() {
            let column = self.column(i).scalar(i);
            let name = schema.field(i).name();
            row.values.insert(name.to_string(), column.clone());
        }
        row
    }
}

