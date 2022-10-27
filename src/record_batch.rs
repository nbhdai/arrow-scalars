use std::collections::HashMap;

use crate::{ArrowScalarError, ScalarValuable, TableRow, Table, TableScalar};
use arrow::record_batch::RecordBatch;

pub trait RowValuable {
    fn row_value(&self, index: usize) -> Result<TableRow, ArrowScalarError>;
}

impl RowValuable for RecordBatch {
    fn row_value(&self, index: usize) -> Result<TableRow, ArrowScalarError> {
        let mut row = TableRow::default();

        let schema = self.schema();
        for i in 0..self.num_columns() {
            let column = self.column(i).scalar(index)?;
            let name = schema.field(i).name();
            row.values.insert(name.to_string(), column.clone());
        }
        Ok(row)
    }
}

impl RowValuable for Table {
    fn row_value(&self, index: usize) -> Result<TableRow, ArrowScalarError> {
        let values = self.values.iter().map(|(name, column)| {
            Ok((name.to_owned(), column.scalar(index)?))
        }).collect::<Result<HashMap<String, TableScalar>,ArrowScalarError>>()?;
        Ok(TableRow { values })
    }
}

impl Table {
    pub fn push(&mut self, row: TableRow) -> Result<(), TableRow> {
        todo!()
    }
}