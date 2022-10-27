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
    fn roll_back(&mut self, elements: Vec<String>, mut row: TableRow) -> TableRow {
        for element in elements {
            if let Some(value) = self.values.get_mut(&element).map(|values| values.pop()).flatten() {
                row.values.insert(element.to_string(), value);
            }
        }
        row
    } 
    pub fn push(&mut self, mut row: TableRow) -> Result<(), TableRow> {
        let mut roll_back: Vec<String> = Vec::with_capacity(self.values.len());
        for (key, column) in self.values.iter_mut() {
            if let Some(value) = row.values.remove(key) {
                if let Err(value) = column.push(value) {
                    row.values.insert(key.to_owned(), value);
                    break;
                } else {
                    roll_back.push(key.to_owned());
                }
            }
        }
        if !row.values.is_empty() {
            Err(self.roll_back(roll_back, row))
        } else {
            Ok(())
        }
    }
}