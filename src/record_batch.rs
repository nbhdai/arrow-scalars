use std::{collections::HashMap, sync::Arc};

use crate::{
    ArrowScalarError, ListValuable, ScalarValuable, Table, TableList, TableRow,
    TableScalar,
};
use arrow::{
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};

pub trait RowValuable {
    fn row(&self, index: usize) -> Result<TableRow, ArrowScalarError>;
    fn column_value(&self, column: &str, index: usize) -> Result<TableScalar, ArrowScalarError>;
}

impl RowValuable for RecordBatch {
    fn row(&self, index: usize) -> Result<TableRow, ArrowScalarError> {
        let mut row = TableRow::default();

        let schema = self.schema();
        for i in 0..self.num_columns() {
            let column = self.column(i).scalar(index)?;
            let name = schema.field(i).name();
            row.values.insert(name.to_string(), column.clone());
        }
        Ok(row)
    }
    fn column_value(&self, column: &str, index: usize) -> Result<TableScalar, ArrowScalarError> {
        if let Some((column_index, _field)) = self.schema().column_with_name(column) {
            self.column(column_index).scalar(index)
        } else {
            Err(ArrowScalarError::AccessError)
        }
    }
}

impl RowValuable for Table {
    fn row(&self, index: usize) -> Result<TableRow, ArrowScalarError> {
        let values = self
            .values
            .iter()
            .map(|(name, column)| Ok((name.to_owned(), column.scalar(index)?)))
            .collect::<Result<HashMap<String, TableScalar>, ArrowScalarError>>()?;
        Ok(TableRow { values })
    }

    fn column_value(&self, column: &str, index: usize) -> Result<TableScalar, ArrowScalarError> {
        self.values
            .get(column)
            .and_then(|column| Some(column.scalar(index)))
            .unwrap_or(Err(ArrowScalarError::AccessError))
    }
}

impl Table {
    pub fn new(schema: &Schema) -> Result<Self, ArrowScalarError> {
        let values = schema
            .fields()
            .iter()
            .map(|field| Ok((field.name().to_owned(), TableList::new(field.data_type())?)))
            .collect::<Result<HashMap<String, TableList>, ArrowScalarError>>()?;
        Ok(Table { values })
    }

    pub fn schema(&self) -> Result<Schema, ArrowScalarError> {
        let fields = self
            .values
            .iter()
            .map(|(name, column)| Ok(Field::new(name, column.data_type()?, true)))
            .collect::<Result<Vec<Field>, ArrowScalarError>>()?;
        Ok(Schema::new(fields))
    }

    pub fn len(&self) -> usize {
        self.values
            .iter()
            .next()
            .map(|(_, column)| column.len())
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn to_arrow(&self) -> Result<RecordBatch, ArrowScalarError> {
        let columns = self
            .values
            .iter()
            .map(|(_name, column)| column.to_array())
            .collect::<Result<Vec<_>, ArrowScalarError>>()?;
        let schema = Arc::new(self.schema()?);
        RecordBatch::try_new(schema, columns).map_err(|err| ArrowScalarError::ArrowError(err))
    }

    pub fn from_arrow(records: &RecordBatch) -> Result<Self, ArrowScalarError> {
        let schema = records.schema();
        let values = schema
            .fields()
            .iter()
            .zip(records.columns())
            .map(|(field, column)| Ok((field.name().to_owned(), column.clone_as_list()?)))
            .collect::<Result<HashMap<_, _>, ArrowScalarError>>()?;
        Ok(Self { values })
    }

    pub fn column(&self, key: &str) -> Option<&TableList> {
        self.values
            .get(key)
            .map(|column| column)
    }

    fn roll_back(&mut self, elements: Vec<String>, mut row: TableRow) -> TableRow {
        for element in elements {
            if let Some(value) = self
                .values
                .get_mut(&element)
                .and_then(|values| values.pop())
            {
                row.values.insert(element.to_string(), value);
            }
        }
        row
    }
    // todo test this
    pub fn push(&mut self, mut row: TableRow) -> Result<(), TableRow> {
        let mut roll_back: Vec<String> = Vec::with_capacity(self.values.len());
        for (key, column) in self.values.iter_mut() {
            if let Some(value) = row.values.remove(key) {
                if let Err(ArrowScalarError::InvalidScalar(value)) = column.push(value) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_table() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]);
        let mut table = Table::new(&schema).unwrap();
        table.push(TableRow {
            values: vec![("a".to_string(), TableScalar::int32(1)), ("b".to_string(), TableScalar::int32(2))]
                .into_iter()
                .collect(),
        }).unwrap();
        table.push(TableRow {
            values: vec![("a".to_string(), TableScalar::int32(3)), ("b".to_string(), TableScalar::int32(4))]
                .into_iter()
                .collect(),
        }).unwrap();
        
        
        let batch = table.to_arrow().unwrap();
        let new_table = Table::from_arrow(&batch).unwrap();
        assert_eq!(table.len(), 2);
        assert_eq!(table.column("a").unwrap().len(), 2);
        assert_eq!(table.column("b").unwrap().len(), 2);
        assert_eq!(table, new_table);
        
    }
}