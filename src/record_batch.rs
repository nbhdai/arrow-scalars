use std::{collections::HashMap, sync::Arc};

use crate::{
    ArrowScalarError, ListValuable, ScalarValuable, Table, TableList, TableRow, TableScalar, FieldProto,
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
        .fields
        .iter().zip(self
            .values
            .iter())
            .map(|(name, column)| Ok((name.name.to_owned(), column.scalar(index)?)))
            .collect::<Result<HashMap<String, TableScalar>, ArrowScalarError>>()?;
        Ok(TableRow { values })
    }

    fn column_value(&self, column: &str, index: usize) -> Result<TableScalar, ArrowScalarError> {
        self.fields.iter().zip(self.values.iter()).find_map(|(field, value)| {
            if field.name == column {
                Some(value.scalar(index))
            } else {
                None
            }
        }).unwrap_or(Err(ArrowScalarError::AccessError))
    }
}

impl Table {
    pub fn new(schema: &Schema) -> Result<Self, ArrowScalarError> {
        let fields = schema.fields().iter().map(|field| FieldProto::from_arrow(field)).collect();
        let values = schema
            .fields()
            .iter()
            .map(|field| Ok(TableList::new(field.data_type())?))
            .collect::<Result<Vec<TableList>, ArrowScalarError>>()?;
        Ok(Table { fields, values })
    }

    pub fn schema(&self) -> Result<Schema, ArrowScalarError> {
        let fields = self
            .fields
            .iter()
            .map(|field| field.to_arrow())
            .collect::<Result<Vec<Field>, ArrowScalarError>>()?;
        Ok(Schema::new(fields))
    }

    pub fn column_by_name(&self, name: &str) -> Option<&TableList> {
        self.fields.iter().zip(self.values.iter()).find_map(|(field, value)| {
            if field.name == name {
                Some(value)
            } else {
                None
            }
        })
    }

    pub fn len(&self) -> usize {
        self.values
            .first()
            .map(|column| column.len())
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn to_arrow(&self) -> Result<RecordBatch, ArrowScalarError> {
        let columns = self
            .values
            .iter()
            .map(|column| column.to_array())
            .collect::<Result<Vec<_>, ArrowScalarError>>()?;
        let schema = Arc::new(self.schema()?);
        RecordBatch::try_new(schema, columns).map_err(|err| ArrowScalarError::ArrowError(err))
    }

    pub fn from_arrow(records: &RecordBatch) -> Result<Self, ArrowScalarError> {
        let schema = records.schema();
        let fields = schema.fields().iter().map(|field| FieldProto::from_arrow(field)).collect();
        let values = records.columns().iter()
            .map(|column| column.clone_as_list())
            .collect::<Result<Vec<_>, ArrowScalarError>>()?;
        Ok(Self { values, fields })
    }

    pub fn column(&self, i: usize) -> Option<&TableList> {
        self.values.get(i).map(|column| column)
    }

    fn roll_back(&mut self, index: usize, mut row: TableRow) -> TableRow {
        for i in 0..index {
            row.values.insert(self.fields[i].name.to_owned(), self.values[i].pop().unwrap());
        }
        row
    }
    // todo test this
    pub fn push(&mut self, mut row: TableRow) -> Result<(), TableRow> {
        
        for (i,(field, values)) in self.fields.iter().zip(self.values.iter_mut()).enumerate() {
            if let Some(value) = row.values.remove(&field.name) {
                if let Err(ArrowScalarError::InvalidScalar(scalar)) =  values.push(value) {
                    row.values.insert(field.name.to_owned(), scalar);
                    return Err(self.roll_back(i, row));
                }
            } else {
                return Err(self.roll_back(i, row));
            }
        }
        if row.values.is_empty() {
            Ok(())
        } else {
            Err(self.roll_back(self.fields.len(), row))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{
            ArrayRef, BooleanArray, Float32Array, Int32Array, Int64Array, StructArray,
            Time32SecondArray,
        },
        datatypes::{DataType, Field, Schema},
    };

    #[test]
    fn test_table() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]);
        let mut table = Table::new(&schema).unwrap();
        table
            .push(TableRow {
                values: vec![
                    ("a".to_string(), TableScalar::int32(1)),
                    ("b".to_string(), TableScalar::int32(2)),
                ]
                .into_iter()
                .collect(),
            })
            .unwrap();
        table
            .push(TableRow {
                values: vec![
                    ("a".to_string(), TableScalar::int32(3)),
                    ("b".to_string(), TableScalar::int32(4)),
                ]
                .into_iter()
                .collect(),
            })
            .unwrap();

        let batch = table.to_arrow().unwrap();
        let new_table = Table::from_arrow(&batch).unwrap();
        assert_eq!(table.len(), 2);
        assert_eq!(table.column(0).unwrap().len(), 2);
        assert_eq!(table.column(1).unwrap().len(), 2);
        assert_eq!(table, new_table);
    }

    pub fn test_record_batch() -> RecordBatch {
        let id_values = vec![Some(1), Some(2), None, Some(3), Some(4)];
        let id_array = Arc::new(Int64Array::from(id_values));

        let timestamps_values = vec![Some(3), Some(2), None, Some(1), Some(4)];
        let timestamps_array = Arc::new(Time32SecondArray::from(timestamps_values));

        let labels_values = vec![true, false, false, true, true];
        let labels_array = Arc::new(BooleanArray::from(labels_values));
        let prediction_values = vec![Some(0.1), Some(0.2), None, Some(0.3), Some(0.4)];
        let prediction_array = Arc::new(Float32Array::from(prediction_values));
        let binary_array = Arc::new(StructArray::from(vec![
            (
                Field::new("y", DataType::Boolean, true),
                labels_array as ArrayRef,
            ),
            (
                Field::new("pred", DataType::Float32, true),
                prediction_array as ArrayRef,
            ),
        ]));
        let extra_data = Arc::new(Int32Array::from(vec![10, 9, 8, 7, 6]));
        RecordBatch::try_from_iter(vec![
            ("model_results", binary_array as ArrayRef),
            ("id", id_array),
            ("timestamps", timestamps_array),
            ("bonus", extra_data),
        ])
        .unwrap()
    }

    #[test]
    fn test_rebuild() {
        let batch = test_record_batch();
        let table = Table::from_arrow(&batch).unwrap();
        let new_batch = table.to_arrow().unwrap();
        assert_eq!(batch, new_batch);
    }
}
