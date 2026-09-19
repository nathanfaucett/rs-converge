use alloc::vec::Vec;

use value::{FromRow, FromRowError};

use crate::QueryResult;

impl QueryResult {
    pub fn rows_as<T>(&self) -> Result<Vec<T>, FromRowError>
    where
        T: FromRow,
    {
        let columns = self
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        self.rows
            .iter()
            .map(|row| T::from_row(row, &columns))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use alloc::{string::String, vec};
    use value::{FromRow, FromRowError, Row, Value};

    use crate::{QueryResult, QueryResultColumn};

    #[derive(Debug, PartialEq)]
    struct User {
        id: i64,
    }

    impl FromRow for User {
        fn from_row(row: &Row, columns: &[&str]) -> Result<Self, FromRowError> {
            Ok(Self {
                id: value::decode(value::value(row, columns, "id")?, "id")?,
            })
        }
    }

    #[test]
    fn maps_rows_using_column_names() {
        let result = QueryResult::new_with_columns(
            vec![Row::new(vec![Value::Integer(1)])],
            vec![QueryResultColumn {
                name: String::from("id"),
                source_table: None,
                source_column: None,
            }],
        );
        assert_eq!(result.rows_as::<User>(), Ok(vec![User { id: 1 }]));
    }
}
