extern crate value as db;

use macros::FromRow;
use uuid::Uuid;
use value::{FromRow as _, Row, Value};

#[derive(Debug, PartialEq, FromRow)]
struct User {
    id: Uuid,
    #[db(column = "display_name")]
    name: Option<String>,
}

#[test]
fn derives_from_named_columns() {
    let id = Uuid::nil();
    let user = User::from_row(
        &Row::new(vec![Value::Null, Value::Uuid(id)]),
        &["display_name", "id"],
    )
    .unwrap();
    assert_eq!(user, User { id, name: None });
}
