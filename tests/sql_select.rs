use ofdb::{FromRow, Row, Uuid, Value};

#[derive(ofdb::FromRow, Debug, PartialEq)]
struct User {
    id: Uuid,
    name: String,
}

#[test]
fn derive_from_row_uses_the_db_facade() {
    let id = Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").unwrap();
    let user = User::from_row(
        &Row::new(vec![Value::Uuid(id), Value::from("Ada")]),
        &["id", "name"],
    )
    .unwrap();
    assert_eq!(
        user,
        User {
            id,
            name: "Ada".into()
        }
    );
}
