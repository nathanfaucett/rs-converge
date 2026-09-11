use std::{marker::PhantomData, ops::RangeBounds, sync::Arc};

use async_stream::stream;
use futures::Stream;
use redb::{Database, ReadableDatabase};

use db_btree::{BTree, BTreeError, BTreeRead, BTreeResult};

use crate::{
    RedbBTreeTransaction,
    key::Key,
    redb::{RedbKey, RedbValue, table_definition},
};

#[derive(Clone)]
pub struct RedbBTree<K, V> {
    db: Arc<Database>,
    name: String,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> RedbBTree<K, V> {
    pub fn new(db: Arc<Database>, name: impl Into<String>) -> Self {
        Self {
            db,
            name: name.into(),
            _marker: PhantomData,
        }
    }
}

impl<K, V> BTreeRead<K, V> for RedbBTree<K, V>
where
    K: RedbKey,
    V: RedbValue,
{
    async fn get(&self, key: &K) -> BTreeResult<Option<V>> {
        let db = self.db.begin_read().map_err(BTreeError::custom)?;

        let table = db
            .open_table(table_definition::<K, V>(&self.name))
            .map_err(BTreeError::custom)?;

        let guard = match table
            .get(Key::new(key.clone()))
            .map_err(BTreeError::custom)?
        {
            Some(value) => value,
            None => return Ok(None),
        };

        let value: V = guard.value().into_inner();

        Ok(Some(value))
    }

    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>>
    where
        R: RangeBounds<K>,
    {
        stream!({
            let db = self.db.begin_read().map_err(BTreeError::custom)?;
            let table = db
                .open_table(table_definition::<K, V>(&self.name))
                .map_err(BTreeError::custom)?;

            let mapped_range = Key::range(range);
            let results = table.range(mapped_range).map_err(BTreeError::custom)?;

            for result in results {
                let (guard_key, guard_value) = result.map_err(BTreeError::custom)?;
                let key: K = guard_key.value().into_inner();
                let value: V = guard_value.value().into_inner();
                yield Ok((key, value));
            }
        })
    }
}

impl<K, V> BTree<K, V> for RedbBTree<K, V>
where
    K: RedbKey,
    V: RedbValue,
{
    type Transaction = RedbBTreeTransaction<K, V>;

    async fn transaction(&self) -> BTreeResult<Self::Transaction> {
        let tx = self.db.begin_write().map_err(BTreeError::custom)?;

        Ok(RedbBTreeTransaction::new(tx, &self.name))
    }
}

#[cfg(test)]
mod test {
    use futures::{StreamExt, executor::block_on};

    use db_btree::{BTree, BTreeRead, BTreeTransaction};

    use super::*;
    use crate::RedbDatabase;

    fn tmp_path() -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let name = format!("test-{}", uuid::Uuid::now_v7());
        path.push(format!("db-redb-{}.db", name));
        path
    }

    fn create_tree<K, V>(table_name: &str) -> RedbBTree<K, V>
    where
        K: RedbKey,
        V: RedbValue,
    {
        let db = Database::create(tmp_path()).expect("failed to create database");
        let tx = db.begin_write().expect("failed to begin transaction");
        tx.open_table(table_definition::<K, V>(table_name))
            .expect("failed to create table");
        tx.commit().expect("failed to commit transaction");
        RedbBTree::<K, V>::new(Arc::new(db), table_name)
    }

    #[test]
    fn it_works() {
        block_on(async {
            let tree = create_tree::<String, String>("test_tree");

            let mut tx = tree
                .transaction()
                .await
                .expect("failed to create transaction");
            tx.insert("key1".to_string(), "value1".to_string())
                .await
                .expect("failed to insert");
            tx.commit().await.expect("failed to commit");

            let got = tree
                .get(&"key1".to_string())
                .await
                .expect("failed to get")
                .expect("missing key");

            assert_eq!(got, "value1".to_string());
        });
    }

    #[test]
    fn insert_and_get() {
        block_on(async {
            let tree = create_tree("ctx_tree");

            let mut tx = tree
                .transaction()
                .await
                .expect("failed to create transaction");
            tx.insert("foo".to_string(), "bar".to_string())
                .await
                .expect("insert failed");
            tx.commit().await.expect("commit failed");

            let value = tree
                .get(&"foo".to_string())
                .await
                .expect("get failed")
                .expect("key missing");
            assert_eq!(value, "bar".to_string());
        });
    }

    #[test]
    fn insert_multiple_keys() {
        block_on(async {
            let tree = create_tree("ctx_nums");

            let mut tx = tree.transaction().await.expect("fail transaction");
            tx.insert(1u64, "1".to_string())
                .await
                .expect("insert 1 failed");
            tx.insert(2u64, "2".to_string())
                .await
                .expect("insert 2 failed");
            tx.insert(3u64, "3".to_string())
                .await
                .expect("insert 3 failed");
            tx.commit().await.expect("commit failed");

            for key in [1u64, 2u64, 3u64] {
                let value = tree
                    .get(&key)
                    .await
                    .expect("get failed")
                    .expect("key missing");
                assert_eq!(value, format!("{}", key), "wrong value for key {}", key);
            }
        });
    }

    #[test]
    fn get_nonexistent_key() {
        block_on(async {
            let tree = create_tree::<String, String>("ctx_empty");

            let result = tree
                .get(&"nonexistent".to_string())
                .await
                .expect("get failed");
            assert!(result.is_none(), "should return none for missing key");
        });
    }

    #[test]
    fn range_query() {
        block_on(async {
            let tree = create_tree("ctx_range");

            // Insert keys 1-5
            let mut tx = tree.transaction().await.expect("fail transaction");
            for i in 1..=5 {
                tx.insert(i, format!("value_{}", i).to_string())
                    .await
                    .expect("insert failed");
            }
            tx.commit().await.expect("commit failed");

            let mut count = 0;
            let results = tree.range(2u64..=4u64).collect::<Vec<_>>().await;
            for result in results {
                let (k, v) = result.expect("range error");
                assert_eq!(k, count + 2); // Should be in order: 2, 3, 4
                assert_eq!(v, format!("value_{}", k));
                count += 1;
            }
            assert_eq!(count, 3, "should have retrieved 3 items");
        });
    }

    #[test]
    fn insert_overwrite() {
        block_on(async {
            let tree = create_tree::<String, String>("ctx_override");

            // Insert same key multiple times (should overwrite)
            let mut tx = tree.transaction().await.expect("fail transaction");
            tx.insert("key".to_string(), "first".to_string())
                .await
                .expect("insert 1 failed");
            tx.insert("key".to_string(), "second".to_string())
                .await
                .expect("insert 2 failed");
            tx.insert("key".to_string(), "third".to_string())
                .await
                .expect("insert 3 failed");
            tx.commit().await.expect("commit failed");

            // Should only have the last value
            let value = tree
                .get(&"key".to_string())
                .await
                .expect("get failed")
                .expect("key missing");
            assert_eq!(value, "third", "should overwrite previous values");
        });
    }

    #[test]
    fn database_transaction_commits_multiple_tables_once() {
        block_on(async {
            let db = Arc::new(Database::create(tmp_path()).expect("failed to create database"));
            let database = RedbDatabase::new(db.clone());

            let transaction = database.transaction().expect("failed to begin transaction");
            let mut first = transaction.table::<String, String>("first");
            first
                .insert("key".into(), "first".into())
                .await
                .expect("failed to insert first value");
            first.commit().await.expect("scoped commit failed");
            let mut second = transaction.table::<String, String>("second");
            second
                .insert("key".into(), "second".into())
                .await
                .expect("failed to insert second value");
            second.commit().await.expect("scoped commit failed");
            transaction
                .commit()
                .expect("failed to commit database transaction");

            let first = RedbBTree::<String, String>::new(db.clone(), "first");
            let second = RedbBTree::<String, String>::new(db, "second");
            assert_eq!(
                first.get(&"key".into()).await.expect("get failed"),
                Some("first".into())
            );
            assert_eq!(
                second.get(&"key".into()).await.expect("get failed"),
                Some("second".into())
            );
        });
    }

    #[test]
    fn range_empty() {
        block_on(async {
            let tree = create_tree::<String, String>("ctx_empty_range");

            let results = tree.range(..).collect::<Vec<_>>().await;

            for result in results {
                let (_, _) = result.expect("range error");
                unreachable!("should not have items");
            }
        });
    }
}
