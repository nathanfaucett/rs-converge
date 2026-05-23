use core::future::Future;

use crate::EngineError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransactionState {
  Uninitialized,
  Active,
  Committed,
  RolledBack,
}

#[derive(Debug)]
pub(crate) struct TransactionLifecycle<TX> {
  state: TransactionState,
  tx: Option<TX>,
}

impl<TX> TransactionLifecycle<TX> {
  pub(crate) fn new() -> Self {
    Self {
      state: TransactionState::Uninitialized,
      tx: None,
    }
  }

  #[cfg(test)]
  pub(crate) fn state(&self) -> TransactionState {
    self.state
  }

  pub(crate) async fn transaction<F, Fut>(&mut self, begin: F) -> Result<&mut TX, EngineError>
  where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<TX, EngineError>>,
  {
    match self.state {
      TransactionState::Uninitialized => {
        let tx = begin().await?;
        self.tx = Some(tx);
        self.state = TransactionState::Active;
      }
      TransactionState::Active => {}
      TransactionState::Committed | TransactionState::RolledBack => {
        return Err(EngineError::SchemaMismatch(
          "transaction already finalized".into(),
        ));
      }
    }

    self
      .tx
      .as_mut()
      .ok_or_else(|| EngineError::SchemaMismatch("transaction missing while active".into()))
  }

  pub(crate) fn take_for_commit(&mut self) -> Option<TX> {
    if self.tx.is_some() {
      self.state = TransactionState::Committed;
    }
    self.tx.take()
  }

  pub(crate) fn take_for_rollback(&mut self) -> Option<TX> {
    if self.tx.is_some() {
      self.state = TransactionState::RolledBack;
    }
    self.tx.take()
  }
}

#[cfg(test)]
mod tests {
  use futures::executor::block_on;

  use super::*;

  #[test]
  fn starts_uninitialized() {
    let lifecycle: TransactionLifecycle<u8> = TransactionLifecycle::new();
    assert_eq!(lifecycle.state(), TransactionState::Uninitialized);
  }

  #[test]
  fn commit_without_active_keeps_uninitialized_state() {
    let mut lifecycle: TransactionLifecycle<u8> = TransactionLifecycle::new();
    assert!(lifecycle.take_for_commit().is_none());
    assert_eq!(lifecycle.state(), TransactionState::Uninitialized);
  }

  #[test]
  fn rollback_without_active_keeps_uninitialized_state() {
    let mut lifecycle: TransactionLifecycle<u8> = TransactionLifecycle::new();
    assert!(lifecycle.take_for_rollback().is_none());
    assert_eq!(lifecycle.state(), TransactionState::Uninitialized);
  }

  #[test]
  fn begin_transitions_to_active_and_returns_mutable_tx() {
    block_on(async {
      let mut lifecycle: TransactionLifecycle<u8> = TransactionLifecycle::new();

      let tx = lifecycle
        .transaction(|| async { Ok(7u8) })
        .await
        .expect("begin transaction");
      *tx = 9;

      assert_eq!(lifecycle.state(), TransactionState::Active);
      assert_eq!(lifecycle.take_for_commit(), Some(9));
      assert_eq!(lifecycle.state(), TransactionState::Committed);
    });
  }

  #[test]
  fn rollback_after_active_transitions_to_rolled_back() {
    block_on(async {
      let mut lifecycle: TransactionLifecycle<u8> = TransactionLifecycle::new();

      lifecycle
        .transaction(|| async { Ok(3u8) })
        .await
        .expect("begin transaction");
      assert_eq!(lifecycle.state(), TransactionState::Active);

      assert_eq!(lifecycle.take_for_rollback(), Some(3));
      assert_eq!(lifecycle.state(), TransactionState::RolledBack);
    });
  }

  #[test]
  fn transaction_after_commit_returns_finalized_error() {
    block_on(async {
      let mut lifecycle: TransactionLifecycle<u8> = TransactionLifecycle::new();

      lifecycle
        .transaction(|| async { Ok(1u8) })
        .await
        .expect("begin transaction");
      let _ = lifecycle.take_for_commit();

      let error = lifecycle
        .transaction(|| async { Ok(2u8) })
        .await
        .expect_err("transaction should be finalized");

      assert!(
        matches!(error, EngineError::SchemaMismatch(message) if message.contains("already finalized"))
      );
      assert_eq!(lifecycle.state(), TransactionState::Committed);
    });
  }

  #[test]
  fn transaction_after_rollback_returns_finalized_error() {
    block_on(async {
      let mut lifecycle: TransactionLifecycle<u8> = TransactionLifecycle::new();

      lifecycle
        .transaction(|| async { Ok(1u8) })
        .await
        .expect("begin transaction");
      let _ = lifecycle.take_for_rollback();

      let error = lifecycle
        .transaction(|| async { Ok(2u8) })
        .await
        .expect_err("transaction should be finalized");

      assert!(
        matches!(error, EngineError::SchemaMismatch(message) if message.contains("already finalized"))
      );
      assert_eq!(lifecycle.state(), TransactionState::RolledBack);
    });
  }
}
