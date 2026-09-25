mod common;

use common::{case_concurrent_user_inserts, standard_suite};
use ofdb_test::{ChaosRunner, test_case, test_suite};

test_suite!(
    #[ignore = "chaos testing is intended for chaos CI workflow"]
    standard_chaos,
    standard_suite(),
    ChaosRunner::default()
);

test_case!(
    #[ignore = "chaos testing is intended for chaos CI workflow"]
    concurrent_inserts_chaos,
    case_concurrent_user_inserts(),
    ChaosRunner::default()
);
