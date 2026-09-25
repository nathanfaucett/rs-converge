mod common;
#[path = "common/sql.rs"]
mod sql;

use common::{case_concurrent_user_inserts, standard_suite};
use ofdb_test::{SingleNodeRunner, test_case, test_suite};
use sql::{sql_limits_suite, sql_suite};

test_suite!(
    standard_local,
    standard_suite(),
    SingleNodeRunner::default()
);
test_suite!(sql_surface_local, sql_suite(), SingleNodeRunner::default());
test_suite!(
    sql_limits_local,
    sql_limits_suite(),
    SingleNodeRunner::default()
);
test_case!(
    concurrent_inserts_local,
    case_concurrent_user_inserts(),
    SingleNodeRunner::default()
);
