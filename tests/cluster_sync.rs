mod common;

use common::{case_concurrent_user_inserts, standard_suite};
use ofdb_test::{ClusterOfflineRunner, ClusterRealtimeRunner, test_case, test_suite};

test_suite!(
    standard_offline,
    standard_suite(),
    ClusterOfflineRunner::default()
);
test_suite!(
    standard_realtime,
    standard_suite(),
    ClusterRealtimeRunner::default()
);
test_case!(
    concurrent_inserts_offline,
    case_concurrent_user_inserts(),
    ClusterOfflineRunner::default()
);
test_case!(
    concurrent_inserts_realtime,
    case_concurrent_user_inserts(),
    ClusterRealtimeRunner::default()
);
