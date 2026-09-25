/// Generates one `#[test]` fn running a whole [`TestSuite`] on a runner.
///
/// `$suite` and `$runner` are expressions evaluated inside the generated test;
/// leading attributes (e.g. `#[ignore = "..."]`) are forwarded to the fn.
#[macro_export]
macro_rules! test_suite {
    ($(#[ $attr:meta ])* $name:ident, $suite:expr, $runner:expr) => {
        #[test]
        $(#[$attr])*
        fn $name() {
            $crate::run(async {
                let suite = $suite;
                let runner = $runner;
                $crate::TestRunner::run_suite(&runner, &suite).await.unwrap();
            });
        }
    };
}

/// Generates one `#[test]` fn running a single [`TestCase`] on a runner.
///
/// `$case` and `$runner` are expressions evaluated inside the generated test;
/// leading attributes (e.g. `#[ignore = "..."]`) are forwarded to the fn.
#[macro_export]
macro_rules! test_case {
    ($(#[ $attr:meta ])* $name:ident, $case:expr, $runner:expr) => {
        #[test]
        $(#[$attr])*
        fn $name() {
            $crate::run(async {
                let case = $case;
                let runner = $runner;
                $crate::TestRunner::run_case(&runner, &case).await.unwrap();
            });
        }
    };
}
