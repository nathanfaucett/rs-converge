use std::{borrow::Cow, collections::BTreeSet};

use value::Row;

/// Logical node identifier within a test scenario.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub usize);

impl From<usize> for NodeId {
    fn from(id: usize) -> Self {
        Self(id)
    }
}

/// Expected error category for negative test steps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpectedError {
    ConstraintViolation,
    SyntaxError,
    TableNotFound,
    ColumnNotFound,
    TypeMismatch,
}

impl ExpectedError {
    pub fn matches(&self, error: &engine::EngineError) -> bool {
        let msg = error.to_string().to_lowercase();
        match self {
            Self::SyntaxError => {
                matches!(
                    error,
                    engine::EngineError::TranslateError(_) | engine::EngineError::Unsupported(_)
                ) || msg.contains("syntax")
                    || msg.contains("expected")
                    || msg.contains("translate error")
                    || msg.contains("unsupported")
                    || msg.contains("invalid query: unsupported")
            }
            Self::ConstraintViolation => {
                msg.contains("unique")
                    || msg.contains("already exists")
                    || msg.contains("duplicate")
                    || msg.contains("tombstoned")
                    || msg.contains("constraint")
                    || msg.contains("primary key was deleted")
                    || msg.contains("cannot update the primary key")
                    || msg.contains("cannot resolve the primary key")
            }
            Self::TableNotFound => {
                msg.contains("table not found") || msg.contains("unknown column table")
            }
            Self::ColumnNotFound => {
                msg.contains("column not found")
                    || msg.contains("unknown insert column")
                    || msg.contains("unknown returning column")
                    || msg.contains("unknown resolution column")
                    || msg.contains("unknown projection column")
                    || msg.contains("index column not found")
            }
            Self::TypeMismatch => {
                msg.contains("must be a uuid")
                    || msg.contains("type mismatch")
                    || msg.contains("wrong column count")
                    || msg.contains("column/value count mismatch")
                    || msg.contains("invalid type")
                    || msg.contains("invalid uuid")
                    || msg.contains("cannot cast")
                    || msg.contains("too many columns")
            }
        }
    }
}

/// A pure data representation of a mutation or step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step {
    pub node: NodeId,
    pub sql: Cow<'static, str>,
    pub expected_error: Option<ExpectedError>,
}

impl Step {
    pub fn sql(node: impl Into<NodeId>, sql: impl Into<Cow<'static, str>>) -> Self {
        Self {
            node: node.into(),
            sql: sql.into(),
            expected_error: None,
        }
    }

    pub fn failing(
        node: impl Into<NodeId>,
        sql: impl Into<Cow<'static, str>>,
        expected_error: ExpectedError,
    ) -> Self {
        Self {
            node: node.into(),
            sql: sql.into(),
            expected_error: Some(expected_error),
        }
    }
}

/// An expected result evaluated against the final converged state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Expectation {
    pub query: Cow<'static, str>,
    pub expected_rows: Vec<Row>,
    /// When None, verified against all active nodes in the cluster.
    pub target_node: Option<NodeId>,
}

impl Expectation {
    pub fn all(query: impl Into<Cow<'static, str>>, expected_rows: Vec<Row>) -> Self {
        Self {
            query: query.into(),
            expected_rows,
            target_node: None,
        }
    }

    pub fn node(
        node: impl Into<NodeId>,
        query: impl Into<Cow<'static, str>>,
        expected_rows: Vec<Row>,
    ) -> Self {
        Self {
            query: query.into(),
            expected_rows,
            target_node: Some(node.into()),
        }
    }
}

/// A complete, self-contained, runner-agnostic test scenario.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestCase {
    pub name: &'static str,
    pub setup: Vec<Cow<'static, str>>,
    pub steps: Vec<Step>,
    pub expectations: Vec<Expectation>,
}

impl TestCase {
    pub fn builder(name: &'static str) -> TestCaseBuilder {
        TestCaseBuilder::new(name)
    }

    pub fn required_nodes(&self) -> usize {
        let mut nodes = BTreeSet::new();
        nodes.insert(NodeId(0));
        for step in &self.steps {
            nodes.insert(step.node);
        }
        for exp in &self.expectations {
            if let Some(node) = exp.target_node {
                nodes.insert(node);
            }
        }
        nodes.len()
    }
}

#[derive(Clone, Debug)]
pub struct TestCaseBuilder {
    name: &'static str,
    setup: Vec<Cow<'static, str>>,
    steps: Vec<Step>,
    expectations: Vec<Expectation>,
}

impl TestCaseBuilder {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            setup: Vec::new(),
            steps: Vec::new(),
            expectations: Vec::new(),
        }
    }

    pub fn setup<I, S>(mut self, statements: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Cow<'static, str>>,
    {
        self.setup.extend(statements.into_iter().map(Into::into));
        self
    }

    pub fn step(mut self, node: impl Into<NodeId>, sql: impl Into<Cow<'static, str>>) -> Self {
        self.steps.push(Step::sql(node, sql));
        self
    }

    pub fn step_failing(
        mut self,
        node: impl Into<NodeId>,
        sql: impl Into<Cow<'static, str>>,
        expected_error: ExpectedError,
    ) -> Self {
        self.steps.push(Step::failing(node, sql, expected_error));
        self
    }

    pub fn step_root(self, sql: impl Into<Cow<'static, str>>) -> Self {
        self.step(0, sql)
    }

    pub fn expect_query(
        mut self,
        query: impl Into<Cow<'static, str>>,
        expected_rows: Vec<Row>,
    ) -> Self {
        self.expectations
            .push(Expectation::all(query, expected_rows));
        self
    }

    pub fn expect_node_query(
        mut self,
        node: impl Into<NodeId>,
        query: impl Into<Cow<'static, str>>,
        expected_rows: Vec<Row>,
    ) -> Self {
        self.expectations
            .push(Expectation::node(node, query, expected_rows));
        self
    }

    pub fn build(self) -> TestCase {
        TestCase {
            name: self.name,
            setup: self.setup,
            steps: self.steps,
            expectations: self.expectations,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TestSuite {
    pub name: &'static str,
    pub cases: Vec<TestCase>,
}

impl TestSuite {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            cases: Vec::new(),
        }
    }

    pub fn add(&mut self, case: TestCase) -> &mut Self {
        self.cases.push(case);
        self
    }

    pub fn with_case(mut self, case: TestCase) -> Self {
        self.cases.push(case);
        self
    }
}
