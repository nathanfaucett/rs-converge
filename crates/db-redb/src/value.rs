use std::any::type_name;

#[derive(Clone, Debug)]
pub struct Value<K>(K);

impl<K> Value<K> {
    pub fn new(value: K) -> Self {
        Self(value)
    }

    pub fn into_inner(self) -> K {
        self.0
    }
}

impl<K> redb::Value for Value<K>
where
    K: redb::Value + 'static,
    for<'a> K: redb::Value<SelfType<'a> = K>,
{
    type SelfType<'a>
        = Value<K::SelfType<'a>>
    where
        Self: 'a;
    type AsBytes<'a>
        = K::AsBytes<'a>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        K::fixed_width()
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Value(K::from_bytes(data))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        K::as_bytes(&value.0)
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new(type_name::<Self>())
    }
}
