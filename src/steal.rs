#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Steal<T>
{
    Empty,
    Busy,
    Success(T),
}

impl<T> Steal<T>
{
    #[inline]
    pub fn is_empty(&self) -> bool
    {
        matches!(self, Self::Empty)
    }

    #[inline]
    pub fn is_busy(&self) -> bool
    {
        matches!(self, Self::Busy)
    }

    #[inline]
    pub fn is_success(&self) -> bool
    {
        matches!(self, Self::Success(_))
    }

    #[inline]
    pub fn success(self) -> Option<T>
    {
        match self
        {
            Self::Success(val) => Some(val),
            _ => None,
        }
    }

    #[inline]
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Steal<U>
    {
        match self
        {
            Self::Success(val) => Steal::Success(f(val)),
            Self::Empty => Steal::Empty,
            Self::Busy => Steal::Busy,
        }
    }

    #[inline]
    pub fn or_else(self, f: impl FnOnce() -> Steal<T>) -> Steal<T>
    {
        match self
        {
            Self::Success(val) => Self::Success(val),
            Self::Empty => f(),
            Self::Busy => match f()
            {
                Steal::Empty => Self::Busy,
                other => other,
            },
        }
    }
}
