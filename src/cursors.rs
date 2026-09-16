/// Represents a snapshot of the [`FixedBuffer`] indices, loaded atomically to ensure consistent CPU-side ordering.
///
/// When working with a [`FixedBuffer`], we often need to synchronize the loading of index values during a single call. This ensures that the CPU maintains a consistent and sequential order of operations.
///
/// ### The Problem
/// In practice, checking indices or retrieving values requires repetitive code. Because these index operations are scattered across various methods, the implementation quickly becomes verbose and difficult to maintain.
///
/// To address this, I introduced a dedicated struct. By loading all necessary index information at once, we eliminate the need for repeated, fragmented lookups.
#[derive(Clone, Copy)]
pub struct CursorData
{
    pub stolen:   u32,
    pub blocked:  u32,
    pub tail:     u32,
    pub capacity: u32,
    pub mask:     u32,
}

impl CursorData
{
    #[inline]
    pub fn empty_slots(&self) -> usize
    {
        self.capacity.wrapping_sub(self.tail).wrapping_add(self.stolen) as usize
    }

    #[inline]
    pub fn filled_slots(&self) -> usize
    {
        self.tail.wrapping_sub(self.blocked) as usize
    }
}
