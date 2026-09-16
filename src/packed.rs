use std::fmt::{self, Debug, Display};

use crate::bits::{pack, unpack};
use crate::cache_padded::CachePadded;
use crate::sync::{AtomicU64, Ordering};

pub struct Packed
{
    packed: CachePadded<AtomicU64>,
}
impl Packed
{
    #[cfg(not(loom))]
    pub const fn new(a: u32, b: u32) -> Self
    {
        Self {
            packed: CachePadded::new(AtomicU64::new(pack(a, b))),
        }
    }

    #[cfg(loom)]
    pub fn new(a: u32, b: u32) -> Self
    {
        Self {
            packed: CachePadded::new(AtomicU64::new(pack(a, b))),
        }
    }
    #[inline]
    pub fn load_unpack(&self, order: Ordering) -> (u32, u32)
    {
        unpack(self.packed.load(order))
    }

    #[inline]
    pub fn store_pack(&self, val: (u32, u32), order: Ordering)
    {
        self.packed.store(pack(val.0, val.1), order);
    }
    #[inline]
    pub fn load(&self, order: Ordering) -> u64
    {
        self.packed.load(order)
    }
    #[inline]
    pub fn store(&self, val: u64, order: Ordering)
    {
        self.packed.store(val, order);
    }

    /// Performs an atomic addition on a 64-bit memory location.
    /// If you want to increment only the high 32 bits, pass `1 << 32`. Any carry will overflow out of
    /// the u64 entirely rather than bleeding into the low 32 bits. Incrementing the low bits works differently,
    /// as an overflow will spill into the high bits, so this method is intended only for the high 32 bits.
    #[inline]
    pub fn fetch_add(&self, val: u64, order: Ordering) -> u64
    {
        self.packed.fetch_add(val, order)
    }

    #[inline]
    pub fn compare_exchange_weak(&self, current: u64, new: u64, success: Ordering, fail: Ordering) -> Result<u64, u64>
    {
        self.packed.compare_exchange_weak(current, new, success, fail)
    }

    #[inline]
    pub fn compare_exchange(&self, current: u64, new: u64, success: Ordering, fail: Ordering) -> Result<u64, u64>
    {
        self.packed.compare_exchange(current, new, success, fail)
    }
}

impl Debug for Packed
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result
    {
        let (stolen, in_progress) = self.load_unpack(Ordering::Relaxed);
        f.debug_struct("Packed").field("a", &stolen).field("b", &in_progress).finish()
    }
}
impl Display for Packed
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result
    {
        let (stolen, in_progress) = self.load_unpack(Ordering::Relaxed);
        write!(f, "Packed(a: {stolen}, b: {in_progress})")
    }
}
