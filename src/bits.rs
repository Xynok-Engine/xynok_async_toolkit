/// Packs two 32-bit numbers into a single 64-bit word.
///
/// `a` occupies the high bits, and `b` occupies the low bits. I pack them this way so that both
/// values are read or written in a single atomic operation, which saves us from worrying about
/// inconsistent loads.
#[inline]
pub const fn pack(a: u32, b: u32) -> u64
{
    (a as u64) << 32 | (b as u64)
}

/// Unpacks the pair of numbers previously combined by [`pack`], returning them in the original `(a, b)` order.
#[inline]
pub const fn unpack(val: u64) -> (u32, u32)
{
    ((val >> 32) as u32, val as u32)
}
