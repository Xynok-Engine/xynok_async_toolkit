/// xorshift64. No `rand` dependency, and the same seed replays the same pressure pattern, so a
/// failure is at least as reproducible as a race can be.
/// https://docs.rs/alazar/latest/alazar/xorshift/struct.XorShift64.html
/// https://github.com/WebDrake/xorshift/blob/master/xorshift.c
pub struct Random(u64);
impl Default for Random
{
    fn default() -> Self
    {
        Self::new(0x5EED_1234)
    }
}
impl Random
{
    #[inline]
    pub fn new(seed: u64) -> Self
    {
        // The `| 1` keeps the state away from 0, which xorshift can never leave.
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    #[inline]
    pub fn next_val(&mut self) -> u64
    {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// return a value from range `[0, n - 1]`. Ex:
    /// `below(10)` -> `0...9`
    #[inline]
    pub fn below(&mut self, n: u64) -> u64
    {
        self.next_val() % n.max(1)
    }
}
