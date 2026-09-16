use crate::sync::spin_loop;
use crate::sync::thread::yield_now;

#[rustfmt::skip] #[cfg(not(any(loom, miri)))] pub const SPIN_LIMIT: u32 = 6; // maxmium is 2^6
#[rustfmt::skip] #[cfg(not(any(loom, miri)))] pub const YIELD_LIMIT: u32 = 10;

#[rustfmt::skip] #[cfg(any(loom, miri))] pub const SPIN_LIMIT: u32 = 0;
#[rustfmt::skip] #[cfg(any(loom, miri))] pub const YIELD_LIMIT: u32 = 1;

/// src: https://github.com/crossbeam-rs/crossbeam/blob/main/crossbeam-utils/src/backoff.rs
pub struct Backoff
{
    step: u32,
}
impl Backoff
{
    pub fn new() -> Self
    {
        Self { step: 0 }
    }

    #[inline]
    pub fn spin(&mut self)
    {
        for _ in 0..1 << self.step.min(SPIN_LIMIT)
        {
            spin_loop();
        }

        if self.step <= SPIN_LIMIT
        {
            self.step += 1;
        }
    }

    #[inline]
    pub fn snooze(&mut self)
    {
        match self.step <= SPIN_LIMIT
        {
            true =>
            {
                for _ in 0..1 << self.step
                {
                    spin_loop();
                }
            }
            false =>
            {
                yield_now();
            }
        }

        if self.step <= YIELD_LIMIT
        {
            self.step += 1;
        }
    }

    #[inline]
    pub fn is_completed(&self) -> bool
    {
        self.step > YIELD_LIMIT
    }
    #[inline]
    pub fn rounds(&self) -> u32
    {
        self.step
    }
    #[inline]
    pub fn reset(&mut self)
    {
        self.step = 0;
    }
}

impl std::fmt::Debug for Backoff
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("Backoff")
            .field("step", &self.step)
            .field("is_completed", &self.is_completed())
            .finish()
    }
}

impl Default for Backoff
{
    fn default() -> Self
    {
        Self::new()
    }
}
