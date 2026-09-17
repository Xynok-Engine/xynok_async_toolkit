#[cfg(target_has_atomic = "64")]
use sync::AtomicU64;
use sync::{AtomicU16, AtomicU32, AtomicU8};

mod sync;
pub mod backoff;
pub mod bits;
pub mod cache_padded;
pub mod cursors;
pub mod fixed_ring_buffer;
pub mod fixed_buffer;
pub mod packed;
pub mod queue_batching;
pub mod slots;
pub mod spinlock;
pub mod swmr_spinlock;
pub mod mwmr_spinlock;
pub mod steal;
pub mod random;
pub mod latch;

#[inline]
pub fn ignore_poison<G>(result: std::sync::LockResult<G>) -> G
{
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[inline]
pub fn available_cores() -> usize
{
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
}

/// Checks if `T` is small and aligned enough for the CPU to perform atomic read and write operations.
///
/// If this returns `false`, you'll need to use a lock for cross-thread access because the hardware cannot
/// handle the operation atomically on its own.
pub const fn is_lock_free<T>() -> bool
{
    let lock_free = is_zero_sized::<T>() || can_transmute::<T, AtomicU8>() || can_transmute::<T, AtomicU16>() || can_transmute::<T, AtomicU32>();

    #[cfg(target_has_atomic = "64")]
    let lock_free = lock_free || can_transmute::<T, AtomicU64>();

    lock_free
}

const fn can_transmute<A, B>() -> bool
{
    size_of::<A>() == size_of::<B>() && align_of::<A>() >= align_of::<B>()
}

const fn is_zero_sized<T>() -> bool
{
    size_of::<T>() == 0
}
