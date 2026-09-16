//! ## One shared front for std and loom
//!
//! This crate is checked with loom, and loom can only see the concurrent operations it provides
//! itself. That means every atomic, every shared cell, every thread operation has to switch over to
//! loom's version during the checks, then switch back for a real build.
//!
//! Sprinkling `#[cfg]` everywhere to do that would bury the real code in noise, and missing a
//! single spot means loom never sees it, so the run comes back clean while the bug is still there.
//!
//! ### How it works
//!
//! The whole crate pulls these types from here instead of straight from std. This is the one place
//! that decides whose version to use, and nothing else has to care.
//!
//! Only the things that genuinely need swapping live here. Anything loom does not model, or that is
//! only used to measure a type's size rather than run concurrently, can just call std directly.
//!
//! > [!IMPORTANT]
//! > Do not reach for std directly for anything on the list below. One shortcut is enough for loom
//! > to lose track of exactly the part that needed watching.

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering};

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicU8, AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering};

#[cfg(loom)]
pub(crate) use loom::cell::UnsafeCell;

/// A cell that lets several places hold a reference and still write through it.
///
/// The std version records nothing, while loom's version tracks every access so it can catch two
/// threads landing on the same cell. This wraps the std one in the same shape as loom's, so calling
/// code only ever writes against a single type.
#[cfg(not(loom))]
#[derive(Debug)]
pub struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(loom))]
impl<T> UnsafeCell<T>
{
    pub const fn new(value: T) -> Self
    {
        Self(std::cell::UnsafeCell::new(value))
    }

    #[inline]
    pub fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R
    {
        f(self.0.get())
    }

    #[inline]
    pub fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R
    {
        f(self.0.get())
    }
}

/// Hints to the CPU that this is a spin wait, so it takes less away from the sibling cores.
///
/// Under loom this becomes a yield, because loom needs a real cut point to try different orderings
/// and does not understand a hardware hint.
#[inline]
pub fn spin_loop()
{
    #[cfg(not(loom))]
    std::hint::spin_loop();

    #[cfg(loom)]
    loom::thread::yield_now();
}

pub mod thread
{
    #[cfg(not(loom))]
    pub use std::thread::{JoinHandle, Thread, current, park, yield_now};

    #[cfg(loom)]
    pub use loom::thread::{JoinHandle, Thread, current, park, yield_now};

    /// Spawns a worker and gives it a name, so a debugger or profiler shows who is who instead of a
    /// row of `Thread-<number>`.
    #[cfg(not(loom))]
    pub fn spawn_named<F>(name: String, f: F) -> std::io::Result<JoinHandle<()>>
    where F: FnOnce() + Send + 'static
    {
        std::thread::Builder::new().name(name).spawn(f)
    }

    /// A loom thread is a coroutine it schedules itself, with no name at the OS level, so the name
    /// is dropped. No real pool gets built during a loom run anyway.
    #[cfg(loom)]
    pub(crate) fn spawn_named<F>(_name: String, f: F) -> std::io::Result<JoinHandle<()>>
    where F: FnOnce() + Send + 'static
    {
        Ok(loom::thread::spawn(f))
    }

    /// Loom does not model the clock, so a timed park becomes a yield. The only place using it is
    /// the wait loop looking for work, and waking up early is always fine there.
    #[cfg(loom)]
    pub(crate) fn park_timeout(_dur: std::time::Duration)
    {
        loom::thread::yield_now();
    }
}
