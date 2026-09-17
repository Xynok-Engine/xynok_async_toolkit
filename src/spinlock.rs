use crate::backoff::Backoff;
use crate::cache_padded::CachePadded;
use crate::sync::{AtomicBool, Ordering, UnsafeCell};

pub struct SpinLock<T>
{
    val:    UnsafeCell<T>,
    locked: CachePadded<AtomicBool>,
}

unsafe impl<T: Send> Send for SpinLock<T> {}
unsafe impl<T: Send> Sync for SpinLock<T> {}

impl<T> SpinLock<T>
{
    #[cfg(not(loom))]
    pub const fn new(val: T) -> Self
    {
        Self {
            val:    UnsafeCell::new(val),
            locked: CachePadded::new(AtomicBool::new(false)),
        }
    }
    #[cfg(loom)]
    pub fn new(val: T) -> Self
    {
        Self {
            val:    UnsafeCell::new(val),
            locked: CachePadded::new(AtomicBool::new(false)),
        }
    }

    #[inline]
    pub fn get(&self) -> SpinGuard<'_, T>
    {
        if self.try_lock_weak()
        {
            return SpinGuard { lock: self };
        }
        self.cas_get()
    }

    #[inline]
    pub fn try_get(&self) -> Option<SpinGuard<'_, T>>
    {
        if self.try_lock()
        {
            return Some(SpinGuard { lock: self });
        }
        None
    }

    #[inline]
    pub fn get_mut(&mut self) -> &mut T
    {
        self.val.with_mut(|p| unsafe { &mut *p })
    }

    #[inline]
    pub fn take(self) -> T
    {
        let this = std::mem::ManuallyDrop::new(self);
        this.val.with_mut(|p| unsafe { p.read() })
    }
}

impl<T> SpinLock<T>
{
    #[cold]
    fn cas_get(&self) -> SpinGuard<'_, T>
    {
        let mut backoff = Backoff::new();
        loop
        {
            if !self.is_locked() && self.try_lock_weak()
            {
                return SpinGuard { lock: self };
            }
            if backoff.is_completed()
            {
                backoff.reset();
                continue;
            }
            backoff.snooze();
        }
    }
    #[inline]
    fn try_lock(&self) -> bool
    {
        self.locked.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok()
    }
    #[inline]
    fn try_lock_weak(&self) -> bool
    {
        self.locked.compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok()
    }
    #[inline]
    pub fn is_locked(&self) -> bool
    {
        self.locked.load(Ordering::Relaxed)
    }
}

impl<T: Default> Default for SpinLock<T>
{
    fn default() -> Self
    {
        Self::new(T::default())
    }
}
impl<T> From<T> for SpinLock<T>
{
    fn from(value: T) -> Self
    {
        Self::new(value)
    }
}

pub struct SpinGuard<'a, T>
{
    pub(super) lock: &'a SpinLock<T>,
}

unsafe impl<T: Sync> Sync for SpinGuard<'_, T> {}

impl<T> Drop for SpinGuard<'_, T>
{
    fn drop(&mut self)
    {
        self.lock.locked.store(false, Ordering::Release);
    }
}

impl<T> std::ops::Deref for SpinGuard<'_, T>
{
    type Target = T;

    fn deref(&self) -> &Self::Target
    {
        self.lock.val.with(|p| unsafe { &*p })
    }
}

impl<T> std::ops::DerefMut for SpinGuard<'_, T>
{
    fn deref_mut(&mut self) -> &mut Self::Target
    {
        self.lock.val.with_mut(|p| unsafe { &mut *p })
    }
}
#[cfg(all(test, loom))]
mod test
{
    use loom::sync::Arc;

    use super::SpinLock;

    #[test]
    fn t0_hai_thread_khong_bao_gio_cung_vao_mot_luc()
    {
        loom::model(|| {
            let lock = Arc::new(SpinLock::new(0usize));
            let l2 = Arc::clone(&lock);
            let t = loom::thread::spawn(move || {
                *l2.get() += 1;
            });
            *lock.get() += 1;
            t.join().unwrap();
            assert_eq!(*lock.get(), 2);
        });
    }
}
