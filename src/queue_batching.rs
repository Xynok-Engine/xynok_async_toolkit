use std::collections::VecDeque;

use crate::backoff::Backoff;
use crate::cache_padded::CachePadded;
use crate::fixed_ring_buffer::FixedRingBuffer;
use crate::sync::{AtomicBool, AtomicUsize, Ordering, UnsafeCell};

pub struct QueueBatching<T>
{
    elements: UnsafeCell<VecDeque<T>>,
    locked:   CachePadded<AtomicBool>,
    len:      CachePadded<AtomicUsize>,
}

unsafe impl<T: Send> Send for QueueBatching<T> {}
unsafe impl<T: Send> Sync for QueueBatching<T> {}

impl<T> QueueBatching<T>
{
    pub fn new() -> Self
    {
        Self::from_queue(VecDeque::new())
    }

    pub fn with_capacity(capacity: usize) -> Self
    {
        Self::from_queue(VecDeque::with_capacity(capacity))
    }

    fn from_queue(queue: VecDeque<T>) -> Self
    {
        let len = queue.len();
        Self {
            elements: UnsafeCell::new(queue),
            locked:   CachePadded::new(AtomicBool::new(false)),
            len:      CachePadded::new(AtomicUsize::new(len)),
        }
    }
}

impl<T> QueueBatching<T>
{
    /// Waits until the internal queue can be acquired
    #[inline]
    pub fn get(&self) -> QueueBatchingGuard<'_, T>
    {
        if self.try_lock_weak()
        {
            return QueueBatchingGuard { queue_batching: self };
        }
        self.cas_get()
    }

    #[inline]
    pub fn try_get(&self) -> Option<QueueBatchingGuard<'_, T>>
    {
        match self.try_lock()
        {
            true => Some(QueueBatchingGuard { queue_batching: self }),
            false => None,
        }
    }

    #[inline]
    pub fn get_mut(&mut self) -> QueueBatchingGuard<'_, T>
    {
        debug_assert!(!self.is_locked(), "Attempted to acquire exclusive access while the lock is already held");
        self.locked.store(true, Ordering::Relaxed);
        QueueBatchingGuard { queue_batching: self }
    }

    pub fn take(self) -> VecDeque<T>
    {
        let this = std::mem::ManuallyDrop::new(self);
        this.elements.with_mut(|p| unsafe { p.read() })
    }
}

impl<T> QueueBatching<T>
{
    #[inline]
    pub fn push(&self, val: T)
    {
        self.get().push_back(val);
    }

    #[inline]
    pub fn push_batch<I>(&self, values: I)
    where I: IntoIterator<Item = T>
    {
        self.get().extend(values);
    }
}

impl<T> QueueBatching<T>
{
    #[inline]
    pub fn pop(&self) -> Option<T>
    {
        if self.is_empty()
        {
            return None;
        }
        self.get().pop_front()
    }

    pub fn pop_batch(&self, out: &mut Vec<T>, limit: usize) -> usize
    {
        if limit == 0 || self.is_empty()
        {
            return 0;
        }

        let mut elements = self.get();
        let taken = limit.min(elements.len());
        out.extend(elements.drain(..taken));
        taken
    }

    #[inline]
    pub fn drain_into(&self, out: &mut Vec<T>) -> usize
    {
        self.pop_batch(out, usize::MAX)
    }

    #[inline]
    pub fn drain_into_buffer(&self, max: usize, dst: &FixedRingBuffer<T>, write_start_cursor: u32) -> usize
    {
        if max == 0 || self.is_empty()
        {
            return 0;
        }
        debug_assert!(
            max <= dst.capacity(),
            "`{}` has only `{}` slots, which cannot hold `{}` elements",
            std::any::type_name::<FixedRingBuffer<T>>(),
            dst.capacity(),
            max
        );

        let mut elements = self.get();
        let moved = max.min(elements.len());
        for (offset, val) in elements.drain(..moved).enumerate()
        {
            unsafe {
                dst.write(write_start_cursor.wrapping_add(offset as u32), val);
            }
        }
        moved
    }

    pub fn clear(&self) -> usize
    {
        let mut elements = self.get();
        let cleared = elements.len();
        elements.clear();
        cleared
    }
}

impl<T> QueueBatching<T>
{
    #[inline]
    pub fn len(&self) -> usize
    {
        self.len.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn is_empty(&self) -> bool
    {
        self.len() == 0
    }

    #[inline]
    pub fn is_locked(&self) -> bool
    {
        self.locked.load(Ordering::Relaxed)
    }
}

impl<T> QueueBatching<T>
{
    #[cold]
    fn cas_get(&self) -> QueueBatchingGuard<'_, T>
    {
        let mut backoff = Backoff::new();
        loop
        {
            if !self.is_locked() && self.try_lock_weak()
            {
                return QueueBatchingGuard { queue_batching: self };
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

    /// Record the length and then release the lock, in that exact order.
    /// `len` comes first, and the `store` release of `locked` enforces the order.
    /// This ensures that anyone who acquires the lock after us will see the updated value.
    /// `len` itself only requires `Relaxed` ordering: it does not guard any memory, and readers do not access the elements based on its value.
    #[inline]
    pub(super) fn release(&self, len: usize)
    {
        self.len.store(len, Ordering::Relaxed);
        self.locked.store(false, Ordering::Release);
    }
}

impl<T> Default for QueueBatching<T>
{
    fn default() -> Self
    {
        Self::new()
    }
}

impl<T> From<VecDeque<T>> for QueueBatching<T>
{
    fn from(value: VecDeque<T>) -> Self
    {
        Self::from_queue(value)
    }
}

impl<T> FromIterator<T> for QueueBatching<T>
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self
    {
        Self::from_queue(VecDeque::from_iter(iter))
    }
}

impl<T> Extend<T> for QueueBatching<T>
{
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I)
    {
        self.get_mut().extend(iter);
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for QueueBatching<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        let mut builder = f.debug_struct("QueueBatching");
        match self.try_get()
        {
            Some(elements) => builder.field("elements", &*elements),
            None => builder.field("elements", &format_args!("<locked>")),
        }
        .finish()
    }
}

pub struct QueueBatchingGuard<'a, T>
{
    queue_batching: &'a QueueBatching<T>,
}

impl<T> QueueBatchingGuard<'_, T>
{
    #[inline]
    pub fn pop(&mut self) -> Option<T>
    {
        self.pop_front()
    }
    #[inline]
    pub fn push(&mut self, val: T)
    {
        self.push_back(val)
    }
}
unsafe impl<T: Sync> Sync for QueueBatchingGuard<'_, T> {}

impl<T> Drop for QueueBatchingGuard<'_, T>
{
    fn drop(&mut self)
    {
        let len = self.len();
        self.queue_batching.release(len);
    }
}

impl<T> std::ops::Deref for QueueBatchingGuard<'_, T>
{
    type Target = VecDeque<T>;

    #[inline]
    fn deref(&self) -> &Self::Target
    {
        self.queue_batching.elements.with(|p| unsafe { &*p })
    }
}

impl<T> std::ops::DerefMut for QueueBatchingGuard<'_, T>
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target
    {
        self.queue_batching.elements.with_mut(|p| unsafe { &mut *p })
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for QueueBatchingGuard<'_, T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        std::fmt::Debug::fmt(&**self, f)
    }
}
