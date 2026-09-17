//! ## A spin lock for many writers and many readers
//!
//! [`MwmrSpinLock`] is the sibling of [`SwmrSpinLock`](crate::swmr_spinlock::SwmrSpinLock) for when
//! more than one thread writes. Writers take turns with each other and also wait for the readers
//! already inside to leave. Readers never wait on each other, any number of them can be in at once,
//! and they only stop when a writer is holding the lock or waiting to get in.
//!
//! The only real difference from the single writer version: claiming the writer flag has to go
//! through a compare exchange instead of one `fetch_or`, because two writers can reach for it at
//! the same moment.

use crate::backoff::Backoff;
use crate::cache_padded::CachePadded;
use crate::sync::{AtomicUsize, Ordering, UnsafeCell};

/// Lowest bit of `state`: a writer is holding the lock, or waiting for the readers to leave.
const WRITER: usize = 1;
/// Every reader takes one unit starting from bit 1, so counting goes up and down in this step.
const READER: usize = 2;

/// A spin lock that lets many readers in at once, with writers taking turns.
///
/// `state` packs both facts into a single number: bit 0 is the writer flag, everything above it
/// counts the readers currently holding. That way both sides only ever touch one memory location.
///
/// Taking the lock for writing happens in two steps: claim the flag so no new reader can get in,
/// then spin until the readers already inside have left. Claiming the flag is also where writers
/// shut each other out. Since the flag going up blocks new readers right away, a steady stream of
/// readers cannot starve a writer.
pub struct MwmrSpinLock<T>
{
    val:   UnsafeCell<T>,
    state: CachePadded<AtomicUsize>,
}

unsafe impl<T: Send> Send for MwmrSpinLock<T> {}
unsafe impl<T: Send + Sync> Sync for MwmrSpinLock<T> {}

impl<T> MwmrSpinLock<T>
{
    #[cfg(not(loom))]
    pub const fn new(val: T) -> Self
    {
        Self {
            val:   UnsafeCell::new(val),
            state: CachePadded::new(AtomicUsize::new(0)),
        }
    }
    #[cfg(loom)]
    pub fn new(val: T) -> Self
    {
        Self {
            val:   UnsafeCell::new(val),
            state: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    /// Takes write access, waiting until no other writer holds the lock and every reader inside has
    /// left.
    #[inline]
    pub fn write(&self) -> MwmrWriteGuard<'_, T>
    {
        // Fast path: the lock is completely free, so a single CAS is all it takes.
        if self.state.compare_exchange_weak(0, WRITER, Ordering::Acquire, Ordering::Relaxed).is_ok()
        {
            return MwmrWriteGuard { lock: self };
        }
        self.lock_exclusive_slow()
    }

    /// Tries once for write access and gives up right away if the lock is busy.
    #[inline]
    pub fn try_write(&self) -> Option<MwmrWriteGuard<'_, T>>
    {
        // Only accept a completely free lock. Claiming the flag and then finding readers still
        // inside would mean putting it back down, and in the meantime another writer was held up
        // for nothing, so better not to claim it at all.
        if self.state.compare_exchange(0, WRITER, Ordering::Acquire, Ordering::Relaxed).is_ok()
        {
            return Some(MwmrWriteGuard { lock: self });
        }
        None
    }

    /// Takes read access, waiting until the writer is done.
    ///
    /// Several threads can hold read access at the same time.
    #[inline]
    pub fn read(&self) -> MwmrReadGuard<'_, T>
    {
        if self.try_lock_shared_weak()
        {
            return MwmrReadGuard { lock: self };
        }
        self.cas_read()
    }

    /// Tries once for read access and gives up right away instead of waiting.
    #[inline]
    pub fn try_read(&self) -> Option<MwmrReadGuard<'_, T>>
    {
        if self.try_lock_shared()
        {
            return Some(MwmrReadGuard { lock: self });
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

impl<T> MwmrSpinLock<T>
{
    /// Claims the writer flag, then waits for the readers to drain.
    ///
    /// The loop itself only deals with the other writers: while the flag is up it keeps waiting,
    /// and once it is down a CAS claims it, leaving the reader count untouched. Only after winning
    /// the flag does it wait on the readers.
    #[cold]
    fn lock_exclusive_slow(&self) -> MwmrWriteGuard<'_, T>
    {
        let mut backoff = Backoff::new();
        loop
        {
            let state = self.state.load(Ordering::Relaxed);
            if state & WRITER == 0
                && self
                    .state
                    .compare_exchange_weak(state, state | WRITER, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
            {
                if state != 0
                {
                    self.drain_readers();
                }
                return MwmrWriteGuard { lock: self };
            }

            if backoff.is_completed()
            {
                backoff.reset();
                continue;
            }
            backoff.snooze();
        }
    }

    /// Spins until the readers that were already inside when the flag went up have all left.
    ///
    /// The flag is up by now, so no new reader can join and the count only goes down.
    #[cold]
    fn drain_readers(&self)
    {
        let mut backoff = Backoff::new();
        while self.state.load(Ordering::Acquire) != WRITER
        {
            if backoff.is_completed()
            {
                backoff.reset();
                continue;
            }
            backoff.snooze();
        }
    }

    #[cold]
    fn cas_read(&self) -> MwmrReadGuard<'_, T>
    {
        let mut backoff = Backoff::new();
        loop
        {
            if self.try_lock_shared_weak()
            {
                return MwmrReadGuard { lock: self };
            }
            if backoff.is_completed()
            {
                backoff.reset();
                continue;
            }
            backoff.snooze();
        }
    }

    /// Adds one reader, but only while no writer is holding the flag.
    #[inline]
    fn try_lock_shared(&self) -> bool
    {
        let state = self.state.load(Ordering::Relaxed);
        if state & WRITER != 0
        {
            return false;
        }
        self.state.compare_exchange(state, state + READER, Ordering::Acquire, Ordering::Relaxed).is_ok()
    }
    #[inline]
    fn try_lock_shared_weak(&self) -> bool
    {
        let state = self.state.load(Ordering::Relaxed);
        if state & WRITER != 0
        {
            return false;
        }
        self.state
            .compare_exchange_weak(state, state + READER, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    /// Whether anyone is holding the lock, readers and writers alike.
    #[inline]
    pub fn is_locked(&self) -> bool
    {
        self.state.load(Ordering::Relaxed) != 0
    }

    /// Whether a writer is holding the lock.
    ///
    /// This turns true the moment a writer claims the flag, which is a little before it actually
    /// gets in, while it is still waiting for the readers to leave.
    #[inline]
    pub fn is_write_locked(&self) -> bool
    {
        self.state.load(Ordering::Relaxed) & WRITER != 0
    }

    /// How many readers are holding the lock at the moment of the read. Handy for a peek, but it
    /// can change the instant you get it, so do not make decisions on it.
    #[inline]
    pub fn reader_count(&self) -> usize
    {
        self.state.load(Ordering::Relaxed) / READER
    }
}

impl<T: Default> Default for MwmrSpinLock<T>
{
    fn default() -> Self
    {
        Self::new(T::default())
    }
}
impl<T> From<T> for MwmrSpinLock<T>
{
    fn from(value: T) -> Self
    {
        Self::new(value)
    }
}

/// Write access, held by one thread at a time.
pub struct MwmrWriteGuard<'a, T>
{
    lock: &'a MwmrSpinLock<T>,
}

unsafe impl<T: Sync> Sync for MwmrWriteGuard<'_, T> {}

impl<T> Drop for MwmrWriteGuard<'_, T>
{
    fn drop(&mut self)
    {
        // No reader could get in while the flag was up, so the count is zero and clearing the whole
        // word is the same as clearing the flag.
        self.lock.state.store(0, Ordering::Release);
    }
}

impl<T> std::ops::Deref for MwmrWriteGuard<'_, T>
{
    type Target = T;

    fn deref(&self) -> &Self::Target
    {
        self.lock.val.with(|p| unsafe { &*p })
    }
}

impl<T> std::ops::DerefMut for MwmrWriteGuard<'_, T>
{
    fn deref_mut(&mut self) -> &mut Self::Target
    {
        self.lock.val.with_mut(|p| unsafe { &mut *p })
    }
}

/// Shared read access. It only offers `Deref`, so there is no way to write through this guard.
pub struct MwmrReadGuard<'a, T>
{
    lock: &'a MwmrSpinLock<T>,
}

unsafe impl<T: Sync> Sync for MwmrReadGuard<'_, T> {}

impl<T> Drop for MwmrReadGuard<'_, T>
{
    fn drop(&mut self)
    {
        self.lock.state.fetch_sub(READER, Ordering::Release);
    }
}

impl<T> std::ops::Deref for MwmrReadGuard<'_, T>
{
    type Target = T;

    fn deref(&self) -> &Self::Target
    {
        self.lock.val.with(|p| unsafe { &*p })
    }
}

#[cfg(all(test, loom))]
mod test
{
    use loom::sync::Arc;

    use super::MwmrSpinLock;

    #[test]
    fn t0_reader_khong_thay_nua_chung_cua_writer()
    {
        loom::model(|| {
            let lock = Arc::new(MwmrSpinLock::new((0usize, 0usize)));
            let l2 = Arc::clone(&lock);
            let t = loom::thread::spawn(move || {
                let mut g = l2.write();
                g.0 += 1;
                g.1 += 1;
            });
            {
                let g = lock.read();
                assert_eq!(g.0, g.1);
            }
            t.join().unwrap();
            let g = lock.read();
            assert_eq!((g.0, g.1), (1, 1));
        });
    }

    #[test]
    fn t1_hai_reader_vao_duoc_cung_luc()
    {
        loom::model(|| {
            let lock = Arc::new(MwmrSpinLock::new(7usize));
            let l2 = Arc::clone(&lock);
            let t = loom::thread::spawn(move || {
                let g = l2.read();
                assert_eq!(*g, 7);
            });
            let g = lock.read();
            assert_eq!(*g, 7);
            drop(g);
            t.join().unwrap();
        });
    }

    #[test]
    fn t2_hai_writer_khong_dam_len_nhau()
    {
        loom::model(|| {
            let lock = Arc::new(MwmrSpinLock::new((0usize, 0usize)));
            let l2 = Arc::clone(&lock);
            let t = loom::thread::spawn(move || {
                let mut g = l2.write();
                // Hai nửa lệch nhau nghĩa là writer kia đang viết dở mà mình vẫn vào được.
                assert_eq!(g.0, g.1);
                g.0 += 1;
                g.1 += 1;
            });

            {
                let mut g = lock.write();
                assert_eq!(g.0, g.1);
                g.0 += 1;
                g.1 += 1;
            }
            t.join().unwrap();

            let g = lock.read();
            assert_eq!((g.0, g.1), (2, 2));
        });
    }

    #[test]
    fn t3_try_write_that_bai_thi_khong_ket_co()
    {
        loom::model(|| {
            let lock = Arc::new(MwmrSpinLock::new(0usize));
            let l2 = Arc::clone(&lock);
            let t = loom::thread::spawn(move || {
                let g = l2.read();
                assert!(*g == 0 || *g == 1);
            });

            if let Some(mut g) = lock.try_write()
            {
                *g += 1;
            }
            t.join().unwrap();

            // Dù try_write có vào được hay không, state phải sạch trở lại. Kẹt cờ thì write() dưới
            // đây treo mãi không ra.
            assert!(!lock.is_write_locked());
            assert_eq!(lock.reader_count(), 0);
            *lock.write() += 10;
            assert!(*lock.read() == 10 || *lock.read() == 11);
        });
    }

    #[test]
    fn t4_writer_doi_reader_ra_het_moi_vao()
    {
        loom::model(|| {
            let lock = Arc::new(MwmrSpinLock::new(0usize));
            let l2 = Arc::clone(&lock);
            let t = loom::thread::spawn(move || {
                let g = l2.read();
                let seen = *g;
                drop(g);
                assert!(seen == 0 || seen == 1);
            });
            *lock.write() += 1;
            t.join().unwrap();
            assert_eq!(*lock.read(), 1);
        });
    }

    #[test]
    fn t5_writer_giu_co_roi_van_doi_hai_reader()
    {
        // Ba thread cùng spin thì loom phải dò số nhánh khổng lồ. Hai reader ở đây dùng try_read
        // nên không spin, còn writer vẫn đi đúng đường drain_readers.
        let mut model = loom::model::Builder::new();
        model.max_branches = 20_000;
        model.preemption_bound = Some(2);
        model.check(|| {
            let lock = Arc::new(MwmrSpinLock::new(0usize));
            let readers: Vec<_> = (0..2)
                .map(|_| {
                    let l = Arc::clone(&lock);
                    loom::thread::spawn(move || {
                        if let Some(g) = l.try_read()
                        {
                            assert!(*g == 0 || *g == 1);
                        }
                    })
                })
                .collect();

            *lock.write() += 1;
            for t in readers
            {
                t.join().unwrap();
            }
            assert_eq!(*lock.read(), 1);
        });
    }
}

#[cfg(all(test, not(loom)))]
mod test_khong_loom
{
    use super::MwmrSpinLock;

    #[test]
    fn t0_doc_ghi_co_ban()
    {
        let lock = MwmrSpinLock::new(1usize);
        assert!(!lock.is_locked());

        {
            let mut g = lock.write();
            *g += 1;
        }
        assert_eq!(*lock.read(), 2);
        assert!(!lock.is_locked());
    }

    #[test]
    fn t1_nhieu_reader_cung_luc_va_dem_dung()
    {
        let lock = MwmrSpinLock::new(5usize);
        let a = lock.read();
        let b = lock.read();
        assert_eq!(lock.reader_count(), 2);
        assert!(lock.is_locked());
        assert!(!lock.is_write_locked());
        assert_eq!((*a, *b), (5, 5));

        drop(a);
        assert_eq!(lock.reader_count(), 1);
        drop(b);
        assert_eq!(lock.reader_count(), 0);
    }

    #[test]
    fn t2_try_write_that_bai_khi_con_reader_va_khong_ket_co()
    {
        let lock = MwmrSpinLock::new(0usize);
        let r = lock.read();
        assert!(lock.try_write().is_none());
        assert!(!lock.is_write_locked());
        drop(r);

        let mut g = lock.try_write().expect("hết reader rồi thì phải vào được");
        *g += 1;
        drop(g);
        assert_eq!(*lock.read(), 1);
    }

    #[test]
    fn t3_try_write_that_bai_khi_writer_khac_dang_giu()
    {
        let lock = MwmrSpinLock::new(0usize);
        let w = lock.write();
        assert!(lock.try_write().is_none());
        assert!(lock.is_write_locked());
        drop(w);
        assert!(lock.try_write().is_some());
    }

    #[test]
    fn t4_try_read_that_bai_khi_writer_dang_giu()
    {
        let lock = MwmrSpinLock::new(0usize);
        let w = lock.write();
        assert!(lock.try_read().is_none());
        drop(w);
        assert!(lock.try_read().is_some());
    }

    #[test]
    fn t5_get_mut_take_default_from()
    {
        let mut lock = MwmrSpinLock::new(1usize);
        *lock.get_mut() += 1;
        assert_eq!(lock.take(), 2);

        assert_eq!(*MwmrSpinLock::<usize>::default().read(), 0);
        assert_eq!(*MwmrSpinLock::from(9usize).read(), 9);
    }

    /// Hai writer và hai reader chạy thật, đủ ngắn để Miri theo nổi.
    ///
    /// Loom lo phần ordering, lượt chạy này để Miri soi chuyện đụng chạm bộ nhớ trong
    /// `UnsafeCell`: lock để lọt là Miri kêu ngay.
    #[test]
    fn t6_hai_writer_hai_reader_chay_that()
    {
        use std::sync::Arc;
        use std::thread;

        const VONG: usize = 8;

        let lock = Arc::new(MwmrSpinLock::new((0usize, 0usize)));

        let readers: Vec<_> = (0..2)
            .map(|_| {
                let l = Arc::clone(&lock);
                thread::spawn(move || {
                    for _ in 0..VONG
                    {
                        let g = l.read();
                        // Writer luôn giữ hai nửa bằng nhau, lệch nghĩa là reader nhìn thấy writer
                        // đang viết dở.
                        assert_eq!(g.0, g.1);
                    }
                })
            })
            .collect();

        let writers: Vec<_> = (0..2)
            .map(|_| {
                let l = Arc::clone(&lock);
                thread::spawn(move || {
                    for _ in 0..VONG
                    {
                        let mut g = l.write();
                        assert_eq!(g.0, g.1);
                        g.0 += 1;
                        g.1 += 1;
                    }
                })
            })
            .collect();

        for t in readers.into_iter().chain(writers)
        {
            t.join().unwrap();
        }

        let g = lock.read();
        assert_eq!((g.0, g.1), (2 * VONG, 2 * VONG));
    }

    #[test]
    fn t7_take_khong_lam_ro_ri_gia_tri_ben_trong()
    {
        use std::sync::Arc;

        let shared = Arc::new(7usize);
        let lock = MwmrSpinLock::new(Arc::clone(&shared));
        assert_eq!(Arc::strong_count(&shared), 2);

        let inner = lock.take();
        assert_eq!(Arc::strong_count(&shared), 2);
        drop(inner);
        assert_eq!(Arc::strong_count(&shared), 1);
    }
}
