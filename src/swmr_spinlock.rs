//! ## A spin lock built for one writer and many readers
//!
//! [`SwmrSpinLock`] is the right pick when exactly one thread ever writes, while any number of
//! threads read. Because the writer never has to race another writer, it can just set its flag
//! with a single `fetch_or` instead of looping on a compare exchange.
//!
//! The one thing you give up is the lock policing writers for you. Two writers at once would
//! quietly hand out two `&mut` to the same value, so this checks and panics instead. See
//! [`SwmrSpinLock::write`].

use crate::backoff::Backoff;
use crate::cache_padded::CachePadded;
use crate::sync::{AtomicUsize, Ordering, UnsafeCell};

/// Lowest bit of `state`: the writer is holding the lock.
const WRITER: usize = 1;
/// Every reader takes one unit starting from bit 1, so counting goes up and down in this step.
const READER: usize = 2;

/// A spin lock that lets many readers in at once, with a single writer.
///
/// `state` packs both facts into a single number: bit 0 is the writer flag, everything above it
/// counts the readers currently holding. That way both sides only ever touch one memory location.
///
/// Taking the lock for writing happens in two steps: raise the flag so no new reader can get in,
/// then spin until the readers already inside have left. Readers never wait on each other, only on
/// the writer.
pub struct SwmrSpinLock<T>
{
    val:   UnsafeCell<T>,
    state: CachePadded<AtomicUsize>,
}

unsafe impl<T: Send> Send for SwmrSpinLock<T> {}
unsafe impl<T: Send + Sync> Sync for SwmrSpinLock<T> {}

impl<T> SwmrSpinLock<T>
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

    /// Takes write access, waiting until every reader currently inside has left.
    ///
    /// # Panics
    ///
    /// If another writer is already holding the lock. Only one thread is allowed to write, so a
    /// second one showing up means the assumption this type is built on has been broken somewhere,
    /// and carrying on would hand out two `&mut` to the same value.
    #[inline]
    #[track_caller]
    pub fn write(&self) -> SwmrWriteGuard<'_, T>
    {
        let prev = self.state.fetch_or(WRITER, Ordering::Acquire);
        assert!(
            prev & WRITER == 0,
            "SwmrSpinLock allows a single writer, but a second one tried to take the lock"
        );

        if prev != 0
        {
            self.drain_readers();
        }
        SwmrWriteGuard { lock: self }
    }

    /// Tries once for write access and gives up right away if a reader is still inside.
    ///
    /// # Panics
    ///
    /// Same as [`SwmrSpinLock::write`]: if another writer is already holding the lock.
    #[inline]
    pub fn try_write(&self) -> Option<SwmrWriteGuard<'_, T>>
    {
        let prev = self.state.fetch_or(WRITER, Ordering::Acquire);
        assert!(
            prev & WRITER == 0,
            "SwmrSpinLock allows a single writer, but a second one tried to take the lock"
        );

        if prev != 0
        {
            // Readers are still inside, so put the flag back down and let them finish. Nobody else
            // can be touching the flag, so a plain clear is enough.
            self.state.fetch_and(!WRITER, Ordering::Relaxed);
            return None;
        }
        Some(SwmrWriteGuard { lock: self })
    }

    /// Takes read access, waiting until the writer is done.
    ///
    /// Several threads can hold read access at the same time.
    #[inline]
    pub fn read(&self) -> SwmrReadGuard<'_, T>
    {
        if self.try_lock_shared_weak()
        {
            return SwmrReadGuard { lock: self };
        }
        self.cas_read()
    }

    /// Tries once for read access and gives up right away instead of waiting.
    #[inline]
    pub fn try_read(&self) -> Option<SwmrReadGuard<'_, T>>
    {
        if self.try_lock_shared()
        {
            return Some(SwmrReadGuard { lock: self });
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

impl<T> SwmrSpinLock<T>
{
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
    fn cas_read(&self) -> SwmrReadGuard<'_, T>
    {
        let mut backoff = Backoff::new();
        loop
        {
            if self.try_lock_shared_weak()
            {
                return SwmrReadGuard { lock: self };
            }
            if backoff.is_completed()
            {
                backoff.reset();
                continue;
            }
            backoff.snooze();
        }
    }

    /// Adds one reader, but only while the writer is not holding the lock.
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

    /// Whether anyone is holding the lock, readers and the writer alike.
    #[inline]
    pub fn is_locked(&self) -> bool
    {
        self.state.load(Ordering::Relaxed) != 0
    }

    /// Whether the writer is holding the lock.
    ///
    /// This turns true the moment the writer raises its flag, which is a little before it actually
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

impl<T: Default> Default for SwmrSpinLock<T>
{
    fn default() -> Self
    {
        Self::new(T::default())
    }
}
impl<T> From<T> for SwmrSpinLock<T>
{
    fn from(value: T) -> Self
    {
        Self::new(value)
    }
}

/// Write access, held by the one thread allowed to write.
pub struct SwmrWriteGuard<'a, T>
{
    lock: &'a SwmrSpinLock<T>,
}

unsafe impl<T: Sync> Sync for SwmrWriteGuard<'_, T> {}

impl<T> Drop for SwmrWriteGuard<'_, T>
{
    fn drop(&mut self)
    {
        // No reader could get in while the flag was up, so the count is zero and clearing the whole
        // word is the same as clearing the flag.
        self.lock.state.store(0, Ordering::Release);
    }
}

impl<T> std::ops::Deref for SwmrWriteGuard<'_, T>
{
    type Target = T;

    fn deref(&self) -> &Self::Target
    {
        self.lock.val.with(|p| unsafe { &*p })
    }
}

impl<T> std::ops::DerefMut for SwmrWriteGuard<'_, T>
{
    fn deref_mut(&mut self) -> &mut Self::Target
    {
        self.lock.val.with_mut(|p| unsafe { &mut *p })
    }
}

/// Shared read access. It only offers `Deref`, so there is no way to write through this guard.
pub struct SwmrReadGuard<'a, T>
{
    lock: &'a SwmrSpinLock<T>,
}

unsafe impl<T: Sync> Sync for SwmrReadGuard<'_, T> {}

impl<T> Drop for SwmrReadGuard<'_, T>
{
    fn drop(&mut self)
    {
        self.lock.state.fetch_sub(READER, Ordering::Release);
    }
}

impl<T> std::ops::Deref for SwmrReadGuard<'_, T>
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

    use super::SwmrSpinLock;

    #[test]
    fn t0_reader_khong_thay_nua_chung_cua_writer()
    {
        loom::model(|| {
            let lock = Arc::new(SwmrSpinLock::new((0usize, 0usize)));
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
            let lock = Arc::new(SwmrSpinLock::new(7usize));
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
    fn t3_try_write_tra_co_ve_khi_con_reader()
    {
        loom::model(|| {
            let lock = Arc::new(SwmrSpinLock::new(0usize));
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

            // Dù try_write có vào được hay không, state phải sạch trở lại. Nếu nhánh rollback quên
            // hạ cờ thì write() dưới đây treo mãi không ra.
            assert!(!lock.is_write_locked());
            assert_eq!(lock.reader_count(), 0);
            *lock.write() += 10;
            assert!(*lock.read() == 10 || *lock.read() == 11);
        });
    }

    #[test]
    fn t4_try_read_khong_chen_vao_luc_writer_giu()
    {
        loom::model(|| {
            let lock = Arc::new(SwmrSpinLock::new(0usize));
            let l2 = Arc::clone(&lock);
            let t = loom::thread::spawn(move || {
                if let Some(g) = l2.try_read()
                {
                    assert!(*g == 0 || *g == 1);
                }
            });

            *lock.write() += 1;
            t.join().unwrap();
            assert_eq!(*lock.read(), 1);
        });
    }

    #[test]
    fn t5_writer_doi_ca_hai_reader_ra_het()
    {
        // Ba thread cùng spin thì loom phải dò số nhánh khổng lồ và test chạy hàng chục phút. Hai
        // reader ở đây dùng try_read nên không spin, còn writer vẫn phải đi qua đúng đường
        // drain_readers, tức là phần muốn kiểm vẫn được kiểm.
        let mut model = loom::model::Builder::new();
        model.max_branches = 20_000;
        model.preemption_bound = Some(2);
        model.check(|| {
            let lock = Arc::new(SwmrSpinLock::new(0usize));
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

    #[test]
    fn t2_writer_doi_reader_ra_het_moi_vao()
    {
        loom::model(|| {
            let lock = Arc::new(SwmrSpinLock::new(0usize));
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
}

#[cfg(all(test, not(loom)))]
mod test_khong_loom
{
    use super::SwmrSpinLock;

    #[test]
    fn t0_doc_ghi_co_ban()
    {
        let lock = SwmrSpinLock::new(1usize);
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
        let lock = SwmrSpinLock::new(5usize);
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
        let lock = SwmrSpinLock::new(0usize);
        let r = lock.read();
        assert!(lock.try_write().is_none());
        // Cờ phải được hạ lại, nếu không thì reader sau này không vào được nữa.
        assert!(!lock.is_write_locked());
        drop(r);

        let mut g = lock.try_write().expect("hết reader rồi thì phải vào được");
        *g += 1;
        drop(g);
        assert_eq!(*lock.read(), 1);
    }

    #[test]
    fn t3_try_read_that_bai_khi_writer_dang_giu()
    {
        let lock = SwmrSpinLock::new(0usize);
        let w = lock.write();
        assert!(lock.try_read().is_none());
        assert!(lock.is_write_locked());
        drop(w);
        assert!(lock.try_read().is_some());
    }

    #[test]
    #[should_panic(expected = "single writer")]
    fn t4_writer_thu_hai_thi_panic()
    {
        let lock = SwmrSpinLock::new(0usize);
        let _first = lock.write();
        let _second = lock.write();
    }

    #[test]
    #[should_panic(expected = "single writer")]
    fn t5_try_write_cung_panic_khi_da_co_writer()
    {
        let lock = SwmrSpinLock::new(0usize);
        let _first = lock.write();
        let _second = lock.try_write();
    }

    #[test]
    fn t6_get_mut_take_default_from()
    {
        let mut lock = SwmrSpinLock::new(1usize);
        *lock.get_mut() += 1;
        assert_eq!(lock.take(), 2);

        assert_eq!(*SwmrSpinLock::<usize>::default().read(), 0);
        assert_eq!(*SwmrSpinLock::from(9usize).read(), 9);
    }

    /// Một writer và hai reader chạy thật, đủ ngắn để Miri theo nổi.
    ///
    /// Loom lo phần ordering, còn lượt chạy này để Miri soi chuyện đụng chạm bộ nhớ trong
    /// `UnsafeCell`: hai bên cùng chạm vào một ô nhớ mà lock để lọt thì Miri kêu ngay.
    #[test]
    fn t8_mot_writer_hai_reader_chay_that()
    {
        use std::sync::Arc;
        use std::thread;

        const VONG: usize = 8;

        let lock = Arc::new(SwmrSpinLock::new((0usize, 0usize)));

        let readers: Vec<_> = (0..2)
            .map(|_| {
                let l = Arc::clone(&lock);
                thread::spawn(move || {
                    for _ in 0..VONG
                    {
                        let g = l.read();
                        // Writer luôn giữ hai nửa bằng nhau, nên lệch nghĩa là reader đã nhìn thấy
                        // writer đang viết dở.
                        assert_eq!(g.0, g.1);
                    }
                })
            })
            .collect();

        for _ in 0..VONG
        {
            let mut g = lock.write();
            g.0 += 1;
            g.1 += 1;
        }

        for t in readers
        {
            t.join().unwrap();
        }

        let g = lock.read();
        assert_eq!((g.0, g.1), (VONG, VONG));
    }

    #[test]
    fn t7_take_khong_lam_ro_ri_gia_tri_ben_trong()
    {
        use std::sync::Arc;

        let shared = Arc::new(7usize);
        let lock = SwmrSpinLock::new(Arc::clone(&shared));
        assert_eq!(Arc::strong_count(&shared), 2);

        let inner = lock.take();
        assert_eq!(Arc::strong_count(&shared), 2);
        drop(inner);
        assert_eq!(Arc::strong_count(&shared), 1);
    }
}
