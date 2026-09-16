use crate::sync::UnsafeCell;
use std::mem::MaybeUninit;

pub struct FixedBuffer<T>
{
    cells: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

impl<T> FixedBuffer<T>
{
    #[track_caller]
    pub fn new(capacity: usize) -> Self
    {
        debug_assert!(
            capacity > 0,
            "`{}` capacity `{}` must be greater than 0 !",
            std::any::type_name::<Self>(),
            capacity
        );
        Self { cells: allocate(capacity) }
    }

    #[inline]
    pub fn is_empty(&self) -> bool
    {
        self.len() < 1
    }

    #[inline]
    pub fn len(&self) -> usize
    {
        self.cells.len()
    }
    #[cfg(not(loom))]
    #[inline]
    pub fn ptr(&self) -> *const T
    {
        self.cells.as_ptr().cast::<T>()
    }
    #[inline]
    pub unsafe fn write(&self, cursor: usize, val: T)
    {
        self.at(cursor).with_mut(|p| unsafe {
            (*p).write(val);
        });
    }
    #[inline]
    pub unsafe fn get_at(&self, cursor: usize) -> &T
    {
        self.at(cursor).with(|p| unsafe { (*p).assume_init_ref() })
    }

    #[inline]
    pub unsafe fn take_at(&self, cursor: usize) -> T
    {
        self.at(cursor).with(|p| unsafe { (*p).assume_init_read() })
    }

    #[inline]
    pub unsafe fn drop_at(&self, cursor: usize)
    {
        self.at(cursor).with_mut(|p| unsafe { (*p).assume_init_drop() });
    }
}

impl<T> FixedBuffer<T>
{
    #[inline]
    fn at(&self, cursor: usize) -> &UnsafeCell<MaybeUninit<T>>
    {
        unsafe { self.cells.get_unchecked(cursor) }
    }
}
#[cfg(not(loom))]
fn allocate<T>(total_slots: usize) -> Box<[UnsafeCell<MaybeUninit<T>>]>
{
    unsafe { Box::new_uninit_slice(total_slots).assume_init() }
}

#[cfg(loom)]
fn allocate<T>(total_slots: usize) -> Box<[UnsafeCell<MaybeUninit<T>>]>
{
    (0..total_slots).map(|_| UnsafeCell::new(MaybeUninit::uninit())).collect()
}
