use crate::sync::UnsafeCell;
use std::mem::MaybeUninit;

const MAX_CAPACITY: usize = 1 << 31;
pub struct FixedRingBuffer<T>
{
    cells: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask:  u32,
}

impl<T> FixedRingBuffer<T>
{
    /// Panics if `capacity` is zero, is not a power of two, or is at least `MAX_CAPACITY`
    #[track_caller]
    pub fn new(capacity: usize) -> Self
    {
        // Unchecked slot access and wrapping cursors require these bounds in every build
        assert!(
            capacity > 0,
            "`{}` capacity `{}` must be greater than 0 !",
            std::any::type_name::<Self>(),
            capacity
        );
        assert!(
            capacity < MAX_CAPACITY,
            "`{}` capacity `{}` exceeds `{}` limit for wrapping u32 indices",
            std::any::type_name::<Self>(),
            capacity,
            MAX_CAPACITY
        );
        assert!(
            capacity.is_power_of_two(),
            "`{}` is initizealed with capacity `{}` is not power of 2 !",
            std::any::type_name::<Self>(),
            capacity
        );
        Self {
            cells: allocate(capacity),
            mask:  (capacity - 1) as u32,
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize
    {
        self.mask as usize + 1
    }

    #[inline]
    pub fn mask(&self) -> u32
    {
        self.mask
    }

    /// A pointer to the first memory cell, useful for prefetching or address comparison.
    /// The buffer does not track which cells contain data, so avoid reading from or writing to this pointer unless you are certain the cell has been initialized via [`Self::write`].
    /// The `loom` version lacks this function because `loom::cell::UnsafeCell` wraps additional metadata, meaning the memory layout no longer matches a standard `T` array.
    #[cfg(not(loom))]
    #[inline]
    pub fn ptr(&self) -> *const T
    {
        self.cells.as_ptr().cast::<T>()
    }

    #[inline]
    fn at(&self, cursor: u32) -> &UnsafeCell<MaybeUninit<T>>
    {
        unsafe { self.cells.get_unchecked((cursor & self.mask) as usize) }
    }

    #[inline]
    pub unsafe fn write(&self, cursor: u32, val: T)
    {
        self.at(cursor).with_mut(|p| unsafe {
            (*p).write(val);
        });
    }

    #[inline]
    pub unsafe fn take_at(&self, cursor: u32) -> T
    {
        self.at(cursor).with(|p| unsafe { (*p).assume_init_read() })
    }

    #[inline]
    pub unsafe fn drop_at(&self, cursor: u32)
    {
        self.at(cursor).with_mut(|p| unsafe { (*p).assume_init_drop() });
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
