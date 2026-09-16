use crate::sync::UnsafeCell;
use std::mem::MaybeUninit;

pub struct Slots<T>
{
    cells: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask:  u32,
}

impl<T> Slots<T>
{
    pub fn new(total_slots: u32) -> Self
    {
        let total_slots = total_slots.next_power_of_two().max(2);
        Self {
            cells: allocate(total_slots as usize),
            mask:  total_slots - 1,
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

    #[inline]
    fn at(&self, idx: u32) -> &UnsafeCell<MaybeUninit<T>>
    {
        unsafe { self.cells.get_unchecked((idx & self.mask) as usize) }
    }

    #[inline]
    pub unsafe fn write(&self, idx: u32, val: T)
    {
        self.at(idx).with_mut(|p| unsafe {
            (*p).write(val);
        });
    }

    #[inline]
    pub unsafe fn read(&self, idx: u32) -> T
    {
        self.at(idx).with(|p| unsafe { (*p).assume_init_read() })
    }

    #[inline]
    pub unsafe fn drop_at(&self, idx: u32)
    {
        self.at(idx).with_mut(|p| unsafe { (*p).assume_init_drop() });
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
