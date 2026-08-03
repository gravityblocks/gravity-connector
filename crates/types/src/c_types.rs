use core::slice;
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    ptr::{NonNull, null_mut},
    slice::Iter,
};

use rts_alloc::Allocator;

/// `CSlice`, as in C-language Slice. For when we're allocating memory manually,
/// and we're personally responsible for freeing.
#[derive(Clone, Copy, Debug)]
pub struct CSlice<T> {
    pub ptr: *mut T,
    /// Number of T objects in slice, not number of bytes!
    pub length: usize,
}

/// Automatically Derefs into a Slice. We almost always want to just work with
/// it as a slice.
impl<T> Deref for CSlice<T> {
    type Target = [T];
    #[inline]
    fn deref(&self) -> &[T] {
        if self.length == 0 { &[] } else { unsafe { slice::from_raw_parts(self.ptr, self.length) } }
    }
}

impl<T> DerefMut for CSlice<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [T] {
        if self.length == 0 {
            &mut []
        } else {
            unsafe { slice::from_raw_parts_mut(self.ptr, self.length) }
        }
    }
}

impl<T> From<&mut [T]> for CSlice<T> {
    #[inline]
    fn from(value: &mut [T]) -> Self {
        Self { ptr: value.as_mut_ptr(), length: value.len() }
    }
}

impl<'a, T> IntoIterator for &'a CSlice<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;
    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl<T> CSlice<T> {
    #[inline]
    pub fn from_offset(offset: CSliceOffset<T>, allocator: &Allocator) -> Self {
        if offset.length == 0 {
            Self { ptr: null_mut(), length: 0 }
        } else {
            Self {
                ptr: unsafe { allocator.ptr_from_offset(offset.offset) }.as_ptr().cast(),
                length: offset.length,
            }
        }
    }

    #[inline]
    pub fn free(&self, allocator: &Allocator) {
        if self.length > 0 {
            unsafe { allocator.free(NonNull::new(self.ptr.cast()).unwrap()) }
        }
    }

    /// Call this if you need to abuse the Rust lifetime checker to make the
    /// lifetime of the slice not dependent on the lifetime of your slice object
    #[inline]
    pub fn to_static_slice(&self) -> &'static [T] {
        if self.length == 0 {
            &mut []
        } else {
            unsafe { slice::from_raw_parts(self.ptr, self.length) }
        }
    }
}

/// Like [`CSlice`], but stores an allocation offset instead of a pointer, to be
/// able to transfer between threads.
///
/// We're personally responsible for freeing, and also keeping track of what
/// allocator the offset belongs to.
#[derive(Debug, Default)]
#[repr(C)]
pub struct CSliceOffset<T> {
    pub offset: usize,
    /// Number of T objects in the slice, not number of bytes!
    pub length: usize,
    data: PhantomData<T>,
}

impl<T> CSliceOffset<T> {
    #[inline]
    pub fn new(offset: usize, length: usize) -> Self {
        Self { offset, length, data: PhantomData }
    }
}

/// Need to do this and not `[derive(Clone, Copy)]` since otherwise, it wouldn't
/// think the type is Clone/Copy when T isn't Clone/Copy
impl<T> Clone for CSliceOffset<T> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for CSliceOffset<T> {}

impl<T> CSliceOffset<T> {
    #[inline]
    pub fn free(&self, allocator: &Allocator) {
        if self.length > 0 {
            unsafe {
                allocator.free(allocator.ptr_from_offset(self.offset));
            }
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
}
