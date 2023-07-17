use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::{transmute, ManuallyDrop};
use std::ops::Deref;
use std::ptr::NonNull;

pub mod trace;

#[derive(Copy, Clone, Eq, PartialEq)]
enum GcState {
    Active,
    Dropped,
    Tracked,
    Untracked,
}

struct GcInfo<'c> {
    next: Cell<GcBoxPtr<'c>>,
    prev: Cell<GcBoxPtr<'c>>,
    state: Cell<GcState>,
    root: Cell<usize>,
    count: Cell<usize>,
}

#[repr(C)]
struct GcBox<'c, T: GcTarget<'c> + ?Sized> {
    metadata: &'static (),
    info: GcInfo<'c>,
    value: UnsafeCell<ManuallyDrop<T>>,
}

impl<'c, T: GcTarget<'c>> GcBox<'c, T> {
    pub fn new(value: T) -> Self {
        Self {
            metadata: GcBoxDynPtr::from_ptr(std::ptr::null_mut::<GcBox<'c, T>>()).metadata,
            info: GcInfo {
                next: Cell::new(GcBoxPtr::null()),
                prev: Cell::new(GcBoxPtr::null()),
                state: Cell::new(GcState::Active),
                root: Cell::new(0),
                count: Cell::new(0),
            },
            value: UnsafeCell::new(ManuallyDrop::new(value)),
        }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> GcBox<'c, T> {
    unsafe fn free(this: NonNull<Self>) {
        let r = this.as_ref();
        let prev = r.info.prev.get();
        let next = r.info.next.get();
        (*prev.as_ptr()).info.next.set(next);
        (*next.as_ptr()).info.prev.set(prev);
        drop(Box::from_raw(this.as_ptr()));
    }

    unsafe fn check(this: NonNull<Self>) {
        let r = this.as_ref();
        match r.info.state.get() {
            GcState::Active => {
                if r.info.root.get() == 0 && r.info.count.get() == 0 {
                    r.info.state.set(GcState::Dropped);
                    ManuallyDrop::drop(&mut *r.value.get());
                    Self::free(this);
                }
            }
            GcState::Dropped => Self::free(this),
            GcState::Tracked | GcState::Untracked => {}
        }
    }
}

#[derive(Copy, Clone)]
#[repr(C)]
struct GcBoxDynPtr<'c> {
    ptr: *const (),
    metadata: &'static (),
    marker: PhantomData<*mut GcBox<'c, dyn GcTarget<'c>>>,
}

impl<'c> GcBoxDynPtr<'c> {
    const fn from_part(ptr: *const (), metadata: &'static ()) -> Self {
        Self {
            ptr,
            metadata,
            marker: PhantomData,
        }
    }

    const fn from_ptr(ptr: *mut GcBox<'c, dyn GcTarget<'c>>) -> Self {
        unsafe { transmute(ptr) }
    }

    const fn as_ptr(self) -> *mut GcBox<'c, dyn GcTarget<'c>> {
        unsafe { transmute(self) }
    }
}

#[derive(Copy, Clone)]
#[repr(transparent)]
struct GcBoxPtr<'c> {
    ptr: *const (),
    marker: PhantomData<*mut GcBox<'c, dyn GcTarget<'c>>>,
}

impl<'c> GcBoxPtr<'c> {
    const fn from_ptr(ptr: *const ()) -> Self {
        Self {
            ptr,
            marker: PhantomData,
        }
    }

    const fn null() -> Self {
        Self::from_ptr(std::ptr::null())
    }

    const fn from_ref<T: GcTarget<'c> + ?Sized>(ptr: &GcBox<'c, T>) -> Self {
        Self::from_ptr(ptr as *const GcBox<'c, T> as *const ())
    }

    fn as_ptr(self) -> *mut GcBox<'c, dyn GcTarget<'c>> {
        let metadata = unsafe { *self.ptr.cast::<&'static ()>() };
        GcBoxDynPtr::from_part(self.ptr, metadata).as_ptr()
    }
}

pub struct GcRoot<'c, T: GcTarget<'c> + ?Sized> {
    ptr: NonNull<GcBox<'c, T>>,
}

impl<'c, T: GcTarget<'c> + ?Sized> GcRoot<'c, T> {
    unsafe fn from_ref(ptr: &GcBox<'c, T>) -> Self {
        ptr.info.root.set(ptr.info.root.get() + 1);
        Self {
            ptr: NonNull::from(ptr),
        }
    }

    fn as_ref(&self) -> &GcBox<'c, T> {
        unsafe { self.ptr.as_ref() }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> Drop for GcRoot<'c, T> {
    fn drop(&mut self) {
        unsafe {
            let node = self.as_ref();
            node.info.root.set(node.info.root.get() - 1);
            GcBox::check(self.ptr)
        }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> GcTarget<'c> for GcRoot<'c, T> {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        let _ = token;
    }
}

pub struct GcObject<'c, T: GcTarget<'c> + ?Sized> {
    ptr: NonNull<GcBox<'c, T>>,
}

impl<'c, T: GcTarget<'c> + ?Sized> GcObject<'c, T> {
    unsafe fn from_ref(ptr: &GcBox<'c, T>) -> Self {
        ptr.info.count.set(ptr.info.count.get() + 1);
        Self {
            ptr: NonNull::from(ptr),
        }
    }

    fn as_ref(&self) -> &GcBox<'c, T> {
        unsafe { self.ptr.as_ref() }
    }

    pub fn from_root(ptr: &GcRoot<'c, T>) -> Self {
        unsafe { Self::from_ref(ptr.as_ref()) }
    }

    pub fn to_root(&self) -> Option<GcRoot<'c, T>> {
        let r = self.as_ref();
        match r.info.state.get() {
            GcState::Active | GcState::Tracked => unsafe { Some(GcRoot::from_ref(self.as_ref())) },
            GcState::Dropped | GcState::Untracked => None,
        }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> Drop for GcObject<'c, T> {
    fn drop(&mut self) {
        unsafe {
            let node = self.as_ref();
            node.info.count.set(node.info.count.get() - 1);
            GcBox::check(self.ptr)
        }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> GcTarget<'c> for GcObject<'c, T> {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        token.accept(self);
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> From<GcRoot<'c, T>> for GcObject<'c, T> {
    fn from(value: GcRoot<'c, T>) -> Self {
        Self::from_root(&value)
    }
}

impl<'s, 'c, T: GcTarget<'c> + ?Sized> From<&'s GcRoot<'c, T>> for GcObject<'c, T> {
    fn from(value: &GcRoot<'c, T>) -> Self {
        Self::from_root(value)
    }
}

pub struct GcTraceToken<'c> {
    head: GcBoxPtr<'c>,
}

impl<'c> GcTraceToken<'c> {
    pub fn accept<T: GcTarget<'c> + ?Sized>(&mut self, value: &GcObject<'c, T>) {
        let r = value.as_ref();
        match r.info.state.get() {
            GcState::Untracked => {
                r.info.state.set(GcState::Tracked);
                r.info.next.set(self.head);
                self.head = GcBoxPtr::from_ref(r);
            }
            GcState::Tracked | GcState::Active | GcState::Dropped => {}
        }
    }
}

pub trait GcTarget<'c>: 'c {
    fn trace(&self, token: &mut GcTraceToken<'c>);
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum GcContextState {
    Normal,
    Gc,
}

struct GcContextRaw<'c> {
    state: Cell<GcContextState>,
    head: Box<GcBox<'c, ()>>,
    tail: Box<GcBox<'c, ()>>,
}

impl<'c> GcContextRaw<'c> {
    fn gc(&self) {
        match self.state.get() {
            GcContextState::Normal => {
                self.state.set(GcContextState::Gc);
                // TODO
                self.state.set(GcContextState::Normal);
            }
            GcContextState::Gc => {}
        }
    }
}

impl<'c> Drop for GcContextRaw<'c> {
    fn drop(&mut self) {
        self.gc();
    }
}

pub struct GcContext<'c> {
    raw: GcContextRaw<'static>,
    marker: PhantomData<*mut &'c ()>,
}

impl<'c> GcContext<'c> {
    fn raw(&self) -> &GcContextRaw<'c> {
        unsafe { &*((&self.raw as *const GcContextRaw<'static>).cast()) }
    }

    pub fn new() -> Self {
        let head = Box::new(GcBox::new(()));
        head.info.root.set(1);
        let tail = Box::new(GcBox::new(()));
        tail.info.root.set(1);

        head.info.next.set(GcBoxPtr::from_ref(tail.deref()));
        tail.info.prev.set(GcBoxPtr::from_ref(head.deref()));
        Self {
            raw: GcContextRaw {
                state: Cell::new(GcContextState::Normal),
                head,
                tail,
            },
            marker: PhantomData,
        }
    }

    pub fn alloc<T: GcTarget<'c>>(&'c self, value: T) -> GcRoot<'c, T> {
        let raw = self.raw();

        let value = Box::new(GcBox::new(value));
        let value = Box::into_raw(value);
        let value_ref = unsafe { &*value };
        let value_ptr = GcBoxPtr::from_ref(value_ref);

        let tail = raw.tail.deref();
        let prev = tail.info.prev.get();

        value_ref.info.prev.set(prev);
        value_ref.info.next.set(GcBoxPtr::from_ref(tail));
        unsafe { (*prev.as_ptr()).info.next.set(value_ptr) };
        tail.info.prev.set(value_ptr);

        unsafe { GcRoot::from_ref(value_ref) }
    }

    pub fn gc(&self) {
        self.raw().gc()
    }
}

#[macro_export]
macro_rules! trace_none {
    ($type:ty) => {
        impl<'c> $crate::GcTarget<'c> for $type {
            fn trace(&self, token: &mut $crate::GcTraceToken<'c>) {
                let _ = token;
            }
        }
    };
}

#[test]
fn test() {
    struct D;
    impl Drop for D {
        fn drop(&mut self) {
            println!("Drop for D");
        }
    }
    trace_none!(D);

    let x = GcContext::new();
    let y = x.alloc(D);
    let z = GcObject::from(&y);
    let w = z.to_root();
}
