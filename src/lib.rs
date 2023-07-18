use std::any::type_name;
use std::cell::{Cell, UnsafeCell};
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::mem::{forget, transmute, ManuallyDrop};
use std::ops::Deref;
use std::ptr::NonNull;

use log::{info, trace, warn};

pub mod trace;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum GcState {
    Active,
    Dropped,
    Tracked,
    Untracked,
}

#[derive(Debug)]
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
    fn new(value: T) -> Self {
        let mut r = Self {
            metadata: GcBoxDynPtr::from_ptr(std::ptr::null::<GcBox<'c, T>>()).metadata,
            info: GcInfo {
                next: Cell::new(GcBoxPtr::null()),
                prev: Cell::new(GcBoxPtr::null()),
                state: Cell::new(GcState::Active),
                root: Cell::new(0),
                count: Cell::new(0),
            },
            value: UnsafeCell::new(ManuallyDrop::new(value)),
        };
        r.metadata = GcBoxDynPtr::from_ptr(&r).metadata;
        r
    }

    fn alloc(value: T) -> *mut Self {
        let r = Box::into_raw(Box::new(Self::new(value)));
        trace!("alloc {} {:?}", type_name::<T>(), r);
        r
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> GcBox<'c, T> {
    unsafe fn free(this: *const Self) {
        trace!("free {:?}", this);
        drop(Box::from_raw(this.cast_mut()));
    }

    unsafe fn remove(this: *const Self) {
        let r = &*this;
        let prev = r.info.prev.get();
        let next = r.info.next.get();
        (*prev.as_ptr()).info.next.set(next);
        (*next.as_ptr()).info.prev.set(prev);
        Self::free(this);
    }

    unsafe fn drop_value(&self) {
        trace!("drop {:?}", self as *const Self);
        ManuallyDrop::drop(&mut *self.value.get());
    }

    unsafe fn check_ref(this: *const Self) {
        let r = &*this;
        match r.info.state.get() {
            GcState::Active => {
                if r.info.root.get() == 0 && r.info.count.get() == 0 {
                    r.info.state.set(GcState::Dropped);
                    Self::drop_value(r);
                    Self::remove(this);
                }
            }
            GcState::Dropped => Self::remove(this),
            GcState::Tracked | GcState::Untracked => {}
        }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> Debug for GcBox<'c, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcBox")
            .field("addr", &(self as *const _))
            .field("metadata", &(self.metadata as *const _))
            .field("next", &self.info.next.get())
            .field("prev", &self.info.prev.get())
            .field("root", &self.info.root.get())
            .field("count", &self.info.count.get())
            .finish()
    }
}

#[derive(Copy, Clone)]
#[repr(C)]
struct GcBoxDynPtr<'c> {
    ptr: *const (),
    metadata: &'static (),
    marker: PhantomData<*const GcBox<'c, dyn GcTarget<'c>>>,
}

impl<'c> Debug for GcBoxDynPtr<'c> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcBoxDynPtr")
            .field("ptr", &self.ptr)
            .field("metadata", &(self.metadata as *const _))
            .finish()
    }
}

impl<'c> GcBoxDynPtr<'c> {
    const fn from_part(ptr: *const (), metadata: &'static ()) -> Self {
        Self {
            ptr,
            metadata,
            marker: PhantomData,
        }
    }

    const fn from_ptr(ptr: *const GcBox<'c, dyn GcTarget<'c>>) -> Self {
        unsafe { transmute(ptr) }
    }

    const fn as_ptr(self) -> *const GcBox<'c, dyn GcTarget<'c>> {
        unsafe { transmute(self) }
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
#[repr(transparent)]
struct GcBoxPtr<'c> {
    ptr: *const (),
    marker: PhantomData<*const GcBox<'c, dyn GcTarget<'c>>>,
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

    const fn from_ref<T: GcTarget<'c> + ?Sized>(ptr: *const GcBox<'c, T>) -> Self {
        Self::from_ptr(ptr as *const ())
    }

    fn as_ptr(self) -> *const GcBox<'c, dyn GcTarget<'c>> {
        let metadata = unsafe { *self.ptr.cast::<&'static ()>() };
        GcBoxDynPtr::from_part(self.ptr, metadata).as_ptr()
    }
}

impl<'c> Debug for GcBoxPtr<'c> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.ptr, f)
    }
}

pub struct GcRootThin<'c> {
    ptr: NonNull<()>,
    marker: PhantomData<GcRoot<'c, dyn GcTarget<'c>>>,
}

impl<'c> Drop for GcRootThin<'c> {
    fn drop(&mut self) {
        unsafe {
            let node = self.as_ref();
            node.info.root.set(node.info.root.get() - 1);
            GcBox::check_ref(self.as_ptr());
        }
    }
}

impl<'c> GcRootThin<'c> {
    unsafe fn from_ptr(ptr: *const GcBox<'c, dyn GcTarget<'c>>) -> Self {
        let r = &*ptr;
        r.info.root.set(r.info.root.get() + 1);
        Self {
            ptr: NonNull::from(r).cast(),
            marker: PhantomData,
        }
    }

    fn as_ptr(&self) -> *const GcBox<'c, dyn GcTarget<'c>> {
        GcBoxPtr::from_ptr(self.ptr.as_ptr()).as_ptr()
    }

    fn as_ref(&self) -> &GcBox<'c, dyn GcTarget<'c>> {
        unsafe { &*self.as_ptr() }
    }

    pub fn downgrade(&self) -> GcObjectThin<'c> {
        unsafe { GcObjectThin::from_ptr(self.as_ptr()) }
    }

    pub fn cast_fat(self) -> GcRoot<'c, dyn GcTarget<'c>> {
        let r = GcRoot {
            ptr: NonNull::from(self.as_ref()),
        };
        forget(self);
        r
    }
}

impl<'c> Deref for GcRootThin<'c> {
    type Target = dyn GcTarget<'c>;

    fn deref(&self) -> &Self::Target {
        unsafe { (&*self.as_ref().value.get()).deref() }
    }
}

impl<'c> Clone for GcRootThin<'c> {
    fn clone(&self) -> Self {
        unsafe { Self::from_ptr(self.as_ref()) }
    }
}

pub struct GcRoot<'c, T: GcTarget<'c> + ?Sized> {
    ptr: NonNull<GcBox<'c, T>>,
}

impl<'c, T: GcTarget<'c> + ?Sized> GcRoot<'c, T> {
    unsafe fn from_ptr(ptr: *const GcBox<'c, T>) -> Self {
        let r = &*ptr;
        r.info.root.set(r.info.root.get() + 1);
        Self {
            ptr: NonNull::from(r),
        }
    }

    fn as_ptr(&self) -> *const GcBox<'c, T> {
        self.ptr.as_ptr()
    }

    fn as_ref(&self) -> &GcBox<'c, T> {
        unsafe { &*self.as_ptr() }
    }

    pub fn downgrade(&self) -> GcObject<'c, T> {
        unsafe { GcObject::from_ptr(self.as_ptr()) }
    }

    pub fn cast_dyn(self) -> GcRoot<'c, dyn GcTarget<'c>> {
        unsafe {
            let r = GcRoot {
                ptr: NonNull::new_unchecked(
                    GcBoxPtr::from_ref(self.ptr.as_ref()).as_ptr().cast_mut(),
                ),
            };
            forget(self);
            r
        }
    }

    pub fn cast_thin(self) -> GcRootThin<'c> {
        unsafe {
            let r = GcRootThin {
                ptr: NonNull::new_unchecked(GcBoxPtr::from_ref(self.ptr.as_ref()).ptr.cast_mut()),
                marker: PhantomData,
            };
            forget(self);
            r
        }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> Clone for GcRoot<'c, T> {
    fn clone(&self) -> Self {
        unsafe { Self::from_ptr(self.as_ptr()) }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> Deref for GcRoot<'c, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { (&*self.as_ref().value.get()).deref() }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> Drop for GcRoot<'c, T> {
    fn drop(&mut self) {
        unsafe {
            let node = self.as_ref();
            node.info.root.set(node.info.root.get() - 1);
            GcBox::check_ref(self.as_ptr());
        }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> GcTarget<'c> for GcRoot<'c, T> {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        let _ = token;
    }
}

pub struct GcObjectThin<'c> {
    ptr: NonNull<()>,
    marker: PhantomData<GcObject<'c, dyn GcTarget<'c>>>,
}

impl<'c> GcObjectThin<'c> {
    unsafe fn from_ptr(ptr: *const GcBox<'c, dyn GcTarget<'c>>) -> Self {
        let r = &*ptr;
        r.info.count.set(r.info.count.get() + 1);
        Self {
            ptr: NonNull::from(r).cast(),
            marker: PhantomData,
        }
    }

    fn as_ptr(&self) -> *const GcBox<'c, dyn GcTarget<'c>> {
        GcBoxPtr::from_ptr(self.ptr.as_ptr()).as_ptr()
    }

    fn as_ref(&self) -> &GcBox<'c, dyn GcTarget<'c>> {
        unsafe { &*self.as_ptr() }
    }

    pub fn upgrade(&self) -> Option<GcRootThin<'c>> {
        let r = self.as_ref();
        match r.info.state.get() {
            GcState::Active | GcState::Tracked => unsafe {
                Some(GcRootThin::from_ptr(self.as_ptr()))
            },
            GcState::Dropped | GcState::Untracked => None,
        }
    }

    pub fn cast_fat(self) -> GcObject<'c, dyn GcTarget<'c>> {
        let r = GcObject {
            ptr: NonNull::from(self.as_ref()),
        };
        forget(self);
        r
    }
}

impl<'c> Drop for GcObjectThin<'c> {
    fn drop(&mut self) {
        unsafe {
            let node = self.as_ref();
            node.info.count.set(node.info.count.get() - 1);
            GcBox::check_ref(self.as_ptr());
        }
    }
}

impl<'c> Clone for GcObjectThin<'c> {
    fn clone(&self) -> Self {
        unsafe { Self::from_ptr(self.as_ref()) }
    }
}

pub struct GcObject<'c, T: GcTarget<'c> + ?Sized> {
    ptr: NonNull<GcBox<'c, T>>,
}

impl<'c, T: GcTarget<'c> + ?Sized> GcObject<'c, T> {
    unsafe fn from_ptr(ptr: *const GcBox<'c, T>) -> Self {
        let r = &*ptr;
        r.info.count.set(r.info.count.get() + 1);
        Self {
            ptr: NonNull::from(r),
        }
    }

    fn as_ptr(&self) -> *const GcBox<'c, T> {
        self.ptr.as_ptr()
    }

    fn as_ref(&self) -> &GcBox<'c, T> {
        unsafe { &*self.as_ptr() }
    }

    pub fn upgrade(&self) -> Option<GcRoot<'c, T>> {
        let r = self.as_ref();
        match r.info.state.get() {
            GcState::Active | GcState::Tracked => unsafe { Some(GcRoot::from_ptr(self.as_ptr())) },
            GcState::Dropped | GcState::Untracked => None,
        }
    }

    pub fn cast_dyn(self) -> GcObject<'c, dyn GcTarget<'c>> {
        unsafe {
            let r = GcObject {
                ptr: NonNull::new_unchecked(
                    GcBoxPtr::from_ref(self.ptr.as_ref()).as_ptr().cast_mut(),
                ),
            };
            forget(self);
            r
        }
    }

    pub fn cast_thin(this: Self) -> GcObjectThin<'c> {
        unsafe {
            let r = GcObjectThin {
                ptr: NonNull::new_unchecked(GcBoxPtr::from_ref(this.ptr.as_ref()).ptr.cast_mut()),
                marker: PhantomData,
            };
            forget(this);
            r
        }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> Clone for GcObject<'c, T> {
    fn clone(&self) -> Self {
        unsafe { Self::from_ptr(self.as_ptr()) }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> Drop for GcObject<'c, T> {
    fn drop(&mut self) {
        unsafe {
            let node = self.as_ref();
            node.info.count.set(node.info.count.get() - 1);
            GcBox::check_ref(self.as_ptr());
        }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> GcTarget<'c> for GcObject<'c, T> {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        token.accept(self);
    }
}

pub struct GcTraceToken<'c> {
    head: GcBoxPtr<'c>,
}

impl<'c> GcTraceToken<'c> {
    unsafe fn push(&mut self, node: GcBoxPtr<'c>) {
        let node = &*node.as_ptr();
        node.info.next.set(self.head);
        self.head = GcBoxPtr::from_ref(node);
    }

    unsafe fn pop(&mut self) -> Option<GcBoxPtr<'c>> {
        let r = self.head;
        if r != GcBoxPtr::null() {
            self.head = (*r.as_ptr()).info.next.get();
            Some(r)
        } else {
            None
        }
    }

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

struct GcNodeBackIter<'c> {
    node: GcBoxPtr<'c>,
}

impl<'c> GcNodeBackIter<'c> {
    unsafe fn clone(&self) -> Self {
        Self { node: self.node }
    }

    fn steal(gc: &GcContextRaw<'c>) -> Self {
        let head = gc.head.deref();
        let tail = gc.tail.deref();
        let left = gc.head.info.next.get();
        let right = gc.tail.info.prev.get();
        if left == GcBoxPtr::from_ref(tail) {
            debug_assert!(right == GcBoxPtr::from_ref(head));
            return Self {
                node: GcBoxPtr::null(),
            };
        }

        head.info.next.set(GcBoxPtr::from_ref(tail));
        tail.info.prev.set(GcBoxPtr::from_ref(head));
        unsafe {
            (*left.as_ptr()).info.prev.set(GcBoxPtr::null());
            (*right.as_ptr()).info.next.set(GcBoxPtr::null());
            Self { node: right }
        }
    }

    fn is_empty(&self) -> bool {
        self.node == GcBoxPtr::null()
    }
}

impl<'c> Iterator for GcNodeBackIter<'c> {
    type Item = NonNull<GcBox<'c, dyn GcTarget<'c>>>;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.node;
        if current == GcBoxPtr::null() {
            return None;
        }
        let current = unsafe { &*current.as_ptr() };
        self.node = current.info.prev.get();
        Some(NonNull::from(current))
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum GcContextState {
    Normal,
    Gc,
}

#[derive(Debug)]
struct GcContextRaw<'c> {
    auto_gc: usize,
    state: Cell<GcContextState>,
    alloc_count: Cell<usize>,
    head: Box<GcBox<'c, ()>>,
    tail: Box<GcBox<'c, ()>>,
}

impl<'c> GcContextRaw<'c> {
    fn new() -> Self {
        let head = Box::new(GcBox::new(()));
        head.info.root.set(1);
        let tail = Box::new(GcBox::new(()));
        tail.info.root.set(1);

        head.info.next.set(GcBoxPtr::from_ref(tail.deref()));
        tail.info.prev.set(GcBoxPtr::from_ref(head.deref()));

        Self {
            state: Cell::new(GcContextState::Normal),
            auto_gc: 0,
            alloc_count: Cell::new(0),
            head,
            tail,
        }
    }

    fn alloc<T: GcTarget<'c>>(&'c self, value: T) -> GcRoot<'c, T> {
        if self.auto_gc != 0 {
            let alloc_count = self.alloc_count.get() + 1;
            if alloc_count == self.auto_gc {
                self.alloc_count.set(0);
                info!("auto gc");
                self.gc();
            } else {
                self.alloc_count.set(alloc_count);
            }
        }

        let value = GcBox::alloc(value);
        let value_ref = unsafe { &*value };
        let value_ptr = GcBoxPtr::from_ref(value_ref);

        let tail = self.tail.deref();
        let prev = tail.info.prev.get();

        value_ref.info.prev.set(prev);
        value_ref.info.next.set(GcBoxPtr::from_ref(tail));
        unsafe { (*prev.as_ptr()).info.next.set(value_ptr) };
        tail.info.prev.set(value_ptr);

        unsafe { GcRoot::from_ptr(value) }
    }

    fn gc(&self) {
        info!("call gc");
        match self.state.get() {
            GcContextState::Normal => {
                struct Guard<'s, 'c>(&'s GcContextRaw<'c>, std::time::Instant);

                impl<'s, 'c> Drop for Guard<'s, 'c> {
                    fn drop(&mut self) {
                        self.0.state.set(GcContextState::Normal);
                        info!("end gc {:?}", self.1.elapsed());
                    }
                }

                info!("begin gc");
                self.state.set(GcContextState::Gc);
                let _guard = Guard(self, std::time::Instant::now());

                let iter = GcNodeBackIter::steal(self);
                if iter.is_empty() {
                    return;
                }
                unsafe {
                    let mut token = GcTraceToken {
                        head: GcBoxPtr::null(),
                    };

                    let mut count = 0;
                    for node in iter.clone() {
                        count += 1;
                        let n = node.as_ref();
                        debug_assert!(n.info.state.get() == GcState::Active);
                        if n.info.root.get() != 0 {
                            n.info.state.set(GcState::Tracked);
                            token.push(GcBoxPtr::from_ref(n));
                        } else {
                            n.info.state.set(GcState::Untracked);
                        }
                    }

                    info!("trace {} target", count);

                    while let Some(node) = token.pop() {
                        let node = &*node.as_ptr();
                        let value = &*node.value.get();
                        value.trace(&mut token);
                    }

                    let mut hold_count = 0;
                    let mut drop_count = 0;

                    let mut that = self.head.info.next.get();
                    for node in iter {
                        let n = node.as_ref();
                        let node = GcBoxPtr::from_ref(n);
                        match n.info.state.get() {
                            GcState::Active | GcState::Dropped => unreachable!(),
                            GcState::Tracked => {
                                hold_count += 1;
                                n.info.next.set(that);
                                (*that.as_ptr()).info.prev.set(node);
                                n.info.state.set(GcState::Active);
                                that = node;
                            }
                            GcState::Untracked => {
                                drop_count += 1;
                                GcBox::drop_value(n);
                                GcBox::free(node.as_ptr());
                            }
                        }
                    }

                    info!("hold {} target", hold_count);
                    info!("drop {} target", drop_count);

                    let head = self.head.deref();
                    (*that.as_ptr()).info.prev.set(GcBoxPtr::from_ref(head));
                    head.info.next.set(that);
                }
            }
            GcContextState::Gc => {}
        }
    }
}

impl<'c> Drop for GcContextRaw<'c> {
    fn drop(&mut self) {
        info!("drop gc");
        self.gc();
        let iter = GcNodeBackIter::steal(self);
        let mut leak_count = 0;
        for node in iter {
            trace!("leak {:?}", node.as_ptr());
            leak_count += 1;
            let n = unsafe { node.as_ref() };
            debug_assert!(n.info.state.get() == GcState::Active);
            n.info.prev.set(GcBoxPtr::null());
            n.info.next.set(GcBoxPtr::null());
            n.info.root.set(n.info.root.get() + 1);
        }
        if leak_count != 0 {
            warn!("leak {} target", leak_count);
        }
    }
}

#[derive(Debug)]
pub struct GcContext<'c> {
    inner: GcContextRaw<'static>,
    marker: PhantomData<*mut &'c ()>,
}

impl<'c> GcContext<'c> {
    fn inner(&self) -> &GcContextRaw<'c> {
        unsafe { &*((&self.inner as *const GcContextRaw<'static>).cast()) }
    }

    fn inner_mut(&mut self) -> &mut GcContextRaw<'c> {
        unsafe { &mut *((&mut self.inner as *mut GcContextRaw<'static>).cast()) }
    }

    pub fn new() -> Self {
        Self {
            inner: GcContextRaw::new(),
            marker: PhantomData,
        }
    }

    pub fn set_auto_gc(&mut self, auto_gc: usize) {
        self.inner_mut().auto_gc = auto_gc;
        self.inner_mut().alloc_count.set(0);
    }

    pub fn alloc<T: GcTarget<'c>>(&'c self, value: T) -> GcRoot<'c, T> {
        self.inner().alloc(value)
    }

    pub fn gc(&self) {
        self.inner().gc()
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
    let _ = env_logger::try_init();

    struct Foo<'c> {
        r: std::cell::RefCell<Option<GcObject<'c, Self>>>,
    }

    impl<'c> Drop for Foo<'c> {
        fn drop(&mut self) {
            println!("Drop for Foo");
        }
    }

    impl<'c> GcTarget<'c> for Foo<'c> {
        fn trace(&self, token: &mut GcTraceToken<'c>) {
            self.r.trace(token);
        }
    }

    let context = GcContext::new();
    let x = context.alloc(Foo {
        r: std::cell::RefCell::new(None),
    });
    *x.r.borrow_mut() = Some(x.downgrade());
    context.gc();
    *x.r.borrow_mut() = None;
}
