use std::any::type_name;
use std::cell::Cell;
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

struct GcInfo<'c> {
    next: Cell<Option<NonNullGcBox<'c>>>,
    prev: Cell<Option<NonNullGcBox<'c>>>,
    state: Cell<GcState>,
    root: Cell<usize>,
    count: Cell<usize>,
}

#[repr(C)]
struct GcBox<'c, T: GcTarget<'c> + ?Sized> {
    metadata: &'static (),
    info: GcInfo<'c>,
    value: ManuallyDrop<T>,
}

impl<'c, T: GcTarget<'c> + 'c> GcBox<'c, T> {
    fn new(value: T) -> Self {
        let mut r = Self {
            metadata: GcBoxDynPtr::from_ptr(std::ptr::null::<GcBox<'c, T>>()).metadata,
            info: GcInfo {
                next: Cell::new(None),
                prev: Cell::new(None),
                state: Cell::new(GcState::Active),
                root: Cell::new(0),
                count: Cell::new(0),
            },
            value: ManuallyDrop::new(value),
        };
        r.metadata = GcBoxDynPtr::from_ptr(&r).metadata;
        r
    }

    fn alloc(value: T) -> NonNull<Self> {
        let r = Box::into_raw(Box::new(Self::new(value)));
        trace!("alloc {} {:?}", type_name::<T>(), r as *mut ());
        unsafe { NonNull::new_unchecked(r) }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> GcBox<'c, T> {
    fn value(&self) -> *const T {
        self.value.deref()
    }

    unsafe fn free(this: NonNull<Self>) {
        trace!("free {:?}", this.as_ptr() as *mut ());
        drop(Box::from_raw(this.as_ptr()));
    }

    unsafe fn remove(this: NonNull<Self>) {
        let r = this.as_ref();
        let prev = r.info.prev.get();
        let next = r.info.next.get();
        prev.unwrap_unchecked().as_ref().info.next.set(next);
        next.unwrap_unchecked().as_ref().info.prev.set(prev);
        Self::free(this);
    }

    unsafe fn drop_value(&mut self) {
        trace!("drop {:?}", self as *mut Self as *mut ());
        ManuallyDrop::drop(&mut self.value);
    }

    unsafe fn check_ref(mut this: NonNull<Self>) {
        let r = this.as_ref();
        match r.info.state.get() {
            GcState::Active => {
                if r.info.root.get() == 0 && r.info.count.get() == 0 {
                    r.info.state.set(GcState::Dropped);
                    Self::drop_value(this.as_mut());
                    Self::remove(this);
                }
            }
            GcState::Dropped => Self::remove(this),
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

    const fn from_ptr(ptr: *const GcBox<'c, dyn GcTarget<'c> + 'c>) -> Self {
        unsafe { transmute(ptr) }
    }

    const fn as_mut(self) -> *mut GcBox<'c, dyn GcTarget<'c> + 'c> {
        unsafe { transmute(self) }
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
#[repr(transparent)]
struct NonNullGcBox<'c> {
    ptr: NonNull<()>,
    marker: PhantomData<*const GcBox<'c, dyn GcTarget<'c>>>,
}

impl<'c> NonNullGcBox<'c> {
    const fn from_non_null<T: GcTarget<'c> + ?Sized>(ptr: NonNull<GcBox<'c, T>>) -> Self {
        Self {
            ptr: ptr.cast(),
            marker: PhantomData,
        }
    }

    fn from_ptr<T: GcTarget<'c> + ?Sized>(ptr: *const GcBox<'c, T>) -> Option<Self> {
        NonNull::new(ptr.cast_mut()).map(|ptr| Self {
            ptr: ptr.cast(),
            marker: PhantomData,
        })
    }

    fn as_non_null(self) -> NonNull<GcBox<'c, dyn GcTarget<'c> + 'c>> {
        unsafe {
            let metadata = *self.ptr.as_ptr().cast::<&'static ()>();
            NonNull::new_unchecked(GcBoxDynPtr::from_part(self.ptr.as_ptr(), metadata).as_mut())
        }
    }

    fn as_ptr(self) -> *const GcBox<'c, dyn GcTarget<'c> + 'c> {
        self.as_non_null().as_ptr()
    }

    unsafe fn as_ref(&self) -> &GcBox<'c, dyn GcTarget<'c> + 'c> {
        &*self.as_ptr()
    }
}

impl<'c> Debug for NonNullGcBox<'c> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.ptr, f)
    }
}

pub struct GcRootThin<'c> {
    ptr: NonNullGcBox<'c>,
    marker: PhantomData<GcRoot<'c, dyn GcTarget<'c>>>,
}

impl<'c> Drop for GcRootThin<'c> {
    fn drop(&mut self) {
        unsafe {
            let node = self.ptr.as_ref();
            node.info.root.set(node.info.root.get() - 1);
            GcBox::check_ref(self.ptr.as_non_null());
        }
    }
}

impl<'c> GcRootThin<'c> {
    unsafe fn from_box(ptr: NonNull<GcBox<'c, dyn GcTarget<'c> + 'c>>) -> Self {
        let r = ptr.as_ref();
        r.info.root.set(r.info.root.get() + 1);
        Self {
            ptr: NonNullGcBox::from_non_null(ptr),
            marker: PhantomData,
        }
    }

    pub fn as_ptr(&self) -> *const (dyn GcTarget<'c> + 'c) {
        unsafe { self.ptr.as_ref().value() }
    }

    pub fn downgrade(&self) -> GcObjectThin<'c> {
        unsafe { GcObjectThin::from_box(self.ptr.as_non_null()) }
    }

    pub fn cast_fat(self) -> GcRoot<'c, dyn GcTarget<'c> + 'c> {
        let r = GcRoot {
            ptr: self.ptr.as_non_null(),
        };
        forget(self);
        r
    }
}

impl<'c> Clone for GcRootThin<'c> {
    fn clone(&self) -> Self {
        unsafe { Self::from_box(self.ptr.as_non_null()) }
    }
}

impl<'c> GcTarget<'c> for GcRootThin<'c> {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        let _ = token;
    }
}

impl<'c> Debug for GcRootThin<'c> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.ptr, f)
    }
}

impl<'c> Deref for GcRootThin<'c> {
    type Target = dyn GcTarget<'c> + 'c;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.as_ptr() }
    }
}

pub struct GcRoot<'c, T: GcTarget<'c> + ?Sized> {
    ptr: NonNull<GcBox<'c, T>>,
}

impl<'c, T: GcTarget<'c> + ?Sized> Drop for GcRoot<'c, T> {
    fn drop(&mut self) {
        unsafe {
            let node = self.ptr.as_ref();
            node.info.root.set(node.info.root.get() - 1);
            GcBox::check_ref(self.ptr);
        }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> GcRoot<'c, T> {
    unsafe fn from_box(ptr: NonNull<GcBox<'c, T>>) -> Self {
        let r = ptr.as_ref();
        r.info.root.set(r.info.root.get() + 1);
        Self { ptr }
    }

    pub fn as_ptr(&self) -> *const T {
        unsafe { self.ptr.as_ref().value() }
    }

    pub fn downgrade(&self) -> GcObject<'c, T> {
        unsafe { GcObject::from_box(self.ptr) }
    }

    pub fn cast_dyn(self) -> GcRoot<'c, dyn GcTarget<'c> + 'c> {
        unsafe {
            let r = GcRoot {
                ptr: NonNull::new_unchecked(
                    NonNullGcBox::from_non_null(self.ptr).as_ptr().cast_mut(),
                ),
            };
            forget(self);
            r
        }
    }

    pub fn cast_thin(self) -> GcRootThin<'c> {
        let r = GcRootThin {
            ptr: NonNullGcBox::from_non_null(self.ptr),
            marker: PhantomData,
        };
        forget(self);
        r
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> Clone for GcRoot<'c, T> {
    fn clone(&self) -> Self {
        unsafe { Self::from_box(self.ptr) }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> GcTarget<'c> for GcRoot<'c, T> {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        let _ = token;
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> Debug for GcRoot<'c, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.ptr, f)
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> Deref for GcRoot<'c, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.as_ptr() }
    }
}

pub struct GcObjectThin<'c> {
    ptr: NonNullGcBox<'c>,
    marker: PhantomData<GcObject<'c, dyn GcTarget<'c>>>,
}

impl<'c> Drop for GcObjectThin<'c> {
    fn drop(&mut self) {
        unsafe {
            let node = self.ptr.as_ref();
            node.info.count.set(node.info.count.get() - 1);
            GcBox::check_ref(self.ptr.as_non_null());
        }
    }
}

impl<'c> GcObjectThin<'c> {
    unsafe fn from_box(ptr: NonNull<GcBox<'c, dyn GcTarget<'c> + 'c>>) -> Self {
        let r = ptr.as_ref();
        r.info.count.set(r.info.count.get() + 1);
        Self {
            ptr: NonNullGcBox::from_non_null(ptr),
            marker: PhantomData,
        }
    }

    pub fn as_ptr(&self) -> *const (dyn GcTarget<'c> + 'c) {
        unsafe { self.ptr.as_ref().value() }
    }

    pub fn upgrade(&self) -> Option<GcRootThin<'c>> {
        let r = unsafe { self.ptr.as_ref() };
        match r.info.state.get() {
            GcState::Active | GcState::Tracked => unsafe {
                Some(GcRootThin::from_box(self.ptr.as_non_null()))
            },
            GcState::Dropped | GcState::Untracked => None,
        }
    }

    pub fn cast_fat(self) -> GcObject<'c, dyn GcTarget<'c> + 'c> {
        let r = GcObject {
            ptr: self.ptr.as_non_null(),
        };
        forget(self);
        r
    }
}

impl<'c> Clone for GcObjectThin<'c> {
    fn clone(&self) -> Self {
        unsafe { Self::from_box(self.ptr.as_non_null()) }
    }
}

impl<'c> GcTarget<'c> for GcObjectThin<'c> {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        token.accept_thin(self);
    }
}

impl<'c> Debug for GcObjectThin<'c> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.ptr, f)
    }
}

pub struct GcObject<'c, T: GcTarget<'c> + ?Sized> {
    ptr: NonNull<GcBox<'c, T>>,
}

impl<'c, T: GcTarget<'c> + ?Sized> Drop for GcObject<'c, T> {
    fn drop(&mut self) {
        unsafe {
            let node = self.ptr.as_ref();
            node.info.count.set(node.info.count.get() - 1);
            GcBox::check_ref(self.ptr);
        }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> GcObject<'c, T> {
    unsafe fn from_box(ptr: NonNull<GcBox<'c, T>>) -> Self {
        let r = ptr.as_ref();
        r.info.count.set(r.info.count.get() + 1);
        Self { ptr }
    }

    pub fn as_ptr(&self) -> *const T {
        unsafe { self.ptr.as_ref().value() }
    }

    pub fn upgrade(&self) -> Option<GcRoot<'c, T>> {
        let r = unsafe { &*self.ptr.as_ptr() };
        match r.info.state.get() {
            GcState::Active | GcState::Tracked => unsafe { Some(GcRoot::from_box(self.ptr)) },
            GcState::Dropped | GcState::Untracked => None,
        }
    }

    pub fn cast_dyn(self) -> GcObject<'c, dyn GcTarget<'c> + 'c> {
        let r = GcObject {
            ptr: NonNullGcBox::from_non_null(self.ptr).as_non_null(),
        };
        forget(self);
        r
    }

    pub fn cast_thin(self) -> GcObjectThin<'c> {
        let r = GcObjectThin {
            ptr: NonNullGcBox::from_non_null(self.ptr),
            marker: PhantomData,
        };
        forget(self);
        r
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> Clone for GcObject<'c, T> {
    fn clone(&self) -> Self {
        unsafe { Self::from_box(self.ptr) }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> GcTarget<'c> for GcObject<'c, T> {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        token.accept(self);
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> Debug for GcObject<'c, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.ptr, f)
    }
}

pub struct GcTraceToken<'c> {
    head: Option<NonNullGcBox<'c>>,
}

impl<'c> GcTraceToken<'c> {
    unsafe fn push(&mut self, node: NonNullGcBox<'c>) {
        let node = node.as_ref();
        node.info.next.set(self.head);
        self.head = NonNullGcBox::from_ptr(node);
    }

    unsafe fn pop(&mut self) -> Option<NonNullGcBox<'c>> {
        let r = self.head;
        if r != None {
            self.head = r.unwrap_unchecked().as_ref().info.next.get();
            r
        } else {
            None
        }
    }

    unsafe fn accept_box<T: GcTarget<'c> + ?Sized>(&mut self, value: NonNull<GcBox<'c, T>>) {
        let value = value.as_ref();
        match value.info.state.get() {
            GcState::Untracked => {
                value.info.state.set(GcState::Tracked);
                value.info.next.set(self.head);
                self.head = NonNullGcBox::from_ptr(value);
            }
            GcState::Tracked | GcState::Active | GcState::Dropped => {}
        }
    }

    pub fn accept<T: GcTarget<'c> + ?Sized>(&mut self, value: &GcObject<'c, T>) {
        unsafe { self.accept_box(value.ptr) };
    }

    pub fn accept_thin(&mut self, value: &GcObjectThin<'c>) {
        unsafe { self.accept_box(value.ptr.as_non_null()) };
    }
}

pub trait GcTarget<'c> {
    fn trace(&self, token: &mut GcTraceToken<'c>);
}

struct GcNodeBackIter<'c> {
    node: Option<NonNullGcBox<'c>>,
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
        if left == NonNullGcBox::from_ptr(tail) {
            debug_assert!(right == NonNullGcBox::from_ptr(head));
            return Self { node: None };
        }

        head.info.next.set(NonNullGcBox::from_ptr(tail));
        tail.info.prev.set(NonNullGcBox::from_ptr(head));
        unsafe {
            left.unwrap_unchecked().as_ref().info.prev.set(None);
            right.unwrap_unchecked().as_ref().info.next.set(None);
            Self { node: right }
        }
    }

    fn is_empty(&self) -> bool {
        self.node == None
    }
}

impl<'c> Iterator for GcNodeBackIter<'c> {
    type Item = NonNullGcBox<'c>;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.node;
        if let Some(current) = current {
            self.node = unsafe { current.as_ref().info.prev.get() };
        }
        current
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum GcContextState {
    Normal,
    Gc,
}

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

        head.info.next.set(NonNullGcBox::from_ptr(tail.deref()));
        tail.info.prev.set(NonNullGcBox::from_ptr(head.deref()));

        Self {
            state: Cell::new(GcContextState::Normal),
            auto_gc: 0,
            alloc_count: Cell::new(0),
            head,
            tail,
        }
    }

    fn set_auto_gc(&mut self, auto_gc: usize) {
        self.auto_gc = auto_gc;
        self.alloc_count.set(0);
    }

    fn alloc<T: GcTarget<'c> + 'c>(&'c self, value: T) -> GcRoot<'c, T> {
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
        let value_ref = unsafe { value.as_ref() };
        let value_ptr = NonNullGcBox::from_non_null(value);

        let tail = self.tail.deref();
        let prev = tail.info.prev.get();

        value_ref.info.prev.set(prev);
        value_ref.info.next.set(NonNullGcBox::from_ptr(tail));
        unsafe {
            prev.unwrap_unchecked()
                .as_ref()
                .info
                .next
                .set(Some(value_ptr))
        };
        tail.info.prev.set(Some(value_ptr));

        unsafe { GcRoot::from_box(value) }
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
                    let mut token = GcTraceToken { head: None };

                    let mut count = 0;
                    for node in iter.clone() {
                        count += 1;
                        let n = node.as_ref();
                        debug_assert!(n.info.state.get() == GcState::Active);
                        if n.info.root.get() != 0 {
                            n.info.state.set(GcState::Tracked);
                            token.push(node);
                        } else {
                            n.info.state.set(GcState::Untracked);
                        }
                    }

                    info!("trace {} target", count);

                    while let Some(node) = token.pop() {
                        let node = node.as_ref();
                        let value = &*node.value();
                        value.trace(&mut token);
                    }

                    let mut hold_count = 0;
                    let mut drop_count = 0;

                    let mut that = self.head.info.next.get();
                    for node in iter {
                        let n = node.as_ref();
                        let node = NonNullGcBox::from_ptr(n);
                        match n.info.state.get() {
                            GcState::Active | GcState::Dropped => unreachable!(),
                            GcState::Tracked => {
                                hold_count += 1;
                                n.info.next.set(that);
                                that.unwrap_unchecked().as_ref().info.prev.set(node);
                                n.info.state.set(GcState::Active);
                                that = node;
                            }
                            GcState::Untracked => {
                                drop_count += 1;
                                GcBox::drop_value(
                                    &mut *node.unwrap_unchecked().as_ptr().cast_mut(),
                                );
                                GcBox::free(node.unwrap_unchecked().as_non_null());
                            }
                        }
                    }

                    info!("hold {} target", hold_count);
                    info!("drop {} target", drop_count);

                    let head = self.head.deref();
                    that.unwrap_unchecked()
                        .as_ref()
                        .info
                        .prev
                        .set(NonNullGcBox::from_ptr(head));
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
            n.info.prev.set(None);
            n.info.next.set(None);
            n.info.root.set(n.info.root.get() + 1);
        }
        if leak_count != 0 {
            warn!("leak {} target", leak_count);
        }
    }
}

impl<'c> Debug for GcContextRaw<'c> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcContextRaw")
            .field("auto_gc", &self.auto_gc)
            .field("alloc_count", &self.alloc_count.get())
            .finish()
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
        self.inner_mut().set_auto_gc(auto_gc);
    }

    pub fn alloc<T: GcTarget<'c> + 'c>(&'c self, value: T) -> GcRoot<'c, T> {
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
