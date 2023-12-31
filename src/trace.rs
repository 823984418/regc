use std::any::TypeId;
use std::cell::{Cell, RefCell};
use std::collections::{LinkedList, VecDeque};
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::marker::{PhantomData, PhantomPinned};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::{trace_none, GcTarget, GcTraceToken};

impl<'c, T: GcTarget<'c> + ?Sized> GcTarget<'c> for &T {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        T::trace(*self, token);
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> GcTarget<'c> for &mut T {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        T::trace(*self, token);
    }
}

impl<'c, T: GcTarget<'c>> GcTarget<'c> for [T] {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        for i in self {
            i.trace(token);
        }
    }
}

impl<'c, T: GcTarget<'c>> GcTarget<'c> for Vec<T> {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        for i in self {
            i.trace(token);
        }
    }
}

impl<'c, T: GcTarget<'c>> GcTarget<'c> for LinkedList<T> {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        for i in self {
            i.trace(token);
        }
    }
}

impl<'c, T: GcTarget<'c>> GcTarget<'c> for VecDeque<T> {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        for i in self {
            i.trace(token);
        }
    }
}

impl<'c, T: GcTarget<'c>> GcTarget<'c> for Option<T> {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        if let Some(x) = self {
            x.trace(token);
        }
    }
}

impl<'c, T: GcTarget<'c>, E: GcTarget<'c>> GcTarget<'c> for Result<T, E> {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        match self {
            Ok(x) => x.trace(token),
            Err(x) => x.trace(token),
        }
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> GcTarget<'c> for Box<T> {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        T::trace(self, token);
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> GcTarget<'c> for Rc<T> {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        T::trace(self, token);
    }
}

impl<'c, T: GcTarget<'c> + Copy> GcTarget<'c> for Cell<T> {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        self.get().trace(token);
    }
}

impl<'c, T: GcTarget<'c> + ?Sized> GcTarget<'c> for RefCell<T> {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        T::trace(self.borrow().deref(), token);
    }
}

impl<'c, T> GcTarget<'c> for PhantomData<T> {
    fn trace(&self, token: &mut GcTraceToken<'c>) {
        let _ = token;
    }
}

trace_none!(PhantomPinned);

trace_none!(bool);
trace_none!(i8);
trace_none!(u8);
trace_none!(i16);
trace_none!(u16);
trace_none!(i32);
trace_none!(u32);
trace_none!(i64);
trace_none!(u64);
trace_none!(i128);
trace_none!(u128);
trace_none!(isize);
trace_none!(usize);
trace_none!(f32);
trace_none!(f64);
trace_none!(str);
trace_none!(String);
trace_none!(OsStr);
trace_none!(OsString);
trace_none!(Path);
trace_none!(PathBuf);
trace_none!(TypeId);
trace_none!(File);
trace_none!(Instant);
trace_none!(Duration);

macro_rules! trace_tuple {
    ($($name:ident)*) => {
        impl<'c, $($name: $crate::GcTarget<'c>),*> $crate::GcTarget<'c> for ($($name,)*) {
            #[allow(unused_variables)]
            fn trace(&self, token: &mut $crate::GcTraceToken<'c>) {
                #[allow(non_snake_case)]
                let ($($name,)*) = self;
                $($name.trace(token);)*
            }
        }
    };
}

macro_rules! trace_tuple_all {
    () => {
        trace_tuple!();
    };
    ($name:ident $($names:ident)*) => {
        trace_tuple_all!($($names)*);
        trace_tuple!($name $($names)*);
    };
}

trace_tuple_all!(T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12 T13 T14 T15);
