# ReGc
A garbage collector that mixes Reference counting and mark sweeping

### Example

#### Rust
```rust
env_logger::init();

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
```

#### Output
```
[2023-07-18T00:56:30Z TRACE regc] alloc regc::test::Foo 0x2876afc19e0
[2023-07-18T00:56:30Z INFO  regc] call gc
[2023-07-18T00:56:30Z INFO  regc] begin gc
[2023-07-18T00:56:30Z INFO  regc] trace 1 target
[2023-07-18T00:56:30Z INFO  regc] hold 1 target
[2023-07-18T00:56:30Z INFO  regc] drop 0 target
[2023-07-18T00:56:30Z INFO  regc] end gc 19µs
[2023-07-18T00:56:30Z TRACE regc] drop 0x2876afc19e0
[2023-07-18T00:56:30Z TRACE regc] free 0x2876afc19e0
[2023-07-18T00:56:30Z INFO  regc] drop gc
[2023-07-18T00:56:30Z INFO  regc] call gc
[2023-07-18T00:56:30Z INFO  regc] begin gc
[2023-07-18T00:56:30Z INFO  regc] end gc 200ns
Drop for Foo
```