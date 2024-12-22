# Contributing / Developing Guide for this project

## Build the project

First of all, this is a normal Rust *binary* project. So normal cargo commands apply:

```bash
cargo check
cargo clippy
cargo build
cargo test
```

Moreoever, the major purpose of this project is to provide a WebAssembly (wasm) binding for Apache DataFusion. To build the WASM binary, we need a few more tools:

- [`wasm-pack`](https://github.com/rustwasm/wasm-pack/): a convenient tool for building, testing, packaging and releasing Rust WASM projects.
- [`wasm-opt`]() (optional) optimizes `wasm` binary to reduce the size. Normally `wasm-pack` will download this tool for you, but at present it is (not working properly)[https://github.com/rustwasm/wasm-pack/issues/1378]. So you may need to install it manually.
- browsers (optional) to test the WASM binary in a headless browser.

Command to install them:

```bash
cargo install wasm-pack
cargo install wasm-opt
```

To build the WASM binary, run:

```bash
wasm-pack build --target web
```

This will generate a `pkg` directory containing the WASM binary and a JavaScript wrapper. You can use it as a npm package. The `--target web` specifies that the WASM binary is for web usage (inside browser), other options like `nodejs` or `deno` also exist if you want to use it in other environments.

## Publish a release

Unlike normal Rust projects, this project is not suited to be published to crates.io, but npmjs.org instead. To publish a release, you need to:

- Check if everything is well tested and documented.
- Bump the version in `Cargo.toml`
- Run `wasm-pack build --target web` to generate the WASM binary
- Run `wasm-pack publish` to publish the package to npmjs.org

## Develop

### Expose new APIs

All the APIs exposed to JavaScript need to be annotated `#[wasm_bindgen]`. For example, the core entry point is defined as follows

```rust
#[wasm_bindgen]
pub struct DataFusionContext {
    // omitted...
}
```

### Logging and Panicking

To log into stdout, use whatever you are familiar with like in other normal Rust libraries. And to log into JavaScript web console, use `crate::console::log`, which is a FFI bridged function to `console.log` in JavaScript:

```rust
console::log("Hey");
```

Your logic might panic in some cases, when this happens across FFI boundry it would become hard to trace down. Hence we add a special panic hook in `lib.rs` which will (ideally) log the panic stack into JavaScript console. Please file an issue if you find it not working.

### Normalize deps

The WASM target is very strict on aspects that we may not aware of usually. Like, it doesn't allow multi-threading, it requires futures to be `Send`, etc. So we need to carefully writing our code, as well as choosing deps. It's hard to avoid using some third-party code that would run into this kind of issue. Here are some tips that might help when it happens:

- Disable unnecessary features whenever possible. Some deps have default features that are not necessary for our project. For example, `opendal` will enable tokio runtime by default and fail to compile in WASM target. We can disable it by specifying `default-features = false` in `Cargo.toml`.
- Use `cargo tree` to find out which dep is causing the issue. The Rust/Cargo dep-resolver will, unfortunately, "infect" the whole project with the same feature settings, even the code is not used. E.g.: the default tokio feature from `opendal` will enable `rt` feature in `tokio`, and causes all occurrences of `tokio` in the dep tree are affected. In this case we can use `cargo tree -i tokio -e features` to inspect who is using `tokio` with unexpected features enabled.
- Check compile issues frequently with `wasm-pack build --target web --no-opt`, especially when you are adding new deps or features. The `--no-opt` flag will disable wasm-opt, which will take lots of time.
