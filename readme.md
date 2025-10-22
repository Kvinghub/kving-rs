# Kving

[![crates.io](https://img.shields.io/crates/v/kving.svg)](https://crates.io/crates/kving)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/Kvinghub/kving-rs/blob/master/LICENSE.txt)

`kving` is a key value database implemented by Rust, designed to serve multiple platforms.

## Quick Start

Use in your project's `Cargo.toml`:

```toml
kving = "0.0.1"
```

```rust
// main.rs
use std::path::PathBuf;

use kving::{Config, Kving};

fn main() -> kving::Result<()> {
    let kving = Kving::with_config(
        Config::builder()
            .set_data_dir(PathBuf::from("test_data"))
            .set_name("test_dbname")
            .build(),
    )?;

    kving.put_isize("isize", 0_isize)?;
    kving.put_usize("usize", 0_usize)?;
    kving.put_f32("f32", 0_f32)?;
    kving.put_f64("f64", 0_f64)?;
    kving.put_bool("bool", true)?;
    kving.put_string("string", "Hello Kving.")?;
    kving.put_blob("blob", &b"Hello Kving!!".to_vec())?;

    println!("---------------------------------");
    println!("isize={:?}", kving.get_isize("isize"));
    println!("usize={:?}", kving.get_usize("usize"));
    println!("f32={:?}", kving.get_f32("f32"));
    println!("f64={:?}", kving.get_f64("f64"));
    println!("bool={:?}", kving.get_bool("bool"));
    println!("string={:?}", kving.get_string("string"));
    println!("blob={:?}", kving.get_blob("blob"));
    println!("---------------------------------");

    for key in kving.list_keys()? {
        let value = kving.get_blob(key);
        println!("blob value: {:?}", value);
    }

    Ok(())
}
```

> Note: The current situation is not very stable, please be cautious when using it in production environments.
