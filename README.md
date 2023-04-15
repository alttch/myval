<h2>
  Myval - a lightweight Apache Arrow data frame for Rust
  <a href="https://crates.io/crates/myval"><img alt="crates.io page" src="https://img.shields.io/crates/v/myval.svg"></img></a>
  <a href="https://docs.rs/myval"><img alt="docs.rs page" src="https://docs.rs/myval/badge.svg"></img></a>
</h2>

## What is Myval?

Mýval (pronounced as [m'ival]) is translated from Czech as raccoon.

## Why not a bear-name?

The common name for raccoon in Czech is "medvídek mýval" which can be
translated as "little bear".

## But there is Polars?

Myval is not a competitor of Polars. Myval is a lightweight Arrow data frame
which is focused on data transformation and IPC.

Because Arrow has got the standardized data layout, data frames can be
converted to Polars and vice-versa with zero-copy:

```rust
let polars_df = polars::frame::DataFrame::from(myval_df);
let myval_df = myval::DataFrame::from(polars_df);
```

As well as Polars, Myval is based on [arrow2](https://crates.io/crates/arrow2).

## Some tricks

### IPC

Consider there is a Arrow stream block (Schema+Chunk) received from e.g. RPC or
Pub/Sub. Convert the block into a Myval data frame:

```rust
let df = myval::DataFrame::from_ipc_block(&buf).unwrap();
```

Need to send a data frame back? Convert it to Arrow stream block with a single
line of code:

```rust
let buf = df.into_ipc_block();
```

Need to send sliced? No problem, there are methods which can easily return
sliced series, sliced data frames or IPC chunks.

### Overriding data types

Consider there is a i64-column "time" which contains nanosecond timestamps. Let
us override its data type:

```rust
use myval::{DataType, TimeUnit};

df.set_data_type("time",
    DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap();
```

### Parsing numbers from strings

Consider there is a utf8-column "value" which should be parsed to floats:

```rust
df.parse_float("value").unwrap();
```

### Set column ordering

Consider there is a Myval data frame with columns "voltage", "temp1", "temp2",
"temp3" which has received data from a server column-by-column in random
ordering. Let us correct the ordering back to normal:

```rust
df.set_ordering(&["voltage", "temp1", "temp2", "temp3"]);
```

### Others

Check the documentation: <https://docs.rs/myval>

## Limitations

* Myval is not designed for data engineering. Use Polars.

* Myval series can contain a single chunk only and there are no plans to extend
this. When a Polars data frame with multiple chunks is converted to Myval, the
chunks are automatically aggregated.
