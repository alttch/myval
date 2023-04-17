<h2>
  Myval - a lightweight Apache Arrow data frame for Rust
  <a href="https://crates.io/crates/myval"><img alt="crates.io page" src="https://img.shields.io/crates/v/myval.svg"></img></a>
  <a href="https://docs.rs/myval"><img alt="docs.rs page" src="https://docs.rs/myval/badge.svg"></img></a>
</h2>

<img src="https://raw.githubusercontent.com/alttch/myval/main/media/myval_w.png"
width="200" />

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

```rust,ignore
let polars_df = polars::frame::DataFrame::from(myval_df);
let myval_df = myval::DataFrame::from(polars_df);
```

As well as Polars, Myval is based on [arrow2](https://crates.io/crates/arrow2).

## Some tricks

### IPC

Consider there is an Arrow stream block (Schema+Chunk) received from e.g. RPC
or Pub/Sub. Convert the block into a Myval data frame:

```rust,ignore
let df = myval::DataFrame::from_ipc_block(&buf).unwrap();
```

Need to send a data frame back? Convert it to Arrow stream block with a single
line of code:

```rust,ignore
let buf = df.into_ipc_block().unwrap();
```

Need to send sliced? No problem, there are methods which can easily return
sliced series, sliced data frames or IPC chunks.

### Overriding data types

Consider there is an i64-column "time" which contains nanosecond timestamps.
Let us override its data type:

```rust,ignore
use myval::{DataType, TimeUnit};

df.set_data_type("time",
    DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap();
```

### Parsing numbers from strings

Consider there is a utf8-column "value" which should be parsed to floats:

```rust,ignore
df.parse_float("value").unwrap();
```

### Set column ordering

Consider there is a Myval data frame with columns "voltage", "temp1", "temp2",
"temp3" which has received data from a server column-by-column in random
ordering. Let us correct the ordering back to normal:

```rust,ignore
df.set_ordering(&["voltage", "temp1", "temp2", "temp3"]);
```

### Others

Check the documentation: <https://docs.rs/myval>

## Working with databases

Arrow provides several ways working with databases. Myval additionally provides
tools to work with PostgreSQL databases in the easy way via the popular
[sqlx](https://crates.io/crates/sqlx) crate ("postgres" feature must be
enabled):

### Fetching data from a data base

```rust,ignore
let pool = Arc::new(PgPoolOptions::new()
    .connect("postgres://postgres:welcome@localhost/postgres")
    .await.unwrap());
let max_size = 100_000;
let stream = myval::db::postgres::fetch("select * from test".to_owned(),
    Some(max_size), pool.clone());
// the stream returns data frames one by one with max data frame size (in
// bytes) = max_size
while let Some(df) = stream.try_next().await.unwrap() {
    // do some stuff
}
```

Why does the stream object require Arc&lt;PgPool&gt;? There is one important reason:
such stream objects are static and can be stored anywhere, e.g. used as cursors
in a client-server architecture.

### Pushing data into a database

#### Server

```rust,ignore
let df = DataFrame::from_ipc_block(payload).unwrap();
// The first received data frame must have "database" field in its schema
// metadata. Next data frames can go without it.
if let Some(dbparams) = df.metadata().get("database") {
    let params: myval::db::postgres::Params = serde_json::from_str(dbparams)
        .unwrap();
    let processed_rows: usize = myval::db::postgres::push(&df, &params,
        &pool).await.unwrap();
}
```

#### Client

Let us push Polars data frame into a PostgreSQL database:

```rust,ignore
let df = myval::DataFrame::from(polars_df);
df.metadata_mut().insert(
    // set "database" metadata field
    "database".to_owned(),
    serde_json::to_string(&json!({
        // table, required
        "table": "test",
        // PostgreSQL schema, optional
        "postgres": { "schema": "public" },
        // keys, required if the table has got keys/unique indexes
        "keys": ["id"],
        // some field parameters
        "fields": {
            // another way to declare a key field
            //"id": { "key": true },
            // the following data frame columns contain strings which must be
            // sent to the database as JSON (for json/jsonb PostgreSQL types)
            "data1": { "json": true },
            "data2": { "json": true }
        }
    }))?,
);
// send the data frame to a server in a single or multiple chunks/blocks
```

#### PostgreSQL types supported

* BOOL, INT2 (16-bit int), INT4 (32-bit int), INT8 (64-bit int), FLOAT4 (32-bit
float), FLOAT8 (64-bit float)

* TIMESTAMP, TIMESTAMPTZ (time zone information is discarded as Arrow arrays
can not have different time zone for each record)

* CHAR, VARCHAR

* JSON/JSONB (encoded back to strings as LargeUtf8)

## General limitations

* Myval is not designed for data engineering. Use Polars.

* Myval series can contain a single chunk only and there are no plans to extend
this. When a Polars data frame with multiple chunks is converted to Myval, the
chunks are automatically aggregated.
