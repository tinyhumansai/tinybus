# tinybus complete example

This is a deliberately separate Cargo package showing a small application
split across four processes:

```text
bus_server  <-- Unix socket -->  catalog_service
     ^                              ^
     |                              |
bus_monitor                    shop_consumer
```

The catalog demonstrates the service side: it exports an object, implements a
typed interface, claims a well-known name, maintains state, and emits a signal.
The shop consumer demonstrates the client side: it checks availability, uses a
typed proxy, applies a per-service deadline, handles structured errors, and
receives signals. The monitor shows that another consumer can independently
subscribe to the same traffic.

## Run it

Use four terminals from this directory:

```sh
cargo run --bin bus_server
cargo run --bin catalog_service
cargo run --bin bus_monitor
cargo run --bin shop_consumer
```

The consumer prints the catalog, buys one item, and waits for the service's
periodic `StockChanged` signal. The monitor prints that same signal. Stop the
catalog and run the consumer again to see availability detection; stop the
broker to see the transport boundary fail instead of hanging indefinitely.

Set `TINYBUS_ADDRESS` to use another socket. For example:

```sh
TINYBUS_ADDRESS=/run/user/$UID/tinybus/demo cargo run --bin bus_server
```

This package is excluded from the root workspace intentionally. A real
integration should live in its own repository and own dependency graph; this
folder is only a runnable teaching example of that boundary.
