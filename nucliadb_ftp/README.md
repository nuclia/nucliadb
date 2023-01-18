# nucliadb_ftp

The `nucliadb_ftp` crate aims to transfer files/directories asynchronously over the network (using a TCP/IP connection),
based on a patched version of [`tokio-tar`](https://github.com/alekece/tokio-tar) crate.

To do so, `nucliadb_ftp` provides two simple and easy-to-use types:
- [`Publisher`] that helps appending files/directories before publishing them.
- [`Listener`] that helps listening (once or multiple time) incoming files/directories.

## Examples

```rust
# tokio_test::block_on(async {
use nucliadb_ftp::{Listener, Publisher};

let listener_task = tokio::spawn(async {
    Listener::default()
        .save_at("my_dir")
        // Uncomment this line if you want to preserve metadata of receveived files/directories.
        // .preserve_metadata()
        .listen_once(4242)
        // Uncomment this line if you want to keep the listener active.
        // .listen(4242)
        .await
        .unwrap();
});

let publisher_task = tokio::spawn(async {
    Publisher::default()
        // Uncomment this line if you want to publish files/directories with their metadata
        //.preserve_metadata()
        // Uncomment this line if you want to follow symlinks in appended directories.
        // .follow_symlink()
        .append("my_dir")
        .append("path/to/my_file")
        .send_to_localhost(4242)
        // Or
        // .sent_to("x.x.x.x:4242")
        .await
        .unwrap();
});

publisher_task.await.unwrap();
listener_task.await.unwrap();

# });
```
