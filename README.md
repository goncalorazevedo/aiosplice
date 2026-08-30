# aiosplice

An async wrapper around os.splice() for zero-copy data transfer between non-blocking file descriptors, built for asyncio. 
It is not a general-utility asyncio wrapper around os.splice().

## Table of contents
 
- [Requirements](#requirements)
- [Function signature](#function-signature)
- [Constraints](#constraints)
- [Example usage](#example-usage)


## Requirements
- Linux only — splice() is a Linux syscall.
- Python 3.10+ — os.splice() present.

## Function signature
```python
async def aiosplice(
    src: int,
    dst: int,
    count: int,
    offset_src: int | None = None,
    offset_dst: int | None = None,
    flags: int = 0,
    wait_on: str = "read",
) -> int:
```
All arguments are transparently passed to [os.splice()](https://docs.python.org/3/library/os.html#os.splice) (see also [splice(2)](https://man7.org/linux/man-pages/man2/splice.2.html)) aside from `flags` and `wait_on`. 
- `flags` adds `SPLICE_F_NONBLOCK` to the underlying splice syscall.
- `wait_on` takes "read" or "write" string literals to communicate which fd to wait on when the splice would block.


It works by calling os.splice() in a loop; on BlockingIOError it registers with the event loop's reader/writer callback 
for whichever fd wait_on points to, awaits readiness, and retries without blocking the event-loop.

There is no timeout and no detection of a wrong choice. If you pick the side that isn't actually going to block, 
the registered callback may never fire and the call hangs indefinitely.

### Constraints
The method does not enforce any validation, but some constraints, other than the `slice(2)` constraints, exist:
- One in-flight call per fd pair. Each src/dst pair is assumed to have at most one aiosplice() call in flight at a time (no locking against concurrent calls sharing a pair.)
- Non-blocking, selectable fds only. `src` and `dst` must already be `O_NONBLOCK` and be fds the event loop's selector can watch, real pipes or sockets.

## Example usage


Below is an example of a zero-copy TCP proxy: each direction of the connection splices through an intermediate pipe 
(src socket -> pipe -> dst socket).

```python
import asyncio
import os
import socket
from asyncio import Task

from aiosplice import aiosplice

running_tasks: set[Task] = set()


class ConnectionClosed(Exception):
    """Raised when either end of the proxied connection closes."""


async def forward_proxy(src: socket.socket, dst: socket.socket):
    r_pipe, w_pipe = os.pipe()
    for pipe in (r_pipe, w_pipe):
        os.set_blocking(pipe, False)

    try:
        while True:
            to_write = await aiosplice(src.fileno(), w_pipe, count = 64 * 1024, wait_on="read")
            if to_write == 0: # EOF
                raise ConnectionClosed(f"{src.fileno()} closed (read EOF)")
            while to_write > 0:
                written = await aiosplice(r_pipe, dst.fileno(), count = 64*1024, wait_on="write")
                if written == 0:
                    raise ConnectionClosed(f"{dst.fileno()} closed (write EOF)")
                to_write -= written
    finally:
        os.close(r_pipe)
        os.close(w_pipe)


async def handle_client(conn: socket.socket, addr: tuple[str, int]):
    loop = asyncio.get_event_loop()
    srv_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    await loop.sock_connect(srv_conn, ("localhost", 25000))

    conn.setblocking(False)
    srv_conn.setblocking(False)

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(forward_proxy(conn, srv_conn))
            tg.create_task(forward_proxy(srv_conn, conn))
    except* ConnectionClosed:
        pass
    except* OSError:
        pass
    finally:
        conn.close()
        srv_conn.close()


async def server():
    listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listen_socket.bind(("localhost", 30000))
    listen_socket.setblocking(False)
    listen_socket.listen()
    loop = asyncio.get_event_loop()
    while True:
        try:
            conn, addr = await loop.sock_accept(listen_socket)
        except OSError:
            continue
        task = loop.create_task(handle_client(conn, addr))
        running_tasks.add(task)
        task.add_done_callback(running_tasks.discard)

asyncio.run(server())
```

An interesting aspect of structured concurrency in this snippet is that either end closing the connection will result 
in the proper cleanup of the other running task, even if slightly delayed.
