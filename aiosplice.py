from typing import Literal

import asyncio
import sys
import os

if sys.platform != "linux":
    raise ImportError(
        "aiosplice requires Linux (os.splice is a Linux-only syscall). "
        f"Detected platform: {sys.platform!r}."
    )

if not hasattr(os, "splice"):
    raise ImportError(
        "aiosplice requires Python 3.10+ (os.splice was added in 3.10). "
        f"Detected version: {sys.version!r}."
    )

__all__ = ["aiosplice"]


async def _wait_ready(loop: asyncio.AbstractEventLoop, fd: int, wait_on: str) -> None:
    fut: asyncio.Future[None] = loop.create_future()
    def _wake(f: asyncio.Future) -> None:
        if not f.done():
            f.set_result(None)
    if wait_on == "read":
        loop.add_reader(fd, _wake, fut)
    else:
        loop.add_writer(fd, _wake, fut)
    try:
        await fut
    finally:
        if wait_on == "read":
            loop.remove_reader(fd)
        else:
            loop.remove_writer(fd)


async def aiosplice(
        src: int,
        dst: int,
        count: int,
        flags: int = 0,
        wait_on: Literal["write", "read"] = "read",
) -> int:
    """Async wrapper around os.splice() for non-blocking fds.

    Written specifically for zero-copy proxy connections, not as a general-purpose utility. It assumes:
      - Each fd pair has at most one aiosplice() call in flight at a time (no concurrent calls sharing a src/dst fd --
        there's no locking against that).
      - The caller knows which side of the transfer will actually stall and passes the matching wait_on (see below).
        There is no timeout and no detection of a wrong choice -- it hangs indefinitely.

    Repeatedly calls os.splice() and, on BlockingIOError, awaits readiness via the event loop before retrying --
    so it never blocks the loop.

    Args:
        src: Source file descriptor to splice from.
        dst: Destination file descriptor to splice to.
        count: Number of bytes to attempt to splice.
        flags: Extra os.splice() flags; os.SPLICE_F_NONBLOCK is added automatically.
        wait_on: Which fd to wait on when splice() would block -- "read" waits
            on src becoming readable, "write" waits on dst becoming writable.

    Returns:
        The number of bytes actually spliced (may be less than count).

    Raises:
        ValueError: If wait_on is not "read" or "write".

    Notes:
        - Picking the wrong wait_on for your traffic pattern means the registered callback may never fire and this
          call hangs indefinitely with no error and no timeout. Choose based on which side of your fds you expect to
          actually stall -- typically the remote/slower end (e.g. a socket) rather than a local pipe, which usually
          has free buffer space and rarely blocks on write.
        - src and dst MUST already be O_NONBLOCK and be fds the event loop's selector can watch
          (i.e. real pipes or sockets, not regular files -- epoll can't select on those; use asyncio.to_thread for
          file-backed fds instead).
        - Per the splice(2) contract, at least one of src / dst must refer to a pipe. This function does not enforce that.
    """
    if wait_on not in ("read", "write"):
        raise ValueError("wait_on must be 'read' or 'write'")

    loop = asyncio.get_running_loop()
    flags |= os.SPLICE_F_NONBLOCK

    while True:
        try:
            return os.splice(src, dst, count, None, None, flags)
        except BlockingIOError:
            await _wait_ready(loop, src if wait_on == "read" else dst, wait_on)
        except InterruptedError:
            continue
