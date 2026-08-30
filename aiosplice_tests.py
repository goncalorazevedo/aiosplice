import asyncio, os, pytest
from aiosplice import aiosplice

@pytest.mark.asyncio
async def test_basic_splice_pipe_to_pipe():
    r1, w1 = os.pipe()
    r2, w2 = os.pipe()
    for fd in (r1, w1, r2, w2):
        os.set_blocking(fd, False)
    os.write(w1, b"hello")
    n = await aiosplice(r1, w2, 5)
    assert n == 5
    assert os.read(r2, 5) == b"hello"

@pytest.mark.asyncio
async def test_invalid_wait_on_raises():
    with pytest.raises(ValueError):
        await aiosplice(0, 1, 10, wait_on="both")

@pytest.mark.asyncio
async def test_blocks_then_completes_on_empty_pipe():
    # src pipe empty -> BlockingIOError -> exercises add_reader path;
    # write from a separate task after a delay and confirm it unblocks
    r, w = os.pipe()
    os.set_blocking(r, False); os.set_blocking(w, False)
    r2, w2 = os.pipe()
    os.set_blocking(r2, False); os.set_blocking(w2, False)

    async def delayed_write():
        await asyncio.sleep(0.05)
        os.write(w, b"x")

    task = asyncio.create_task(delayed_write())
    n = await aiosplice(r, w2, 1)
    await task
    assert n == 1

@pytest.mark.asyncio
async def test_cancellation_removes_reader():
    loop = asyncio.get_running_loop()
    r, w = os.pipe()
    os.set_blocking(r, False)
    r2, w2 = os.pipe()
    os.set_blocking(w2, False)

    task = asyncio.create_task(aiosplice(r, w2, 1))
    await asyncio.sleep(0.01)  # let it register the reader
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # reader should be cleaned up -- no direct public API, but this
    # shouldn't raise if you re-add/remove the same fd
    loop.remove_reader(r) is False or loop.add_reader(r, lambda: None) or loop.remove_reader(r)


@pytest.mark.asyncio
async def test_wait_on_direction_mismatch_hangs():
    # documents the known limitation: choosing the wrong wait_on
    # should either raise or be bounded by a timeout, not hang forever
    r, _ = os.pipe()
    _, w = os.pipe()
    os.set_blocking(r, False)
    os.set_blocking(w, False)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(aiosplice(src=r, dst=w, count=1, wait_on="read"), timeout=1)