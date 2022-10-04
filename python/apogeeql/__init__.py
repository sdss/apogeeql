import asyncio
from functools import partial
from sdsstools import get_logger

log = get_logger('apogeeql')

__version__ = '2.0.0a0'


async def wrapBlocking(func, *args, **kwargs):
    loop = asyncio.get_event_loop()

    wrapped = partial(func, *args, **kwargs)

    return await loop.run_in_executor(None, wrapped)