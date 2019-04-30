# Hosting Based Interface

## Server Paradigm:

```python

import asyncio, hbi

POOR_THROUGHPUT = False

if '__job_context__' == __name__:

    po2peer: hbi.PostingEnd = None
    ho4peer: hbi.HostingEnd = None

    def __hbi_init__(po, ho):
        global po2peer: hbi.PostingEnd
        global ho4peer: hbi.HostingEnd

        po2peer, ho4peer = po, ho

    async def do_job(action, data_len):
        global po2peer: hbi.PostingEnd
        global ho4peer: hbi.HostingEnd

        # can be plain binary blob
        job_data = bytearray(data_len)
        # or numpy array with shape&dtype infered from data_len
        #shape, dtype = data_len
        #job_data = np.empty(shape, dtype)

        await ho4peer.co_recv_data(job_data)

        # use action/job_data
        job_result = ...

        # !! try best to avoid such synchronous service calls !!
        if POOR_THROUGHPUT:
            await ho4peer.co_send_obj(repr(job_result))
            # !! this holds down throughput REALLY !!
        else:
            # it's best for throughput to send asynchronous notification back
            # to the service consumer
            await ho4peer.co_send_code(rf'''
job_done({job_result!r})
''')

elif '__main__' == __name__:

    serving_addr = {'host': '127.0.0.1', 'port': 3232}

    async def serve_jobs():
        await hbi.HBIS(
            # listening IP address
            serving_addr,
            # the service context factory function,
            # create an isolated context for each consumer connection
            lambda po, ho: runpy.run_module(
                # use this module file for both service context and `python -m` entry point
                mod_name = __package__, # this module file needs to be `some/py_pkg/__main__.py`
                # telling the module init purpose via run_name, i.e. the global __name__ value
                run_name = '__job_context__',
            ),
        ).serve_until_closed()

    asyncio.run(serve_jobs())

```

## Client Paradigm:

```python

import asyncio, hbi

from some.job.source import gather_jobs

POOR_THROUGHPUT = False

def find_service(locator):
    ...
    return {'host': '127.0.0.1', 'port': 3232}

jobs_queue = None

def jobs_pending() -> bool:
    return jobs_queue is not None and not jobs_queue.empty()

async def fetch_next_job():
    return await jobs_queue.get()

async def reschedule_job(job):
    await job_queue.put(job)

po2peer: hbi.PostingEnd = None
ho4peer: hbi.HostingEnd = None

def __hbi_init__(po, ho):
    global po2peer: hbi.PostingEnd
    global ho4peer: hbi.HostingEnd

    po2peer, ho4peer = po, ho

# it's best for throughput to use such an asynchronous callback
# to react to results of service calls.
async def job_done(job_result):
    assert po2peer is not None and ho4peer is not None
    print('Job done:', job_result)

async def work_out(reconnect_wait=10):
    global jobs_queue
    # create the jobs queue within a coroutine, for its constructor to use
    # the correct loop associated with the os thread running it
    jobs_queue = asyncio.Queue()
    # spawn a new concurrent green thread to gather jobs
    asyncio.create_task(gather_jobs(jobs_queue))

    service_addr = find_service(...)
    react_context = globals()
    hbic = hbi.HBIC(service_addr, react_context)

    while jobs_pending():
        job = None
        try:
            async with hbic as po:
                while job is not None or jobs_pending():
                    if job is None:
                        job = await fetch_next_job()
                    async with po.co() as co:
                        await co.send_code(rf'''
do_job({job.action!r}, {job.data_len!r})
''')
                        await co.send_data(job.data)

                        # !! try best to avoid such synchronous service calls !!
                        if POOR_THROUGHPUT:
                            job_result = await co.recv_obj()
                            # !! this holds down throughput REALLY !!

                        job = None
        except Exception:
            logger.error("Distributed job failure, retrying ...", exc_info=True)
            if job is not None:
                await reschedule_job(job)
                job = None
            await asyncio.sleep(reconnect_wait)

asyncio.run(work_out())

```
