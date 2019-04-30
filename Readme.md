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
            # !! this holds back throughput REALLY !!
            await ho4peer.co_send_obj(repr(job_result))
        else:
            # it's best for throughput to send asynchronous notification back
            # to the service consumer
            await ho4peer.co_send_code(rf'''
job_done({job_result!r})
''')

else:

    serving_addr = {'host': '0.0.0.0', 'port': 1234}

    async def serve_jobs():
        hbis = hbi.HBIS(
            serving_addr,
            lambda po, ho: runpy.run_module(
                mod_name=__name__,  # reuse this module file
                run_name='__job_context__',  # distinguish by run_name
            ),  # create an isolated context for each consumer connection
        )
        await hbis.serve_until_closed()

    asyncio.run(serve_jobs())

```

## Client Paradigm:

```python

import asyncio, hbi

POOR_THROUGHPUT = False

def find_service(...):
    ...

def jobs_pending() -> bool:
    ...

async def fetch_next_job():
    # raises StopIteration
    ...

async def reschedule_job(job):
    ...

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
    ...

async def work_out():
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
                            # !! this holds back throughput REALLY !!

                        job = None
        except StopIteration:  # raised by fetch_next_job()
            break  # all jobs done
        except Exception:
            logger.error("Distributed job failure, retrying ...", exc_info=True)
            if job is not None:
                await reschedule_job(job)
                job = None
            await asyncio.sleep(RECONNECT_WAIT)

asyncio.run(work_out())

```
