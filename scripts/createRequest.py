import aiohttp
import asyncio

semaphore = asyncio.Semaphore(10)  # max 10 requests at a time

async def fetch(url, session, i):
    async with semaphore:  # only 10 allowed at once
        print(f"sent request {i}")
        async with session.get(url) as response:
            data = await response.text()
            print(f"Request {i} -> status {response.status}, length {len(data)}")
            return data

async def main():
    url = "http://localhost:9110/api/v1/monitor/45"
    i = 0
    async with aiohttp.ClientSession() as session:
        tasks = set()  # track running tasks
        while True:
            i += 1
            # start new request
            task = asyncio.create_task(fetch(url, session, i))
            tasks.add(task)

            # when task finishes, remove from set
            task.add_done_callback(tasks.discard)

            # if we already have 10 running, wait for one to finish
            if len(tasks) >= 10:
                _done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

            # (optional) sleep between requests
            await asyncio.sleep(0.1)

if __name__ == "__main__":
    asyncio.run(main())
        
