import os, pytest_asyncio, httpx, asyncpg
from app.main import app

@pytest_asyncio.fixture
async def client():
    from app import main
    main.pool=await asyncpg.create_pool(os.getenv('DATABASE_URL','postgresql://app:app@localhost:5432/generation'))
    async with main.pool.acquire() as c:
        await c.execute('TRUNCATE processed_webhooks, attempts, tasks, payments, ledger, users RESTART IDENTITY CASCADE')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as c:
        yield c
    await main.pool.close()
