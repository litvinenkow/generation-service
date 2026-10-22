import asyncio, hashlib, hmac, json, os, uuid
import asyncpg, httpx, pytest
from app import main

SECRET=os.getenv('WEBHOOK_SECRET','local-development-secret').encode()
def signed(payload):
    raw=json.dumps(payload,separators=(',',':')).encode()
    return raw, {'X-Signature':hmac.new(SECRET,raw,hashlib.sha256).hexdigest()}

async def payment(client,pid='p1',uid=1,tokens=50):
    raw,headers=signed({'payment_id':pid,'user_id':uid,'tokens':tokens,'status':'paid'})
    return await client.post('/webhooks/payments',content=raw,headers=headers)

@pytest.mark.asyncio
async def test_duplicate_payment_concurrently(client):
    results=await asyncio.gather(*(payment(client) for _ in range(20)))
    assert all(r.status_code==200 for r in results)
    async with main.pool.acquire() as c:
        assert await c.fetchval('SELECT balance FROM users WHERE id=1')==50
        assert await c.fetchval("SELECT count(*) FROM ledger WHERE kind='payment'")==1

@pytest.mark.asyncio
async def test_last_leg_failure_refunds_once(client,monkeypatch):
    async def no_send(*args): pass
    monkeypatch.setattr(main,'send_attempt',no_send)
    await payment(client,'fund',2)
    r=await client.post('/generate',json={'user_id':2,'model_slug':'video-long','prompt':'x','ref_images':[]})
    tid=r.json()['task_id']
    async with main.pool.acquire() as c:
        first=await c.fetchrow('SELECT id FROM attempts WHERE task_id=$1',uuid.UUID(tid))
    await main.fail_attempt(tid,first['id'],'technical')
    async with main.pool.acquire() as c:
        aid=await c.fetchval("SELECT id FROM attempts WHERE task_id=$1 AND provider='alpha'",uuid.UUID(tid))
        await c.execute("UPDATE attempts SET status='waiting',external_id='final-id' WHERE id=$1",aid)
    payload={'external_id':'final-id','status':'failed','error_type':'technical'}
    raw,headers=signed(payload)
    rs=await asyncio.gather(*(client.post('/webhooks/providers/alpha',content=raw,headers=headers) for _ in range(10)))
    assert all(x.status_code==200 for x in rs)
    async with main.pool.acquire() as c:
        assert await c.fetchval("SELECT count(*) FROM ledger WHERE kind='generation_refund'")==1
        assert await c.fetchval('SELECT balance FROM users WHERE id=2')==50

@pytest.mark.asyncio
async def test_first_leg_failure_completes_second(client,monkeypatch):
    async def controlled(task_id,attempt_id,provider,prompt,refs):
        if provider=='alpha': await main.fail_attempt(task_id,attempt_id,'technical'); return
        async with main.pool.acquire() as c:
            await c.execute("UPDATE attempts SET external_id='success-beta',status='waiting' WHERE id=$1",attempt_id)
    monkeypatch.setattr(main,'send_attempt',controlled)
    await payment(client,'fund3',3)
    res=await client.post('/generate',json={'user_id':3,'model_slug':'image-fast','prompt':'x','ref_images':[]})
    tid=res.json()['task_id']
    raw,headers=signed({'external_id':'success-beta','status':'completed','result_url':'https://example.test/image'})
    await client.post('/webhooks/providers/beta',content=raw,headers=headers)
    task=(await client.get('/tasks/'+tid)).json()
    assert task['status']=='completed' and len(task['attempts'])==2

@pytest.mark.asyncio
async def test_late_success_after_watchdog_timeout_is_ignored(client,monkeypatch):
    monkeypatch.setattr(main,'TIMEOUT',0)
    async def no_send(*args):
        async with main.pool.acquire() as c: await c.execute("UPDATE attempts SET external_id='late',status='waiting' WHERE id=$1",args[1])
    monkeypatch.setattr(main,'send_attempt',no_send)
    await payment(client,'fund4',4)
    res=await client.post('/generate',json={'user_id':4,'model_slug':'image-fast','prompt':'x','ref_images':[]})
    tid=res.json()['task_id']
    await main.timeout_once()
    async with main.pool.acquire() as c:
        row=await c.fetchrow("SELECT id FROM attempts WHERE task_id=$1 AND provider='beta'",uuid.UUID(tid))
        await c.execute("UPDATE attempts SET external_id='late-beta',status='waiting',started_at=now()-interval '1 minute' WHERE id=$1",row['id'])
    await main.timeout_once()
    raw,headers=signed({'external_id':'late-beta','status':'completed','result_url':'https://late.invalid/file'})
    await client.post('/webhooks/providers/beta',content=raw,headers=headers)
    task=(await client.get('/tasks/'+tid)).json()
    assert task['status']=='failed'
    async with main.pool.acquire() as c:
        assert await c.fetchval("SELECT count(*) FROM ledger WHERE kind='generation_refund'")==1
        assert await c.fetchval("SELECT count(*) FROM tasks WHERE id=$1 AND result_url IS NULL",uuid.UUID(tid))==1
