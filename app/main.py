import asyncio, hashlib, hmac, json, os, uuid
from contextlib import asynccontextmanager
from datetime import timedelta

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

DB = os.getenv('DATABASE_URL', 'postgresql://app:app@localhost:5432/generation')
SECRET = os.getenv('WEBHOOK_SECRET', 'local-development-secret').encode()
TIMEOUT = int(os.getenv('PROVIDER_TIMEOUT', '60'))
PROVIDERS = {'alpha': os.getenv('ALPHA_URL', 'http://localhost:8101'), 'beta': os.getenv('BETA_URL', 'http://localhost:8102')}
pool: asyncpg.Pool

class Generate(BaseModel):
    user_id: int
    model_slug: str
    prompt: str
    ref_images: list[str] = Field(default_factory=list)

class Payment(BaseModel):
    payment_id: str
    user_id: int
    tokens: int = Field(gt=0)
    status: str

def sign(body: bytes) -> str:
    return hmac.new(SECRET, body, hashlib.sha256).hexdigest()

def valid_signature(body: bytes, signature: str | None) -> bool:
    return bool(signature) and hmac.compare_digest(sign(body), signature)

async def send_attempt(task_id: str, attempt_id: int, provider: str, prompt: str, refs: list[str]):
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.post(f'{PROVIDERS[provider]}/tasks', json={'task_id': task_id, 'prompt': prompt, 'ref_images': refs})
            response.raise_for_status()
            external_id = response.json()['external_id']
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("UPDATE attempts SET external_id=$2,status='waiting' WHERE id=$1 AND status='sending'", attempt_id, external_id)
    except Exception:
        await fail_attempt(task_id, attempt_id, 'technical')

async def fail_attempt(task_id: str, attempt_id: int, error_type: str):
    next_call = None
    async with pool.acquire() as conn:
        async with conn.transaction():
            task = await conn.fetchrow('SELECT * FROM tasks WHERE id=$1 FOR UPDATE', uuid.UUID(task_id))
            attempt = await conn.fetchrow('SELECT * FROM attempts WHERE id=$1 FOR UPDATE', attempt_id)
            if not task or task['status'] != 'processing' or attempt['status'] not in ('sending','waiting'): return
            await conn.execute("UPDATE attempts SET status='failed',error_type=$2,finished_at=now() WHERE id=$1", attempt_id, error_type)
            await conn.execute('UPDATE tasks SET last_error=$2,updated_at=now() WHERE id=$1', uuid.UUID(task_id), error_type)
            if error_type == 'payload':
                await refund_locked(conn, task)
                return
            legs = await conn.fetch('SELECT * FROM model_legs WHERE model_slug=$1 AND priority>$2 ORDER BY priority', task['model_slug'], attempt['leg_priority'])
            leg = next((x for x in legs if not (error_type == 'moderation' and x['skip_on_moderation'])), None)
            if leg is None:
                await refund_locked(conn, task)
                return
            aid = await conn.fetchval("INSERT INTO attempts(task_id,leg_priority,provider,status) VALUES($1,$2,$3,'sending') RETURNING id", uuid.UUID(task_id), leg['priority'], leg['provider'])
            await conn.execute('UPDATE tasks SET current_leg=$2,updated_at=now() WHERE id=$1', uuid.UUID(task_id), leg['priority'])
            next_call = (aid, leg['provider'], task['prompt'], task['ref_images'])
    if next_call: await send_attempt(task_id, *next_call)

async def refund_locked(conn, task):
    await conn.execute("UPDATE tasks SET status='failed',updated_at=now() WHERE id=$1 AND status='processing'", task['id'])
    await conn.execute("INSERT INTO ledger(user_id,delta,kind,reference) VALUES($1,$2,'generation_refund',$3) ON CONFLICT(kind,reference) DO NOTHING", task['user_id'], await conn.fetchval('SELECT price FROM models WHERE slug=$1', task['model_slug']), str(task['id']))
    await conn.execute('UPDATE users SET balance=balance+$2 WHERE id=$1', task['user_id'], await conn.fetchval('SELECT price FROM models WHERE slug=$1', task['model_slug']))

async def timeout_once():
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT a.id,a.task_id FROM attempts a JOIN tasks t ON t.id=a.task_id WHERE a.status='waiting' AND t.status='processing' AND a.started_at < now()-($1::int * interval '1 second') LIMIT 100", TIMEOUT)
    for row in rows: await fail_attempt(str(row['task_id']), row['id'], 'technical')

async def timeout_watchdog():
    while True:
        try:
            await timeout_once()
        except Exception: pass
        await asyncio.sleep(1)

async def watchdog_leader():
    conn = await asyncpg.connect(DB)
    try:
        while not await conn.fetchval('SELECT pg_try_advisory_lock(77123901)'): await asyncio.sleep(2)
        await timeout_watchdog()
    finally:
        await conn.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = await asyncpg.create_pool(DB, min_size=2, max_size=10)
    watcher = asyncio.create_task(watchdog_leader())
    yield
    watcher.cancel(); await pool.close()

app = FastAPI(lifespan=lifespan)

@app.post('/generate', status_code=202)
async def generate(body: Generate):
    task_id = uuid.uuid4()
    async with pool.acquire() as conn:
        async with conn.transaction():
            model = await conn.fetchrow('SELECT * FROM models WHERE slug=$1', body.model_slug)
            if not model: raise HTTPException(400, 'unknown model')
            legs = await conn.fetch('SELECT * FROM model_legs WHERE model_slug=$1 ORDER BY priority', body.model_slug)
            leg = next((x for x in legs if len(body.ref_images) <= x['max_ref_images']), None)
            if leg is None: raise HTTPException(400, 'no compatible provider leg')
            await conn.execute('INSERT INTO users(id) VALUES($1) ON CONFLICT DO NOTHING', body.user_id)
            user = await conn.fetchrow('SELECT balance FROM users WHERE id=$1 FOR UPDATE', body.user_id)
            active = await conn.fetchval("SELECT count(*) FROM tasks WHERE user_id=$1 AND status='processing'", body.user_id)
            if active >= 3: raise HTTPException(429, 'too many active tasks')
            if user['balance'] < model['price']: raise HTTPException(402, 'insufficient tokens')
            await conn.execute('UPDATE users SET balance=balance-$2 WHERE id=$1', body.user_id, model['price'])
            await conn.execute("INSERT INTO ledger(user_id,delta,kind,reference) VALUES($1,$2,'generation_debit',$3)", body.user_id, -model['price'], str(task_id))
            await conn.execute("INSERT INTO tasks(id,user_id,model_slug,prompt,ref_images,status,current_leg) VALUES($1,$2,$3,$4,$5,'processing',$6)", task_id, body.user_id, body.model_slug, body.prompt, json.dumps(body.ref_images), leg['priority'])
            aid = await conn.fetchval("INSERT INTO attempts(task_id,leg_priority,provider,status) VALUES($1,$2,$3,'sending') RETURNING id", task_id, leg['priority'], leg['provider'])
    await send_attempt(str(task_id), aid, leg['provider'], body.prompt, body.ref_images)
    return {'task_id': str(task_id)}

@app.post('/webhooks/providers/{name}')
async def provider_webhook(name: str, request: Request):
    raw = await request.body()
    if not valid_signature(raw, request.headers.get('X-Signature')): raise HTTPException(401, 'invalid signature')
    if name not in PROVIDERS: raise HTTPException(404, 'unknown provider')
    data = json.loads(raw)
    async with pool.acquire() as conn:
        attempt = await conn.fetchrow('SELECT id,task_id FROM attempts WHERE provider=$1 AND external_id=$2', name, data['external_id'])
    if not attempt: return {'ok': True}
    if data['status'] == 'failed':
        await fail_attempt(str(attempt['task_id']), attempt['id'], data['error_type'])
    elif data['status'] == 'completed':
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("INSERT INTO processed_webhooks(provider,external_id,outcome) VALUES($1,$2,'completed') ON CONFLICT DO NOTHING", name, data['external_id'])
                task = await conn.fetchrow('SELECT * FROM tasks WHERE id=$1 FOR UPDATE', attempt['task_id'])
                current = await conn.fetchrow('SELECT * FROM attempts WHERE id=$1 FOR UPDATE', attempt['id'])
                if task['status']=='processing' and current['status']=='waiting':
                    await conn.execute("UPDATE attempts SET status='completed',finished_at=now() WHERE id=$1", attempt['id'])
                    await conn.execute("UPDATE tasks SET status='completed',result_url=$2,updated_at=now() WHERE id=$1", task['id'], data['result_url'])
    return {'ok': True}

@app.post('/webhooks/payments')
async def payment_webhook(request: Request):
    raw = await request.body()
    if not valid_signature(raw, request.headers.get('X-Signature')): raise HTTPException(401, 'invalid signature')
    data = Payment.model_validate_json(raw)
    if data.status != 'paid': return {'ok': True}
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute('INSERT INTO users(id) VALUES($1) ON CONFLICT DO NOTHING', data.user_id)
            inserted = await conn.fetchval('INSERT INTO payments(payment_id,user_id,tokens,status) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING RETURNING payment_id', data.payment_id,data.user_id,data.tokens,data.status)
            if inserted:
                await conn.execute("INSERT INTO ledger(user_id,delta,kind,reference) VALUES($1,$2,'payment',$3)", data.user_id,data.tokens,data.payment_id)
                await conn.execute('UPDATE users SET balance=balance+$2 WHERE id=$1', data.user_id,data.tokens)
    return {'ok': True}

@app.get('/tasks/{task_id}')
async def get_task(task_id: uuid.UUID):
    async with pool.acquire() as conn:
        task=await conn.fetchrow('SELECT * FROM tasks WHERE id=$1',task_id)
        if not task: raise HTTPException(404,'task not found')
        attempts=await conn.fetch('SELECT provider,error_type,status,started_at,finished_at FROM attempts WHERE task_id=$1 ORDER BY id',task_id)
    return {'task_id':str(task_id),'status':task['status'],'current_leg':task['current_leg'],'last_error':task['last_error'],'result_url':task['result_url'],'attempts':[dict(x) for x in attempts]}

@app.get('/users/{user_id}/balance')
async def balance(user_id:int):
    async with pool.acquire() as conn:
        row=await conn.fetchrow('SELECT balance FROM users WHERE id=$1',user_id)
        if not row: raise HTTPException(404,'user not found')
        entries=await conn.fetch('SELECT delta,kind,reference,created_at FROM ledger WHERE user_id=$1 ORDER BY id DESC LIMIT 20',user_id)
    return {'user_id':user_id,'balance':row['balance'],'ledger':[dict(x) for x in entries]}
