import asyncio, hashlib, hmac, os, random, uuid
from fastapi import FastAPI, Request, Response
import httpx

app=FastAPI()
NAME=os.getenv('PROVIDER_NAME','alpha')
SECRET=os.getenv('WEBHOOK_SECRET','local-development-secret').encode()
CALLBACK=os.getenv('CALLBACK_URL','http://localhost:8000/webhooks/providers/'+NAME)

@app.post('/tasks')
async def create_task(request: Request):
    body=await request.json()
    mode=os.getenv('SYNC_MODE','')
    if mode=='503': return Response(status_code=503)
    if mode=='timeout': await asyncio.sleep(30)
    external=str(uuid.uuid4())
    async def deliver():
        delay=float(os.getenv('WEBHOOK_DELAY',str(random.randint(2,20))))
        await asyncio.sleep(delay)
        if os.getenv('NEVER_CALLBACK','false').lower()=='true': return
        rate=float(os.getenv('FAILURE_RATE','0'))
        forced=os.getenv('FAILURE_TYPE','technical')
        outcome=forced if random.random()<rate else 'completed'
        payload={'external_id':external,'status':'failed','error_type':outcome} if outcome!='completed' else {'external_id':external,'status':'completed','result_url':f'https://{NAME}.invalid/results/{external}'}
        raw=__import__('json').dumps(payload,separators=(',',':')).encode()
        headers={'X-Signature':hmac.new(SECRET,raw,hashlib.sha256).hexdigest()}
        async with httpx.AsyncClient() as client:
            await client.post(CALLBACK,content=raw,headers=headers)
            if os.getenv('DUPLICATE_CALLBACK','false').lower()=='true': await client.post(CALLBACK,content=raw,headers=headers)
    asyncio.create_task(deliver())
    return {'external_id':external}
