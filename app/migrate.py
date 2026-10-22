import asyncio, os
from pathlib import Path
import asyncpg

async def main():
    conn = await asyncpg.connect(os.environ.get('DATABASE_URL','postgresql://app:app@localhost:5432/generation'))
    try:
        await conn.execute('CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())')
        for path in sorted(Path('migrations').glob('*.sql')):
            async with conn.transaction():
                if await conn.fetchval('SELECT 1 FROM schema_migrations WHERE version=$1', path.name): continue
                await conn.execute(path.read_text())
                await conn.execute('INSERT INTO schema_migrations(version) VALUES($1)', path.name)
    finally: await conn.close()
if __name__ == '__main__': asyncio.run(main())
