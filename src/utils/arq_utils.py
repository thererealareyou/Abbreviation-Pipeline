from arq import ArqRedis
from fastapi import Request


async def get_arq_pool(request: Request) -> ArqRedis:
    return request.app.state.arq_pool
