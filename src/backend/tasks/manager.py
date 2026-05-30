import json
import logging
from sqlalchemy import update
from src.backend.models import Document, Chunk, ExtractedItem, SystemState

logger = logging.getLogger('arq.user')


class PipelineManager:
    def __init__(self):
        self.launchers = {
            "extract": self._launch_extract,
            "define": self._launch_define,
            "resolve": self._launch_resolve,
            "transliterate": self._launch_transliterate,
        }

    async def start(self, redis, db, doc_id: int, config: list[dict], trace_id: str):
        pipe_key = f"pipeline:{doc_id}"
        await redis.set(f"{pipe_key}:config", json.dumps(config))
        await redis.set(f"{pipe_key}:stage_idx", 0)
        await self._launch_stage(redis, db, doc_id, 0, config, trace_id)

    async def complete_batch(self, ctx, db, doc_id: int, trace_id: str):
        redis = ctx['redis']
        pipe_key = f"pipeline:{doc_id}"

        remaining = await redis.decr(f"{pipe_key}:remaining_jobs")
        if remaining == 0:
            stage_idx = int(await redis.get(f"{pipe_key}:stage_idx"))
            config = json.loads(await redis.get(f"{pipe_key}:config"))
            await self._launch_stage(redis, db, doc_id, stage_idx + 1, config, trace_id)

    async def _launch_stage(self, redis, db, doc_id: int, stage_idx: int, config: list[dict], trace_id: str):
        pipe_key = f"pipeline:{doc_id}"

        if stage_idx >= len(config):
            await redis.delete(f"{pipe_key}:config", f"{pipe_key}:stage_idx", f"{pipe_key}:remaining_jobs")

            if doc_id == 0:
                await db.execute(
                    update(SystemState)
                    .where(SystemState.key.in_(["build_term", "build_abbr"]))
                    .values(value="ready")
                )
                await db.commit()
                logger.info("[PIPELINE] Глобальная сборка словарей и транслитерация завершены!")
            else:
                await db.execute(update(Document).where(Document.id == doc_id).values(status="completed"))
                await db.commit()
                logger.info(f"[PIPELINE] Документ doc_id={doc_id} полностью обработан!")
            return

        stage_info = config[stage_idx]
        launcher = self.launchers.get(stage_info["stage"])

        if not launcher:
            logger.error(f"[PIPELINE] Не найден лаунчер для этапа: {stage_info['stage']}")
            return

        jobs_count = await launcher(redis, db, doc_id, stage_info, trace_id)

        if jobs_count == 0:
            logger.info(f"[PIPELINE] Этап {stage_info['stage']} пуст, переходим дальше")
            await self._launch_stage(redis, db, doc_id, stage_idx + 1, config, trace_id)
        else:
            await redis.set(f"{pipe_key}:stage_idx", stage_idx)
            await redis.set(f"{pipe_key}:remaining_jobs", jobs_count)

    async def _launch_extract(self, redis, db, doc_id, stage_info, trace_id):
        from sqlalchemy import select
        batch_size = stage_info.get("batch_size", 10)

        stmt = select(Chunk.id).where(Chunk.doc_id == doc_id).order_by(Chunk.order)
        chunk_ids = (await db.scalars(stmt)).all()

        if not chunk_ids:
            return 0

        jobs_count = 0
        for i in range(0, len(chunk_ids), batch_size):
            batch = list(chunk_ids[i: i + batch_size])
            for task_name in stage_info["tasks"]:
                await redis.enqueue_job(task_name, doc_id, batch, trace_id=trace_id)
                jobs_count += 1
        return jobs_count

    async def _launch_define(self, redis, db, doc_id, stage_info, trace_id):
        from sqlalchemy import select
        batch_size = stage_info.get("batch_size", 10)
        jobs_count = 0

        for meta in stage_info["tasks"]:
            stmt = select(ExtractedItem.id).join(Chunk).where(
                Chunk.doc_id == doc_id,
                ExtractedItem.item_type == meta["type"],
                ExtractedItem.is_final == False
            )
            item_ids = (await db.scalars(stmt)).all()

            for i in range(0, len(item_ids), batch_size):
                batch = list(item_ids[i: i + batch_size])
                await redis.enqueue_job(meta["task"], doc_id, batch, trace_id=trace_id)
                jobs_count += 1
        return jobs_count

    async def _launch_resolve(self, redis, db, doc_id, stage_info, trace_id):
        jobs_count = 0
        for task_name in stage_info["tasks"]:
            await redis.enqueue_job(task_name, doc_id, trace_id=trace_id)
            jobs_count += 1
        return jobs_count

    async def _launch_transliterate(self, redis, db, doc_id, stage_info, trace_id):
        jobs_count = 0
        for task_name in stage_info["tasks"]:
            await redis.enqueue_job(task_name, doc_id, trace_id=trace_id)
            jobs_count += 1
        return jobs_count


pipeline_manager = PipelineManager()
