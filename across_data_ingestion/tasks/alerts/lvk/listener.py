import asyncio
import json

import structlog
from gcn_kafka import Consumer

from .config import gcn_lvk_config
from .util import ingest_lvk_alert

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


async def entrypoint() -> None:
    try:
        consumer = Consumer(
            client_id=gcn_lvk_config.GCN_LVK_CLIENT_ID,
            client_secret=gcn_lvk_config.GCN_LVK_CLIENT_SECRET,
        )

        consumer.subscribe(["igwn.gwalert"])

        for message in consumer.consume():
            value = message.value()
            if value is None:
                continue
            alert = json.loads(value)
            if not isinstance(alert, dict):
                logger.warning("Skipping LVK alert with non-object payload")
                continue
            ingest_lvk_alert(alert)

    except Exception as e:
        logger.error(
            "LVK alert listener encountered an unknown error: ",
            err=e,
            exc_info=True,
        )
        return


### Local testing with sample file
def _read_json_file(path: str) -> dict:
    with open(path) as f:
        return json.loads(f.read())


async def test_entrypoint() -> None:
    logger.info("Running test entrypoint for LVK alert listener")
    alert = await asyncio.to_thread(_read_json_file, "MS181101ab-preliminary.json")
    logger.info("Ingesting LVK alert from test file")
    ingest_lvk_alert(alert)
