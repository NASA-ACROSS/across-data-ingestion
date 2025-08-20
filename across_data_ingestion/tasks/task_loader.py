from asyncio import create_task

from .example import check_server
from .schedules.chandra.high_fidelity_planned import (
    entrypoint as chandra_high_fidelity_planned_schedule_ingestion_task,
)
from .schedules.fermi.lat_planned import (
    entrypoint as fermi_planned_schedule_ingestion_task,
)
from .schedules.hst.low_fidelity_planned import (
    entrypoint as HST_low_fidelity_schedule_ingestion_task,
)
from .schedules.ixpe.low_fidelity_planned import (
    entrypoint as ixpe_low_fidelity_schedule_ingestion_task,
)
from .schedules.nicer.low_fidelity_planned import (
    entrypoint as nicer_low_fidelity_schedule_ingestion_task,
)
from .schedules.nustar.as_flown import (
    entrypoint as nustar_as_flown_schedule_ingestion_task,
)
from .schedules.nustar.low_fidelity_planned import (
    entrypoint as nustar_low_fidelity_schedule_ingestion_task,
)
from .schedules.swift.low_fidelity_planned import (
    entrypoint as swift_low_fidelity_schedule_ingestion_task,
)
from .schedules.tess.low_fidelity_planned import (
    entrypoint as TESS_low_fidelity_schedule_ingestion_task,
)
from .schedules.xmm_newton.low_fidelity_planned import (
    entrypoint as XMM_Newton_log_fidelity_schedule_ingestion_task,
)
from .tles.tle_ingestion import (
    entrypoint as tle_ingestion_task,
)


async def init_tasks():
    """
    Place tasks here to get initialized at startup.
    Each task definition contains its own configuration using a repeat_every decorator
    For more information see https://fastapiutils.github.io/fastapi-utils//user-guide/repeated-tasks/
    """
    create_task(check_server())
    create_task(TESS_low_fidelity_schedule_ingestion_task())
    create_task(fermi_planned_schedule_ingestion_task())
    create_task(nustar_low_fidelity_schedule_ingestion_task())
    create_task(nicer_low_fidelity_schedule_ingestion_task())
    create_task(ixpe_low_fidelity_schedule_ingestion_task())
    create_task(nustar_as_flown_schedule_ingestion_task())
    create_task(HST_low_fidelity_schedule_ingestion_task())
    create_task(tle_ingestion_task())
    create_task(chandra_high_fidelity_planned_schedule_ingestion_task())
    await XMM_Newton_log_fidelity_schedule_ingestion_task()
    create_task(swift_low_fidelity_schedule_ingestion_task())
