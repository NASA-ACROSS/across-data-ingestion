import os
import tempfile
from base64 import b64decode
from datetime import UTC, datetime

import astropy_healpix as ah  # type: ignore
import numpy as np
import structlog
from ligo.skymap import postprocess  # type: ignore
from ligo.skymap.healpix_tree import interpolate_nested  # type: ignore
from ligo.skymap.io import fits  # type: ignore

from ....util.across_server import client, sdk

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


def get_skymap_90_50_area(skymap):
    skymap.sort("PROBDENSITY", reverse=True)
    level, _ = ah.uniq_to_level_ipix(skymap["UNIQ"])
    pixel_area = ah.nside_to_pixel_area(ah.level_to_nside(level))
    prob = pixel_area * skymap["PROBDENSITY"]
    cumprob = np.cumsum(prob)

    i_90 = cumprob.searchsorted(0.9)
    i_50 = cumprob.searchsorted(0.5)
    area_90 = pixel_area[:i_90].sum()
    area_50 = pixel_area[:i_50].sum()

    return area_90, area_50


def process_contours(skymap_str) -> list[sdk.LocalizationCreate]:
    """Process skymap contours.

    What it does step by step:

    Decode & load the map — The base64 string is decoded into raw bytes, written to a
    temporary FITS file (a standard astronomy image/data format), and read into memory
    as a HEALPix array. Think of HEALPix as a way of dividing the surface of a sphere
    (the sky) into equal-area pixels — like a global grid, but for a sphere instead of
    a flat map. Each pixel stores a probability value.

    Upsample the resolution (interpolate_nested) — The raw map may be coarse in places.
    This step interpolates/refines the pixel grid to a higher resolution so that later
    contour-drawing is smoother and more accurate.

    Rank pixels by probability — All pixels are sorted from most-likely to least-likely
    location of the event.

    Compute cumulative probability ("credible levels") — Starting from the most probable
    pixel and working down, it keeps a running total. This turns "this pixel has
    probability X" into "this pixel is part of the smallest region that contains Y% of
    total probability." That running total is the credible level for each pixel.

    Draw contours at 50% and 90% credible levels — This is the "confidence region" concept,
    analogous to error bars: the 50% contour is the smallest patch of sky with a 50% chance
    of containing the true source location, and 90% is the same idea for a wider, more
    inclusive region. postprocess.contour traces the boundary polygons of these regions on
    the sphere and converts them to RA/Dec (right ascension/declination — the sky's version
    of longitude/latitude) coordinates in degrees.

    Args:
        skymap_str (str): Base64-encoded skymap string.

    Returns:
        list: List of localizations with contours and probabilities enclosed.
    """
    skymap_bytes = b64decode(skymap_str)

    # small hack to read the skymap into ligo fits format
    # they don't have a read from bytes - only filestream
    with tempfile.NamedTemporaryFile(suffix=".fits.gz", delete=False) as tmp:
        tmp.write(skymap_bytes)  # type: ignore
        tmp_path = tmp.name
    try:
        pixel_prob, _ = fits.read_sky_map(tmp_path, nest=True)
    except Exception as e:
        raise RuntimeError("Failed to read skymap") from e
    finally:
        os.unlink(tmp_path)

    # calculate the 90/50 contours
    pixel_prob = interpolate_nested(pixel_prob, nest=True)

    sorted_indices = np.flipud(
        np.argsort(pixel_prob).astype(np.int32)  # type: ignore
    )
    cumulative_prob = np.cumsum(pixel_prob[sorted_indices])  # type: ignore

    credible_level = np.empty_like(pixel_prob)
    credible_level[sorted_indices] = cumulative_prob * 100

    logger.info("Calculating LVK alert contours")
    contours = list(
        postprocess.contour(  # type: ignore
            credible_level, [50, 90], nest=True, degrees=True, simplify=True
        )
    )

    # put the contours into our broker alert schema
    localizations = []
    for level, polys in zip([50, 90], contours):
        contours_payload: list[sdk.LocalizationContourBase] = []

        for i, poly in enumerate(polys):
            contours_payload.append(
                sdk.LocalizationContourBase(
                    contour=[sdk.Point(x=ra, y=dec) for ra, dec in poly]
                )
            )

        localizations.append(
            sdk.LocalizationCreate(
                contours=contours_payload,
                probability_enclosed=level / 100.0,
            )
        )

    return localizations


def ingest_lvk_alert(alert: dict):
    """
    Parse an LVK alert and extract relevant information.

    Args:
        alert (dict): The LVK alert payload.

    Returns:
        dict: Parsed alert information including localizations.
    """
    alert_type = str.lower(alert.get("alert_type"))  # type: ignore
    try:
        status = sdk.BrokerAlertStatus(alert_type)
    except ValueError as e:
        raise ValueError(f"Unknown LVK alert_type: {alert_type!r}") from e

    skymap_str = alert.get("event", {}).pop("skymap")

    logger.info("Processing LVK alert skymap")
    localizations = process_contours(skymap_str)

    broker_alert_create = sdk.BrokerAlertCreate(
        status=status,
        broker_name="GCN Kafka LVK",
        data_source=sdk.BrokerAlertDataSource.IGWN,
        broker_event_type=sdk.BrokerEventType.GW,
        broker_event_name=alert["superevent_id"],
        broker_received_on=datetime.now(tz=UTC),
        broker_event_datetime=alert["event"]["time"],
        payload=alert,
        localizations=localizations,
    )

    sdk.BrokerAlertApi(client).create_broker_alert(broker_alert_create)
    logger.info("Created new Broker Alert", broker_event_name=alert["superevent_id"])

    return broker_alert_create
