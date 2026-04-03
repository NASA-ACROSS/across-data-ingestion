from across.sdk.v1 import (
    ObservationCreate,
    ObservationFootprintCreate,
    ObservationType,
    Point,
    Telescope,
)
from across.tools import Coordinate, Polygon
from across.tools.footprint import Footprint


def sdk_footprints_to_tools(footprint: list[list[Point]]) -> Footprint:
    """Convert SDK footprint representation into tools Footprint format.

    This function transforms a nested list of SDK `Point` objects into an
    `across.tools.Footprint` object composed of `Polygon` detectors, where each detector
    is defined by a list of `Coordinate` objects.

    Args:
        footprint (list[list[Point]]):
            A list of detectors, where each detector is represented as a list
            of `Point` objects. Each `Point` contains `x` (RA) and `y` (Dec)
            values.

    Returns:
        Footprint:
            A `Footprint` object containing a list of `Polygon` detectors with
            coordinates converted to `Coordinate` objects.

    """
    detectors = []
    for detector in footprint:
        detectors.append(
            Polygon(
                coordinates=[Coordinate(ra=point.x, dec=point.y) for point in detector]
            )
        )

    return Footprint(detectors=detectors)


def generate_observation_footprint(
    footprint: list[list[Point]], ra: float, dec: float, roll_angle: float
) -> list[ObservationFootprintCreate]:
    """Project a footprint onto a sky position and convert to SDK format.

    This function takes an input footprint defined in detector-relative
    coordinates, projects it onto a sky coordinate with a specified roll
    angle, and converts the result into a list of
    `ObservationFootprintCreate` objects for SDK usage.

    Args:
        footprint (list[list[Point]]):
            A list of detectors, where each detector is a list of `Point`
            objects representing the footprint geometry in detector space.

        ra (float):
            Right ascension of the target sky position in degrees.

        dec (float):
            Declination of the target sky position in degrees.

        roll_angle (float):
            Roll angle (rotation) to apply during projection, in degrees.

    Returns:
        list[ObservationFootprintCreate]:
            The projected footprint to be associated with the created observation

    """
    tools_footprint = sdk_footprints_to_tools(footprint)

    projected_footprint = tools_footprint.project(
        coordinate=Coordinate(ra=ra, dec=dec), roll_angle=roll_angle
    )

    footprint_creates = []

    for projected_detector in projected_footprint.detectors:
        footprint_creates.append(
            ObservationFootprintCreate(
                polygon=[
                    Point(x=coord.ra, y=coord.dec)
                    for coord in projected_detector.coordinates
                ]
            )
        )

    return footprint_creates


def tmp_do_it(
    telescope: Telescope, observations: list[ObservationCreate]
) -> list[ObservationCreate]:

    if telescope.instruments:
        instrument_footprint = {}

        for instrument in telescope.instruments:
            if instrument.footprints:
                instrument_footprint[instrument.id] = instrument.footprints

        for i, observation in enumerate(observations):
            if (
                observation.instrument_id in instrument_footprint.keys()
                and observation.type == ObservationType.IMAGING
            ):
                footprint = instrument_footprint[observation.instrument_id]
                roll_angle = (
                    observation.pointing_angle if observation.pointing_angle else 0.0
                )
                if observation.pointing_position:
                    observations[i].footprint = generate_observation_footprint(
                        footprint,
                        observation.pointing_position.ra,
                        observation.pointing_position.dec,
                        roll_angle,
                    )

    return observations
