from across.sdk.v1 import (
    ObservationFootprintCreate,
    Point,
)
from across.tools import Coordinate, Polygon
from across.tools.footprint import Footprint


def _convert(footprint_points: list[list[Point]]) -> Footprint:
    """Convert SDK footprint representation into tools Footprint format.

    This function transforms a nested list of SDK `Point` objects into an
    `across.tools.Footprint` object composed of `Polygon` detectors, where each detector
    is defined by a list of `Coordinate` objects.

    Args:
        footprint_points (list[list[Point]]):
            A list of detectors, where each detector is represented as a list
            of `Point` objects. Each `Point` contains `x` (RA) and `y` (Dec)
            values.

    Returns:
        Footprint:
            A `Footprint` object containing a list of `Polygon` detectors with
            coordinates converted to `Coordinate` objects.

    """
    detectors = []
    for detector in footprint_points:
        detectors.append(
            Polygon(
                coordinates=[Coordinate(ra=point.x, dec=point.y) for point in detector]
            )
        )

    return Footprint(detectors=detectors)


def project_footprint(
    footprint_points: list[list[Point]], ra: float, dec: float, roll_angle: float
) -> list[ObservationFootprintCreate]:
    """Project a footprint onto a sky position and convert to SDK format.

    This function takes an input footprint defined in detector-relative
    coordinates, projects it onto a sky coordinate with a specified roll
    angle, and converts the result into a list of
    `ObservationFootprintCreate` objects for SDK usage.

    Args:
        footprint_points (list[list[Point]]):
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
    tools_footprint = _convert(footprint_points)

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
