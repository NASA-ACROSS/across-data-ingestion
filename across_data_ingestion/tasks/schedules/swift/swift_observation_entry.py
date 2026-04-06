from astropy.time import Time  # type: ignore[import-untyped]
from swifttools.swift_too.swift.obsquery import SwiftAFSTEntry  # type: ignore
from swifttools.swift_too.swift.planquery import SwiftPPSTEntry  # type: ignore


class SwiftObservationEntry:
    """
    Custom Swift_PPST_Entry to handle the UVOT mode as a string instead of a UVOTMode object.
    This is necessary to avoid multiple HTTP requests to the Swift TOO catalog.
    """

    obsid: str
    targname: str
    ra: str
    dec: str
    begin: str
    end: str
    exposure: float
    roll: float
    uvot: str
    bat: str
    xrt: str
    fom: float
    segment: int
    target_id: str

    def __init__(self, **kwargs):
        """
        Initializes a CustomSwiftEntry from keyword arguments.
        """
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def from_entry(
        cls, entry: SwiftAFSTEntry | SwiftPPSTEntry
    ) -> "SwiftObservationEntry":
        """
        Converts a SwiftAFSTEntry or SwiftPPSTEntry to a SwiftObservationEntry.
        """
        return cls(
            obsid=entry.obsid,
            targname=entry.targname,
            ra=entry.ra,
            dec=entry.dec,
            begin=Time(entry.begin).isot,
            end=Time(entry.end).isot,
            exposure=entry.exposure.seconds,
            roll=entry.roll,
            uvot=entry.uvot,
            bat=entry.bat,
            xrt=entry.xrt,
            fom=entry.fom,
            segment=entry.segment,
            target_id=entry.target_id,
        )
