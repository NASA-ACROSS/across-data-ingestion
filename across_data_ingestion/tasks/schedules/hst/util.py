import pydantic

from ....util.across_server import sdk

# List of target names found in observations to ignore
# Mostly calibration observations
TARGET_NAMES_TO_IGNORE = [
    "DARK-NM",
    "WAVEHITM",
    "DARK",
    "BIAS",
    "TUNGSTEN",
    "NONE",
    "WAVELINE",
    "ANY",
    "WAVE",
    "DARK-EARTH-CALIB",
]


class InstrumentInfo(pydantic.BaseModel):
    id: str
    bandpass: sdk.Bandpass
    type: sdk.ObservationType


def get_obs_type(
    filter: sdk.Filter,
    across_instrument: sdk.Instrument,
) -> sdk.ObservationType:
    # Get the observation type
    # Parse from filter name without HST or instrument name
    filter_descriptor = filter.name.split(" ")[-1]

    # Filter element key
    # "G" = grism
    # "E" = grating
    # "P" = prism
    # "FR" = ACS grating ramp filter elements
    spectroscopy_filter_element = filter_descriptor.startswith(("G", "E", "P", "FR"))

    # All COS observations are spectroscopic
    is_spectroscopy = (
        "COS" in across_instrument.short_name or spectroscopy_filter_element
    )

    if is_spectroscopy:
        obs_type = sdk.ObservationType.SPECTROSCOPY
    else:
        # All the rest are imaging elements
        obs_type = sdk.ObservationType.IMAGING

    return obs_type


def get_instrument_short_name_from_observation(
    obs_instrument: str,
) -> str | None:
    if "ACS" in obs_instrument:
        instrument_short_name = "HST_ACS"
    elif "COS" in obs_instrument:
        instrument_short_name = "HST_COS"
    elif "STIS" in obs_instrument:
        instrument_short_name = "HST_STIS"
    elif "WFC3" in obs_instrument and "UV" in obs_instrument:
        instrument_short_name = "HST_WFC3_UVIS"
    elif "WFC3" in obs_instrument and "IR" in obs_instrument:
        instrument_short_name = "HST_WFC3_IR"
    else:
        instrument_short_name = None

    return instrument_short_name
