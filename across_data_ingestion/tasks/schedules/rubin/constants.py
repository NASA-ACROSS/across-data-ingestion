from ....util.across_server import sdk

RUBIN_BANDPASS_u = sdk.WavelengthBandpass.model_validate(
    {
        "min": 3492.5,
        "max": 3955.5,
        "unit": sdk.WavelengthUnit.ANGSTROM,
        "filter_name": "rubin_u",
    }
)
RUBIN_BANDPASS_g = sdk.WavelengthBandpass.model_validate(
    {
        "min": 4064.5,
        "max": 5549.5,
        "unit": sdk.WavelengthUnit.ANGSTROM,
        "filter_name": "rubin_g",
    }
)
RUBIN_BANDPASS_r = sdk.WavelengthBandpass.model_validate(
    {
        "min": 5521.5,
        "max": 6920.5,
        "unit": sdk.WavelengthUnit.ANGSTROM,
        "filter_name": "rubin_r",
    }
)
RUBIN_BANDPASS_i = sdk.WavelengthBandpass.model_validate(
    {
        "min": 6916,
        "max": 8202,
        "unit": sdk.WavelengthUnit.ANGSTROM,
        "filter_name": "rubin_i",
    }
)
RUBIN_BANDPASS_z = sdk.WavelengthBandpass.model_validate(
    {
        "min": 8160,
        "max": 9200,
        "unit": sdk.WavelengthUnit.ANGSTROM,
        "filter_name": "rubin_z",
    }
)
RUBIN_BANDPASS_y = sdk.WavelengthBandpass.model_validate(
    {
        "min": 9322,
        "max": 10184,
        "unit": sdk.WavelengthUnit.ANGSTROM,
        "filter_name": "rubin_y",
    }
)

RUBIN_BANDPASSES: dict[str, sdk.Bandpass] = {
    "rubin_u": sdk.Bandpass(RUBIN_BANDPASS_u),
    "rubin_g": sdk.Bandpass(RUBIN_BANDPASS_g),
    "rubin_r": sdk.Bandpass(RUBIN_BANDPASS_r),
    "rubin_i": sdk.Bandpass(RUBIN_BANDPASS_i),
    "rubin_z": sdk.Bandpass(RUBIN_BANDPASS_z),
    "rubin_y": sdk.Bandpass(RUBIN_BANDPASS_y),
}
