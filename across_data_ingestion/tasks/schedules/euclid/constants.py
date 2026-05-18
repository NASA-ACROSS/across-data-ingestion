from ....util.across_server import sdk

EUCLID_VIS_BANDPASS = sdk.WavelengthBandpass(
    filter_name="Euclid VIS",
    min=550.0,
    max=990.0,
    unit=sdk.WavelengthUnit.NM,
)

EUCLID_NISP_RED_GRISM = sdk.WavelengthBandpass(
    filter_name="Euclid NISP Red Grism",
    min=1250.0,
    max=1850.0,
    unit=sdk.WavelengthUnit.NM,
)

EUCLID_NISP_BLUE_GRISM = sdk.WavelengthBandpass(
    filter_name="Euclid NISP Blue Grism",
    min=920.0,
    max=1250.0,
    unit=sdk.WavelengthUnit.NM,
)

EUCLID_NISP_BANDPASS_DICT = {
    "Y": sdk.WavelengthBandpass(
        filter_name="Euclid NISP Y",
        min=920,
        max=1146,
        unit=sdk.WavelengthUnit.NM,
    ),
    "J": sdk.WavelengthBandpass(
        filter_name="Euclid NISP J",
        min=1146,
        max=1372,
        unit=sdk.WavelengthUnit.NM,
    ),
    "H": sdk.WavelengthBandpass(
        filter_name="Euclid NISP H",
        min=1372,
        max=2000,
        unit=sdk.WavelengthUnit.NM,
    ),
}

BANDPASS_EXPOSURE_TIMES: dict[str, int] = {
    "Euclid VIS": 566,
    "Euclid NISP Red Grism": 574,
    "Euclid NISP Blue Grism": 574,
    "Euclid NISP Y": 112,
    "Euclid NISP J": 112,
    "Euclid NISP H": 112,
}
