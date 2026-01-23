from ....util.across_server import sdk

SWIFT_XRT_BANDPASS = sdk.EnergyBandpass(
    filter_name="Swift XRT",
    min=0.3,
    max=10.0,
    unit=sdk.EnergyUnit.KEV,
)

SWIFT_BAT_BANDPASS = sdk.EnergyBandpass(
    filter_name="Swift BAT",
    min=15.0,
    max=150.0,
    unit=sdk.EnergyUnit.KEV,
)

SWIFT_UVOT_BANDPASS_DICT = {
    "u": sdk.WavelengthBandpass(
        filter_name="Swift UVOT u",
        min=308,
        max=385,
        unit=sdk.WavelengthUnit.NM,
    ),
    "b": sdk.WavelengthBandpass(
        filter_name="Swift UVOT b",
        min=391,
        max=487,
        unit=sdk.WavelengthUnit.NM,
    ),
    "v": sdk.WavelengthBandpass(
        filter_name="Swift UVOT v",
        min=509,
        max=585,
        unit=sdk.WavelengthUnit.NM,
    ),
    "uvw1": sdk.WavelengthBandpass(
        filter_name="Swift UVOT uvw1",
        min=226,
        max=294,
        unit=sdk.WavelengthUnit.NM,
    ),
    "uvw2": sdk.WavelengthBandpass(
        filter_name="Swift UVOT uvw2",
        min=160,
        max=225,
        unit=sdk.WavelengthUnit.NM,
    ),
    "uvm2": sdk.WavelengthBandpass(
        filter_name="Swift UVOT uvm2",
        min=200,
        max=249,
        unit=sdk.WavelengthUnit.NM,
    ),
    "white": sdk.WavelengthBandpass(
        filter_name="Swift UVOT white",
        min=160,
        max=800,
        unit=sdk.WavelengthUnit.NM,
    ),
}
