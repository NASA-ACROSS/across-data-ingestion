from ....util.across_server import sdk

XRISM_BANDPASSES = {
    "Resolve": sdk.EnergyBandpass(
        filter_name="XRISM Resolve",
        min=1.7,
        max=12.0,
        unit=sdk.EnergyUnit.KEV,
    ),
    "Xtend": sdk.EnergyBandpass(
        filter_name="XRISM Xtend",
        min=0.4,
        max=13.0,
        unit=sdk.EnergyUnit.KEV,
    ),
}


XRISM_OBSERVATION_TYPES = {
    "Resolve": sdk.ObservationType.SPECTROSCOPY,
    "Xtend": sdk.ObservationType.IMAGING,
}
