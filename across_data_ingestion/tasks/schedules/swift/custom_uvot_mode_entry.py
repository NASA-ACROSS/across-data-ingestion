from swifttools.swift_too.swift.uvot import UVOTModeEntry  # type: ignore


class CustomUVOTModeEntry:
    """
    Custom UVOTModeEntry to handle the UVOT mode as a string instead of a UVOTMode object.
    This is necessary to avoid multiple HTTP requests to the Swift TOO catalog.
    """

    filter_name: str
    weight: float

    def __init__(self, **kwargs):
        """
        Initializes a CustomUVOTModeEntry from keyword arguments.
        """
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __eq__(self, value):
        assert isinstance(value, CustomUVOTModeEntry), (
            "Can only compare with another CustomUVOTModeEntry"
        )
        return self.filter_name == value.filter_name and self.weight == value.weight

    @classmethod
    def from_entry(cls, entry: UVOTModeEntry) -> "CustomUVOTModeEntry":
        """
        Converts a UVOTModeEntry to a CustomUVOTModeEntry.
        """
        return cls(filter_name=str.lower(entry.filter_name), weight=entry.weight)
