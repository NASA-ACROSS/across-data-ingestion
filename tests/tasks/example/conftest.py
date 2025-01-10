from unittest.mock import patch


def mock_repeat_every(func):
    return func


# MUST MOCK DECORATOR BEFORE THE UNIT UNDER TEST GETS IMPORTED!
patch(
    "fastapi_utils.tasks.repeat_every", lambda *args, **kwargs: mock_repeat_every
).start()
