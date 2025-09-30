import across.sdk.v1 as sdk
from across.sdk.v1.api_client_wrapper import ApiClientWrapper

from ...core import config
from .ssm_credentials import SSMCredentials

client = ApiClientWrapper.get_client(
    configuration=sdk.Configuration(host=config.ACROSS_SERVER_URL),
    creds=SSMCredentials() if not config.is_local() else None,
)
