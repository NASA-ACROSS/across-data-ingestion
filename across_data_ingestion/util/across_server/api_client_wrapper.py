import across.sdk.v1 as sdk
from across.sdk.v1.api_client_wrapper import ApiClientWrapper

from ...core import config
from .ssm_credentials import SSMCredentials

sdk_config = sdk.Configuration(host=config.ACROSS_SERVER_URL)

# use local dev service account
if config.is_local():
    sdk_config.username = config.ACROSS_SERVER_ID
    sdk_config.password = config.ACROSS_SERVER_SECRET

client = ApiClientWrapper.get_client(
    configuration=sdk_config,
    creds=SSMCredentials() if not config.is_local() else None,
)
