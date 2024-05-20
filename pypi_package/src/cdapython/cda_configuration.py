from os import path
from ssl import get_default_verify_paths

from cda_client.configuration import Configuration

class CdaConfiguration(Configuration):
    
    def __init__(
        self,
        host = None,
        api_key=None,
        api_key_prefix=None,
        access_token=None,
        username=None,
        password=None,
        discard_unknown_keys=False,
        disabled_client_side_validations="",
        server_index=None,
        server_variables=None,
        server_operation_index=None,
        server_operation_variables=None,
        ssl_ca_cert=None,
        verify = None,
        verbose = None,
    ):
        if host is None:
            
            host = 'https://cancerdata.dsde-prod.broadinstitute.org/'

        self._host = host.strip("/")

        self.verbose = verbose

        self.verify = verify

        super().__init__(
            self._host,
            api_key,
            api_key_prefix,
            access_token,
            username,
            password,
            discard_unknown_keys,
            disabled_client_side_validations,
            server_index,
            server_variables,
            server_operation_index,
            server_operation_variables,
            ssl_ca_cert,
        )

    def get_host_settings(self):
        """Gets an array of host settings

        :return: An array of host settings
        """
        return [
            {
                'url': self._host,
                'description': 'URL of CDA REST API service',
            }
        ]


