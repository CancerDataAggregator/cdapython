import os
import re
import pandas as pd

import cda_client

#############################################################################################################################
#
# get_api_url(): Return the current system URL for the CDA REST API.
#
#############################################################################################################################

def get_api_url():
    """
    Return the currently-set URL pointing to the CDA REST API.
    """

    # System default.

    default_api_url = 'https://cda.datacommons.cancer.gov'

    # Has the user set a non-default URL?

    local_api_url = os.environ.get( '__CDA_API_URL' )

    if local_api_url is not None and len( local_api_url ) > 0:
        
        return local_api_url

    else:
        
        return default_api_url

#############################################################################################################################
#
# set_api_url(): Set the current system URL for the CDA REST API.
#
#############################################################################################################################

def set_api_url( new_api_url ):
    """
    Set the current system URL for the CDA REST API.
    """

    # Do some basic sanity checking.

    if re.search( r'^https*:\/\/', new_api_url ) is None:
        raise RuntimeError( 'set_api_url(): Only HTTP and HTTPS URLs are allowed.' )
    elif len( new_api_url ) > 100:
        raise RuntimeError( 'set_api_url(): Whatever that was, it wasn\'t the URL of the CDA REST API.' )

    os.environ['__CDA_API_URL'] = new_api_url


