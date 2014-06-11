
# SDK configuration
oauth_token = '<<< >>>'
oauth_token_secret = '<<< >>>'
api_key = '<<< >>>'
api_secret = '<<< >>>'

# Location of the Picasa DB
picasa_db_location = '<<< >>>'


# Import my config (this is where the real data will be stored)...
try:
    from my_config import *
except ImportError:
    pass
