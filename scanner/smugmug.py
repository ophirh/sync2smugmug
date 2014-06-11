import os
import urlparse
import logging
import sys
import config
from smugpy import SmugMug, SmugMugException

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)


class MySmugMug(SmugMug):
    def __init__(self, **kwargs):
        super(MySmugMug, self).__init__(kwargs)

        # Restore previously obtained access key
        self.oauth_token = config.oauth_token
        self.oauth_token_secret = config.oauth_token_secret
        self.api_key = config.api_key
        self.oauth_secret = config.api_secret

    @staticmethod
    def create_with_access_key_established():
        """ Factory method - creates a connection when the access key was already established """

        oauth_token = config.oauth_token
        oauth_token_secret = config.oauth_token_secret
        api_key = config.api_key
        oauth_secret = config.api_secret

        api = MySmugMug(api_key=api_key,
                        oauth_secret=oauth_secret,
                        oauth_token=oauth_token,
                        oauth_token_secret=oauth_token_secret,
                        api_version='1.3.0',
                        secure=True,
                        app_name='MigrateFromPicasa')

        return api

    def auth_getAccessToken(self, **kwargs):
        """Override the behavior to also save the keys to file once obtained"""
        rsp = super(MySmugMug, self).auth_getAccessToken(kwargs)

        print 'Update your config.py file with the following values:'
        print "oauth_token = '%s'" % self.oauth_token
        print "oauth_token_secret = '%s'" % self.oauth_token_secret
        print "api_key = '%s'" % self.api_key
        print "api_secret = '%s'" % self.api_secret

        return rsp
