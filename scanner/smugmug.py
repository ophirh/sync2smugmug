import os
import urlparse
import logging
import sys
from smugpy import SmugMug, SmugMugException

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)


class MySmugMug(SmugMug):
    def __init__(self, **kwargs):
        super(MySmugMug, self).__init__(kwargs)

        self.access_key_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../data',
                                                 'accesskey.txt')

        # Restore previously obtained access key
        if os.path.exists(self.access_key_file_path):
            with open(self.access_key_file_path) as f:
                qs = urlparse.parse_qs(f.readline())
                self.oauth_token = qs['oauth_token'][0]
                self.oauth_token_secret = qs['oauth_token_secret'][0]
                self.api_key = qs['api_key'][0]
                self.oauth_secret = qs['api_secret'][0]

    @staticmethod
    def create_with_access_key_established():
        """ Factory method - creates a connection when the access key was already established """
        access_key_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../data', 'accesskey.txt')

        # Restore previously obtained access key
        if not os.path.exists(access_key_file_path):
            raise SmugMugException('Failed to find access key file at %s' % access_key_file_path)

        with open(access_key_file_path) as f:
            qs = urlparse.parse_qs(f.readline())
            oauth_token = qs['oauth_token'][0]
            oauth_token_secret = qs['oauth_token_secret'][0]
            api_key = qs['api_key'][0]
            oauth_secret = qs['api_secret'][0]

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

        # Save to file
        with open(self.access_key_file_path, "w") as f:
            line = 'oauth_token=%s&oauth_token_secret=%s&api_key=%s&api_secret=%s' % \
                   (self.access_token, self.access_token_secret, self.api_key, self.oauth_secret)
            f.write(line)

        return rsp
