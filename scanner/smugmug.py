import logging
import sys

from rauth import OAuth1Session

from config import api_key, api_secret, oauth_token, oauth_token_secret

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

API_BASE_URL = "https://api.smugmug.com/api/v2"


class SmugMugConnection(object):
    def __init__(self, nickname):
        self._nickname = nickname
        self._session = OAuth1Session(consumer_key=api_key,
                                      consumer_secret=api_secret,
                                      access_token=oauth_token,
                                      access_token_secret=oauth_token_secret)

        self._headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        # Issue a request to get the user's JSON
        r = self._session.get("{}/user/{}".format(API_BASE_URL, nickname), headers=self._headers)
        self._user = r.json()['Response']['User']
        self._root_folder_uri = self._user['Uris']['Node']['Uri']

    def node_get(self, node_uri=None, with_children=True):
        node_uri = node_uri or self._root_folder_uri

        # Get the node
        r = self._session.get("{}{}".format(API_BASE_URL, node_uri), headers=self._headers)
        node = r.json()['Response']['Node']

        # Node get the children
        children = []
        if with_children and node['Type'] != 'Album':
            r = self._session.get("{}{}!children".format(API_BASE_URL, node_uri), headers=self._headers)
            children = r.json()['Response']['Node']

        return node, children

    def album_get_all(self):
        r = self._session.get("{}/user/{}!albums".format(API_BASE_URL, self._nickname), headers=self._headers)
        return r.json()['Response']['Album']

    def images_get(self, **kwargs):
        # TODO
        pass

    def images_delete(self, **kwargs):
        # TODO
        pass

    def images_changeSettings(self, **kwargs):
        # TODO
        pass

    def images_update(self, **kwargs):
        # TODO
        pass

    def albums_changeSettings(self, **kwargs):
        # TODO
        pass

    def categories_create(self, **kwargs):
        # TODO
        pass

    def subcategories_create(self, **kwargs):
        # TODO
        pass
