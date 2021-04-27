from multiprocessing.pool import AsyncResult
from typing import List, Union, Dict, Generator, Tuple

from rauth import OAuth1Session
from requests import HTTPError, Response

from ..concurrent_tasks import image_download, image_upload
from ..disk import ImageOnDisk
from ..utils import get_task_pool


class SmugMugConnection:
    API_SERVER = 'https://api.smugmug.com'
    API_PREFIX = 'api/v2'
    API_BASE_URL = f'{API_SERVER}/{API_PREFIX}'

    def __init__(self,
                 account: str,
                 consumer_key: str,
                 consumer_secret: str,
                 access_token: str,
                 access_token_secret: str,
                 use_test_folder: bool = False):
        self._account = account
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret
        self._access_token = access_token
        self._access_token_secret = access_token_secret
        self._headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }

        self._session = OAuth1Session(consumer_key=self._consumer_key,
                                      consumer_secret=self._consumer_secret,
                                      access_token=self._access_token,
                                      access_token_secret=self._access_token_secret)

        # Issue a request to get the user's JSON
        response = self.request_get(f'user/{account}')
        self._user = response['User']

        self._root_folder_uri = self._user['Uris']['Folder']['Uri']
        if use_test_folder:
            # Switch to use the Test folder
            self._root_folder_uri = f'{self._root_folder_uri}/Test/Test1'

    def request(self, method, url, *args, **kwargs) -> Response:
        try:
            if 'headers' not in kwargs:
                kwargs['headers'] = self._headers

            r = self._session.request(method, url, *args, **kwargs)
            r.raise_for_status()

            return r

        except HTTPError:
            print(f'Method: {method}, Url: {url}, args: {args}, kwargs: {kwargs}')
            raise

    def request_get(self, relative_uri: str, *args, **kwargs) -> Dict:
        r = self.request('GET', self._format_url(relative_uri), *args, **kwargs)
        return r.json()['Response']

    def request_post(self, relative_uri: str, json: Union[Dict, List], *args, **kwargs) -> Dict:
        r = self.request('POST', self._format_url(relative_uri), *args, json=json, **kwargs)
        return r.json()['Response']

    def request_delete(self, relative_uri: str):
        self.request('DELETE', self._format_url(relative_uri))

    @property
    def root_folder_uri(self) -> str:
        return self._root_folder_uri

    @property
    def account(self) -> str:
        return self._account

    @property
    def user(self) -> str:
        return self._user

    def folder_delete(self, folder: 'FolderOnSmugmug'):
        self.request_delete(folder.uri)

    def album_delete(self, album: 'AlbumOnSmugmug'):
        self.request_delete(album.uri)

    def image_delete(self, image: 'ImageOnSmugmug'):
        self.request_delete(image.uri)

    def folder_get(self, folder_uri: str = None, with_children: bool = True) -> Tuple[Dict, List[Dict], List[Dict]]:
        folder_uri = folder_uri or self._root_folder_uri

        r = self.request_get(folder_uri)
        folder = r['Folder']

        # Node get the children
        if with_children:
            sub_folders = self._get_items(folder['Uris']['Folders']['Uri'], object_name='Folder')
            albums = self._get_items(folder['Uris']['FolderAlbums']['Uri'], object_name='Album')

        else:
            sub_folders, albums = None, None

        return folder, sub_folders, albums

    def node_create(self,
                    parent_folder: 'FolderOnSmugmug',
                    node_on_disk: Union['FolderOnDisk', 'AlbumOnDisk']) -> Dict:
        """
        Create a new node (either an Album or a Folder) under the parent folder
        """

        object_type = 'Folder' if node_on_disk.is_folder else 'Album'

        r = self.request_post(f'node/{parent_folder.node_id}!children',
                              json={
                                  'Name': node_on_disk.name,
                                  'UriName': self._encode_uri_name(node_on_disk.name),
                                  'Type': object_type
                              })

        if node_on_disk.is_folder:
            uri = r['Node']['Uris']['FolderByID']['Uri']
        else:
            uri = r['Node']['Uris']['Album']['Uri']

        node = self.request_get(uri)
        return node[object_type]

    def album_images_get(self, album_record: Dict) -> List[Dict]:
        return self._get_items(relative_uri=album_record['Uris']['AlbumImages']['Uri'], object_name='AlbumImage')

    def image_download(self,
                       to_album: 'AlbumOnDisk',
                       image_on_smugmug: 'ImageOnSmugmug') -> AsyncResult:
        """
        Download a single image from the Album on Smugmug to a folder on disk
        """
        return get_task_pool().apply_async(image_download, (self, image_on_smugmug, to_album,))

    def image_upload(self,
                     to_album: 'AlbumOnSmugmug',
                     image_on_disk: ImageOnDisk,
                     image_to_replace: 'ImageOnSmugmug' = None) -> AsyncResult:
        """
        Upload an image to an album

        :param ImageOnDisk image_on_disk: Image to upload
        :param ImageOnSmugmug image_to_replace: Image to replace (optional)
        :param AlbumOnSmugmug to_album: Album to upload to
        """

        return get_task_pool().apply_async(image_upload, (self, to_album, image_on_disk, image_to_replace,))

    def _get_items(self, relative_uri: str, object_name: str) -> List[Dict]:
        """
        Materialized full list of items (through pagination)
        """

        def iter_items() -> Generator[Dict, None, None]:
            # Run the initial request
            response = self.request_get(relative_uri)

            items = response.get(object_name) or []

            # Now check if we need to get more pages, if so, iterate
            paging = response.get('Pages') or {}
            total_count = paging.get('Total') or len(items)
            items_found = 0

            while total_count > items_found:
                response = self.request_get(relative_uri, params={'start': items_found + 1, 'count': 100})
                items = response.get(object_name)
                if items:
                    for item in items:
                        yield item
                        items_found += 1

        return list(iter_items())

    @classmethod
    def _format_url(cls, uri: str) -> str:
        if uri.startswith('/'):
            uri = uri[1:]

        prefix = f'{cls.API_PREFIX}/'
        if uri.startswith(prefix):
            uri = uri[len(prefix):]

        return f'{cls.API_BASE_URL}/{uri}'

    @classmethod
    def _encode_uri_name(cls, name: str) -> str:
        return name.replace(' ', '-').replace(',', '')
