import asyncio
import logging
import hashlib
import os
from typing import List, Union, Dict, Generator, Tuple, AsyncIterator

from aioretry import retry, RetryInfo, RetryPolicyStrategy
from authlib.integrations.httpx_client import AsyncOAuth1Client
from httpx import HTTPError, Response

from sync2smugmug.disk.image import ImageOnDisk

logger = logging.getLogger(__name__)


# This example shows the usage with python typings
def retry_policy(info: RetryInfo) -> RetryPolicyStrategy:
    """
    Retry policy for connection issues
    """
    if isinstance(info.exception, ConnectionError):
        if info.fails < 3:
            logger.warning(f"Connection failed ({info.exception})! retrying...")
            return False, info.fails * 0.1

    # Raise any other exception
    return True, 0


class BaseSmugMugConnection:
    """
    Connection class implementing the basic auth and transport protocols with Smugmug
    """
    API_SERVER = 'https://api.smugmug.com'
    API_PREFIX = 'api/v2'
    API_BASE_URL = f'{API_SERVER}/{API_PREFIX}'
    CONCURRENT_CONNECTIONS = 25

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
        self._use_test_folder = use_test_folder
        self._user = None
        self._headers = {
            'Host': 'www.smugmug.com',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        self._sem = asyncio.Semaphore(self.CONCURRENT_CONNECTIONS)

    @property
    def root_folder_uri(self) -> str:
        return self._root_folder_uri

    @property
    def account(self) -> str:
        return self._account

    @property
    def user(self) -> str:
        return self._user

    async def __aenter__(self):
        self._asession = AsyncOAuth1Client(self._consumer_key,
                                           self._consumer_secret,
                                           token=self._access_token,
                                           token_secret=self._access_token_secret)

        # Issue a request to get the user's JSON
        response = await self.request_get(f'user/{self._account}')
        self._user = response['User']

        self._root_folder_uri = self._user['Uris']['Folder']['Uri']
        if self._use_test_folder:
            # Switch to use the Test folder
            self._root_folder_uri = f'{self._root_folder_uri}/Test/Test2'

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._asession is not None:
            await self._asession.aclose()
            self._asession = None

    async def request(self, method, url, *args, **kwargs) -> Response:
        async with self._sem:
            try:
                if 'headers' not in kwargs:
                    kwargs['headers'] = self._headers

                r = await self._asession.request(method, url, *args, **kwargs)
                r.raise_for_status()

                return r

            except HTTPError as e:
                logger.exception(f'Failed request: {method}, Url: {url}, args: {args}, kwargs: {kwargs}')
                raise e

    async def request_get(self, relative_uri: str, *args, **kwargs) -> Dict:
        r = await self.request('GET', self._format_url(relative_uri), *args, **kwargs)
        return r.json()['Response']

    async def request_post(self, relative_uri: str, json: Union[Dict, List], *args, **kwargs) -> Dict:
        async with self._sem:
            # For some reason, the async post does not work with Smugmug, so we are sending here a synchronous call.
            # r = await self.request('POST', self._format_url(relative_uri), *args, json=json, **kwargs)

            from authlib.integrations.requests_client import OAuth1Session
            session = OAuth1Session(self._consumer_key,
                                    self._consumer_secret,
                                    token=self._access_token,
                                    token_secret=self._access_token_secret)

            if 'headers' not in kwargs:
                kwargs['headers'] = self._headers

            try:
                r = session.post(self._format_url(relative_uri), *args, json=json, **kwargs)
                r.raise_for_status()

            except Exception as e:
                logger.exception(f'Failed to post {relative_uri} to {str(json)}')

                raise e

            return r.json()['Response']

    async def request_delete(self, relative_uri: str):
        async with self._sem:
            await self.request('DELETE', self._format_url(relative_uri))

    async def stream(self, absolute_uri: str) -> AsyncIterator[bytes]:
        async with self._sem:  # Limit concurrency to avoid timeouts
            async with self._asession.stream('GET', absolute_uri) as r:
                r.raise_for_status()

                async for chunk in r.aiter_raw(chunk_size=1024 * 1024):
                    yield chunk

    async def upload(self,
                     album_uri: str,
                     image_data: bytes,
                     image_name: str,
                     keywords: str,
                     image_to_replace_uri: str = None):
        headers = {
            'X-Smug-AlbumUri': album_uri,
            'X-Smug-Title': image_name,
            'X-Smug-Caption': image_name,
            'X-Smug-Keywords': keywords,
            'X-Smug-ResponseType': 'JSON',
            'X-Smug-Version': 'v2',
            'Content-MD5': hashlib.md5(image_data).hexdigest(),
        }

        if image_to_replace_uri:
            headers['X-Smug-ImageUri'] = image_to_replace_uri

        async with self._sem:  # Limit concurrency to avoid timeouts
            # Again - not sure why POST does not work here!!!
            from authlib.integrations.requests_client import OAuth1Session
            session = OAuth1Session(self._consumer_key,
                                    self._consumer_secret,
                                    token=self._access_token,
                                    token_secret=self._access_token_secret)

            r = session.post('https://upload.smugmug.com/',
                             files={'file': (image_name, image_data)},
                             headers=headers)
            r.raise_for_status()

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
        return name.replace(' ', '-').replace(',', '').capitalize()


class SmugMugConnection(BaseSmugMugConnection):
    """
    Connection class providing a higher level interface to connect to smugmug
    """

    def __init__(self,
                 account: str,
                 consumer_key: str,
                 consumer_secret: str,
                 access_token: str,
                 access_token_secret: str,
                 use_test_folder: bool = False):
        super().__init__(account=account,
                         consumer_key=consumer_key,
                         consumer_secret=consumer_secret,
                         access_token=access_token,
                         access_token_secret=access_token_secret,
                         use_test_folder=use_test_folder)

    async def folder_delete(self, folder: 'FolderOnSmugmug'):
        await self.request_delete(folder.uri)

    async def album_delete(self, album: 'AlbumOnSmugmug'):
        await self.request_delete(album.uri)

    async def image_delete(self, image: 'ImageOnSmugmug'):
        await self.request_delete(image.uri)

    async def folder_get(self,
                         folder_uri: str = None,
                         with_children: bool = True) -> Tuple[Dict, List[Dict], List[Dict]]:

        folder_uri = folder_uri or self._root_folder_uri

        r = await self.request_get(folder_uri)
        folder = r['Folder']

        # Node get the children
        sub_folders, albums = None, None

        if with_children:
            if 'Folders' in folder['Uris']:
                sub_folders = await self._unpack_pagination(folder['Uris']['Folders']['Uri'], object_name='Folder')

            albums = await self._unpack_pagination(folder['Uris']['FolderAlbums']['Uri'], object_name='Album')

        return folder, sub_folders, albums

    async def folder_create(self,
                            parent_folder: 'FolderOnSmugmug',
                            folder_on_disk: 'FolderOnDisk'):
        """
        Create a new sub-folder under the parent folder
        """

        assert parent_folder.is_folder and folder_on_disk.is_folder

        post_url = parent_folder.record['Uris']['Folders']['Uri']

        r = await self.request_post(post_url,
                                    json={
                                        'Name': folder_on_disk.name,
                                        'UrlName': self._encode_uri_name(folder_on_disk.name),
                                        'Privacy': 'Unlisted',
                                    })

        return r['Folder']

    async def album_create(self,
                           parent_folder: 'FolderOnSmugmug',
                           album_on_disk: Union['FolderOnDisk', 'AlbumOnDisk']) -> Dict:
        """
        Create a new album under the parent folder
        """
        assert parent_folder.is_folder and album_on_disk.is_album

        # post_url = parent_folder.record['Uris']['FolderAlbums']['Uri']
        #
        # r = await self.request_post(post_url,
        #                             json={
        #                                 'Name': album_on_disk.name,
        #                                 'UrlName': self._encode_uri_name(album_on_disk.name),
        #                                 'Privacy': 'Unlisted',
        #                             })

        # For some unknown reason, the Smugmug API returns 400 errors when trying to POST to the 'FolderAlbums' Uri
        # The workaround is to use the 'Node' endpoint and then lookup the album record
        node_url = parent_folder.record['Uris']['Node']['Uri']
        r = await self.request_post(f'{node_url}!children',
                                    json={
                                        'Name': album_on_disk.name,
                                        # 'UrlName': self._encode_uri_name(album_on_disk.name),
                                        # 'Privacy': 'Unlisted',
                                        'Type': 'Album',
                                    })

        # Wait for eventual consistency to settle before we lookup the record again
        await asyncio.sleep(0.5)

        # Lookup the album record
        album_url = r['Node']['Uris']['Album']['Uri']
        r = await self.request_get(album_url)

        return r['Album']

    async def album_images_get(self, album_record: Dict) -> List[Dict]:
        return await self._unpack_pagination(relative_uri=album_record['Uris']['AlbumImages']['Uri'],
                                             object_name='AlbumImage')

    async def get_download_url(self, image_on_smugmug: 'ImageOnSmugmug') -> str:
        # TODO: Problem! The downloaded name has the original (.avi / .mov) extension but the stored format is mp4.
        if image_on_smugmug.is_video and 'LargestVideo' in image_on_smugmug.record['Uris']:
            # Need to fetch the largest video url - videos are NOT accessible via 'ArchivedUri'
            r = await self.request_get(image_on_smugmug.record['Uris']['LargestVideo']['Uri'])
            return r['LargestVideo']['Url']

        # The archived version holds the original copy of the photo (but not for videos)
        return image_on_smugmug.record['ArchivedUri']

    @retry(retry_policy)
    async def image_download(self,
                             to_album: 'AlbumOnDisk',
                             image_on_smugmug: 'ImageOnSmugmug'):
        """
        Download a single image from the Album on Smugmug to a folder on disk
        """
        image_on_disk = ImageOnDisk(album=to_album, relative_path=image_on_smugmug.relative_path)

        logger.debug(f'Downloading {image_on_smugmug} to {to_album}')

        temp_file_name = f"{image_on_disk.disk_path}.tmp"

        with open(temp_file_name, 'wb') as f:
            async for chunk in self.stream(await self.get_download_url(image_on_smugmug)):
                f.write(chunk)

        # Now that we have completed writing the file to disk, we can use a rename operation to make that download
        # 'atomic'. If the process failed mid-download, the scan will pick the image again for download.
        if os.path.exists(image_on_disk.disk_path):
            os.remove(image_on_disk.disk_path)

        os.rename(temp_file_name, image_on_disk.disk_path)

        logger.info(f'Downloaded {image_on_smugmug}')

    @retry(retry_policy)
    async def image_upload(self,
                           to_album: 'AlbumOnSmugmug',
                           image_on_disk: ImageOnDisk,
                           image_to_replace: 'ImageOnSmugmug' = None):
        """
        Upload an image to an album

        :param ImageOnDisk image_on_disk: Image to upload
        :param ImageOnSmugmug image_to_replace: Image to replace (optional)
        :param AlbumOnSmugmug to_album: Album to upload to
        """

        try:
            action = 'Upload' if image_to_replace is None else 'Replace'
            logger.debug(f'{action} {image_on_disk}')

            # Read the entire file into memory for multipart upload (and for the oauth signature to work)
            with open(image_on_disk.disk_path, 'rb') as f:
                image_data = f.read()

            await self.upload(album_uri=to_album.album_uri,
                              image_data=image_data,
                              image_name=image_on_disk.name,
                              keywords=image_on_disk.keywords,
                              image_to_replace_uri=image_to_replace.image_uri if image_to_replace else None)

            logger.info(f'{action}ed {image_on_disk}')

        except HTTPError:
            logger.exception(f'Failed to upload {image_on_disk} to {to_album}')
            raise

    async def _unpack_pagination(self, relative_uri: str, object_name: str) -> List[Dict]:
        """
        Materialized full list of items (through pagination)
        """

        async def iter_items() -> Generator[Dict, None, None]:
            # Run the initial request
            response = await self.request_get(relative_uri)

            items = response.get(object_name) or []
            for item in items:
                yield item

            # Now check if we need to get more pages, if so, iterate
            paging = response.get('Pages') or {}
            total_count = paging.get('Total') or len(items)
            items_found = len(items)

            while total_count > items_found:
                response = await self.request_get(relative_uri, params={'start': items_found + 1, 'count': 100})
                items = response.get(object_name)
                if items:
                    for item in items:
                        yield item
                        items_found += 1

        return [a async for a in iter_items()]
