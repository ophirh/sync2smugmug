import asyncio
import logging
import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from typing import List, Union, Dict, Generator, AsyncIterator

from aioretry import retry, RetryInfo, RetryPolicyStrategy
from authlib.integrations.httpx_client import AsyncOAuth1Client
from httpx import HTTPError, Response, TransportError

logger = logging.getLogger(__name__)


# This example shows the usage with python typings
def retry_policy(info: RetryInfo) -> RetryPolicyStrategy:
    """
    Retry policy for connection issues
    """
    if isinstance(info.exception, TransportError):
        if info.fails < 3:
            logger.warning(f"Connection failed ({info.exception})! retrying...")
            return False, info.fails * 1.0

    # Raise any other exception
    return True, 0


class SmugMugConnection:
    """
    Connection class implementing the basic auth and transport protocols with Smugmug
    """

    API_SERVER = "https://api.smugmug.com"
    API_PREFIX = "api/v2"
    API_BASE_URL = f"{API_SERVER}/{API_PREFIX}"
    CONCURRENT_CONNECTIONS = 25

    def __init__(
        self,
        account: str,
        consumer_key: str,
        consumer_secret: str,
        access_token: str,
        access_token_secret: str,
        use_test_folder: bool = False,
    ):
        self._account = account
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret
        self._access_token = access_token
        self._access_token_secret = access_token_secret
        self._use_test_folder = use_test_folder
        self._user = None
        self._headers = {
            "Host": "www.smugmug.com",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self._sem = asyncio.Semaphore(self.CONCURRENT_CONNECTIONS)
        self._root_folder_uri = None

        # Session objects
        self._asession = None
        self._session = None

        self._threadpool = None

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
        await self.connect()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    async def connect(self):
        self._asession = AsyncOAuth1Client(
            self._consumer_key,
            self._consumer_secret,
            token=self._access_token,
            token_secret=self._access_token_secret,
        )

        # Issue a request to get the user's JSON
        response = await self.request_get(f"user/{self._account}")
        self._user = response["User"]

        self._root_folder_uri = self._user["Uris"]["Folder"]["Uri"]
        if self._use_test_folder:
            # Switch to use the Test folder
            self._root_folder_uri = f"{self._root_folder_uri}/Test/Test2"

        from authlib.integrations.requests_client import OAuth1Session

        self._session = OAuth1Session(
            self._consumer_key,
            self._consumer_secret,
            token=self._access_token,
            token_secret=self._access_token_secret,
        )

        self._threadpool = ThreadPoolExecutor(
            max_workers=self.CONCURRENT_CONNECTIONS * 3, thread_name_prefix="uploader"
        )

    async def disconnect(self):
        if self._asession is not None:
            await self._asession.aclose()
            self._asession = None

        if self._session is not None:
            self._session.close()
            self._session = None

        if self._threadpool is not None:
            self._threadpool.shutdown(wait=True)

    async def request(self, method, url, *args, **kwargs) -> Response:
        assert self._asession is not None

        async with self._sem:
            try:
                if "headers" not in kwargs:
                    kwargs["headers"] = self._headers

                r = await self._asession.request(method, url, *args, **kwargs)
                r.raise_for_status()

                return r

            except HTTPError as e:
                logger.exception(
                    f"Failed request: {method}, Url: {url}, args: {args}, kwargs: {kwargs}"
                )
                raise e

    async def request_get(self, relative_uri: str, *args, **kwargs) -> Dict:
        r = await self.request("GET", self._format_url(relative_uri), *args, **kwargs)
        return r.json()["Response"]

    async def request_post(
        self, relative_uri: str, json: Union[Dict, List], *args, **kwargs
    ) -> Dict:
        assert self._asession is not None and self._session is not None

        if "headers" not in kwargs:
            kwargs["headers"] = self._headers

        def sync_post() -> Response:
            return self._session.post(
                self._format_url(relative_uri), *args, json=json, **kwargs
            )

        async with self._sem:
            try:
                # Not sure why the async version fails!!!
                # r = await self._asession.post('https://upload.smugmug.com/',
                #                               data=image_data,
                #                               headers=headers)

                # Run sync version in a threadpool instead!
                r = await asyncio.get_running_loop().run_in_executor(
                    self._threadpool, sync_post
                )
                r.raise_for_status()

            except Exception as e:
                logger.exception(f"Failed to post {relative_uri} to {str(json)}")

                raise e

            return r.json()["Response"]

    async def request_delete(self, relative_uri: str):
        async with self._sem:
            await self.request("DELETE", self._format_url(relative_uri))

    async def request_stream(self, absolute_uri: str) -> AsyncIterator[bytes]:
        assert self._asession is not None

        async with self._sem:  # Limit concurrency to avoid timeouts
            async with self._asession.stream("GET", absolute_uri) as r:
                r.raise_for_status()

                async for chunk in r.aiter_raw(chunk_size=1024 * 1024):
                    yield chunk

    async def request_upload(
        self,
        album_uri: str,
        image_data: bytes,
        image_name: str,
        keywords: str,
        image_to_replace_uri: str = None,
    ):
        assert self._asession is not None and self._session is not None

        headers = {
            "X-Smug-AlbumUri": album_uri,
            "X-Smug-Title": image_name,
            "X-Smug-Caption": image_name,
            "X-Smug-Keywords": keywords,
            "X-Smug-ResponseType": "JSON",
            "X-Smug-Version": "v2",
            "Content-MD5": hashlib.md5(image_data).hexdigest(),
        }

        if image_to_replace_uri:
            headers["X-Smug-ImageUri"] = image_to_replace_uri

        # Sync function that will run in a thread pool
        def sync_post() -> Response:
            return self._session.post(
                "https://upload.smugmug.com/",
                files={image_name: image_data},
                headers=headers,
            )

        async with self._sem:  # Limit concurrency to avoid timeouts
            # Not sure why the async version fails!!!
            # r = await self._asession.post('https://upload.smugmug.com/',
            #                               data=image_data,
            #                               headers=headers)

            # Run async version in a threadpool instead!
            r = await asyncio.get_running_loop().run_in_executor(
                self._threadpool, sync_post
            )

            r.raise_for_status()

            response = r.json()
            if response["stat"] == "fail":
                raise HTTPError(
                    f"Failed to upload image {image_name} ({response['message']})"
                )

    @classmethod
    def _format_url(cls, uri: str) -> str:
        if uri.startswith("/"):
            uri = uri[1:]

        prefix = f"{cls.API_PREFIX}/"
        if uri.startswith(prefix):
            uri = uri[len(prefix) :]

        return f"{cls.API_BASE_URL}/{uri}"

    @classmethod
    def encode_uri_name(cls, name: str) -> str:
        return name.replace(" ", "-").replace(",", "").capitalize()

    @retry(retry_policy)
    async def request_download(self, image_uri: str, local_path: str):
        """
        Download a single image from the Album on Smugmug to a folder on disk
        """
        temp_file_name = f"{local_path}.tmp"

        with open(temp_file_name, "wb") as f:
            async for chunk in self.request_stream(image_uri):
                f.write(chunk)

        # Now that we have completed writing the file to disk, we can use a rename operation to make that download
        # 'atomic'. If the process failed mid-download, the scan will pick the image again for download.
        if os.path.exists(local_path):
            os.remove(local_path)

        os.rename(temp_file_name, local_path)

    async def unpack_pagination(
        self, relative_uri: str, object_name: str
    ) -> List[Dict]:
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
            paging = response.get("Pages") or {}
            total_count = paging.get("Total") or len(items)
            items_found = len(items)

            while total_count > items_found:
                response = await self.request_get(
                    relative_uri, params={"start": items_found + 1, "count": 100}
                )
                items = response.get(object_name)
                if items:
                    for item in items:
                        yield item
                        items_found += 1

        return [a async for a in iter_items()]
