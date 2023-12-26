import asyncio
import calendar
import dataclasses
import hashlib
import logging
from concurrent import futures
from datetime import datetime
from pathlib import Path
from typing import List, Union, Dict, Generator, AsyncIterator, ClassVar

import aioretry
from authlib.integrations import httpx_client, requests_client
import httpx
import requests

from sync2smugmug import configuration

logger = logging.getLogger(__name__)


def retry_policy(info: aioretry.RetryInfo) -> aioretry.RetryPolicyStrategy:
    """
    Retry policy for connection issues
    """
    if isinstance(info.exception, (httpx.TransportError, requests.HTTPError)):
        if info.fails < 3:
            logger.warning(f"Connection failed ({info.exception})! retrying...")
            return False, info.fails * 1.0

    # Raise any other exception
    return True, 0


class SmugmugCoreConnection:
    """
    Connection class implementing the basic auth and transport protocols with Smugmug
    """
    API_SERVER = "https://api.smugmug.com"
    API_PREFIX = "api/v2"
    API_BASE_URL = f"{API_SERVER}/{API_PREFIX}"
    TIMEOUT = 10

    def __init__(self, connection_params: configuration.ConnectionParams):
        self._connection_params = connection_params
        self._user = None
        self._headers = {
            "Host": "www.smugmug.com",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self._root_folder_uri = None
        self._test_root_folder_uri = None

        # Session objects
        self._async_session: httpx_client.AsyncOAuth1Client | None = None
        self._session: requests.Session | None = None

        self._threadpool = None

    @property
    def root_folder_uri(self) -> str:
        """
        The root URI is the Smugmug account root folder URI unless we're in test upload mode. In case of test
        upload, we will redirect everything into a special 'Test' folder.
        """
        return self._test_root_folder_uri if self._connection_params.test_upload else self._root_folder_uri

    def is_test_root_folder_uri(self, uri: str) -> bool:
        """
        Used to identify the test root folder (to allow ignoring it and prevent circular dependency)
        """
        return uri == self._test_root_folder_uri

    async def connect(self):
        # Create an async session (this is how we should work always)
        self._async_session = httpx_client.AsyncOAuth1Client(
            self._connection_params.consumer_key,
            self._connection_params.consumer_secret,
            token=self._connection_params.access_token,
            token_secret=self._connection_params.access_token_secret,
        )

        # Also create a sync client (because some of the Smugmug APIs do not work with the async client)
        self._session = requests_client.OAuth1Session(
            self._connection_params.consumer_key,
            self._connection_params.consumer_secret,
            token=self._connection_params.access_token,
            token_secret=self._connection_params.access_token_secret,
        )

        # Issue a request to get the user's JSON
        response = await self.request_get(f"user/{self._connection_params.account}")
        self._user = response["User"]
        self._root_folder_uri = self._user["Uris"]["Folder"]["Uri"]
        self._test_root_folder_uri = f"{self._root_folder_uri}/Test"

        self._threadpool = futures.ThreadPoolExecutor(thread_name_prefix="uploader")

    async def disconnect(self):
        if self._async_session is not None:
            await self._async_session.aclose()
            self._async_session = None

        if self._session is not None:
            self._session.close()
            self._session = None

        if self._threadpool is not None:
            self._threadpool.shutdown(wait=True)

    async def _request(self, method, url, *args, **kwargs) -> httpx.Response:
        assert self._async_session is not None, "Call connect first!"

        try:
            if "headers" not in kwargs:
                kwargs["headers"] = self._headers

            r = await self._async_session.request(method, url, *args, **kwargs)
            r.raise_for_status()

            return r

        except httpx.HTTPError as e:
            logger.exception(f"Failed request: {method}, Url: {url}, args: {args}, kwargs: {kwargs}")
            raise e

    async def request_get(self, relative_uri: str, *args, **kwargs) -> Dict:
        r = await self._request("GET", self._format_url(relative_uri), *args, **kwargs)
        return r.json()["Response"]

    async def request_post(self, relative_uri: str, json_data: Union[Dict, List], *args, **kwargs) -> Dict:
        """
        Posts data.

        For some (currently unknown) reason, the async version of the post consistently returns 400,
        where the sync version works.
        """
        assert self._session is not None, "Call connect first!"

        if "headers" not in kwargs:
            kwargs["headers"] = self._headers

        def sync_post() -> httpx.Response:
            return self._session.post(
                self._format_url(relative_uri), *args, json=json_data, **kwargs
            )

        try:
            # Run sync version in a threadpool instead!
            r = await asyncio.get_running_loop().run_in_executor(self._threadpool, sync_post)
            r.raise_for_status()

            return r.json()["Response"]

        except requests.HTTPError as e:
            logger.exception(f"Failed to post to {relative_uri} ({str(json_data)}) - {str(e)}")

            raise e

    async def request_delete(self, relative_uri: str):
        await self._request("DELETE", self._format_url(relative_uri))

    async def request_stream(self, absolute_uri: str) -> AsyncIterator[bytes]:
        assert self._async_session is not None, "Call connect first!"

        async with self._async_session.stream(method="GET", url=absolute_uri, timeout=self.TIMEOUT) as r:
            r.raise_for_status()

            async for chunk in r.aiter_raw(chunk_size=1024 * 1024):
                yield chunk

    @aioretry.retry(retry_policy)
    async def request_upload(
            self,
            album_uri: str,
            image_path: Path,
            image_name: str,
            dry_run: bool,
            image_to_replace_uri: str = None,
    ):
        assert self._async_session is not None and self._session is not None, "Call connect first!"

        if dry_run:
            return

        # Sync function that will run in a thread pool
        image_data: bytes = image_path.read_bytes()

        headers = {
            "X-Smug-AlbumUri": album_uri,
            "X-Smug-Title": image_name,
            "X-Smug-Caption": image_name,
            # "X-Smug-Keywords": keywords,
            "X-Smug-ResponseType": "JSON",
            "X-Smug-Version": "v2",
            "Content-MD5": hashlib.md5(image_data).hexdigest(),
        }

        if image_to_replace_uri:
            headers["X-Smug-ImageUri"] = image_to_replace_uri

        def sync_post() -> httpx.Response:
            return self._session.post(
                "https://upload.smugmug.com/",
                files={image_name: image_data},
                headers=headers,
            )

        # Run sync version in a threadpool instead (async version does not work)
        r = await asyncio.get_running_loop().run_in_executor(
            self._threadpool,
            sync_post
        )

        r.raise_for_status()

        response = r.json()
        if response["stat"] == "fail":
            raise httpx.HTTPError(f"Failed to upload image {image_name} ({response['message']})")

    @classmethod
    def _format_url(cls, uri: str) -> str:
        if uri.startswith("/"):
            uri = uri[1:]

        prefix = f"{cls.API_PREFIX}/"
        if uri.startswith(prefix):
            uri = uri[len(prefix):]

        return f"{cls.API_BASE_URL}/{uri}"

    @classmethod
    def encode_uri_name(cls, name: str) -> str:
        return name.replace(" ", "-").replace(",", "").capitalize()

    async def request_download(self, image_uri: str, local_path: Path):
        """
        Download a single image from the Album on Smugmug to a source_folder on disk
        """
        temp_file_name = local_path.with_suffix(".tmp")

        with open(temp_file_name, "wb") as f:
            async for chunk in self.request_stream(f"{self.API_BASE_URL}{image_uri}"):
                f.write(chunk)

        # Now that we have completed writing the file to disk, we can use a rename operation to make that download
        # 'atomic'. If the process failed mid-download, the scan will pick the image again for download.
        local_path.unlink(missing_ok=True)

        temp_file_name.rename(local_path)

    async def paginate(
            self,
            relative_uri: str,
            object_name: str,
            page_size: int = 100
    ) -> Generator[Dict, None, None]:
        """
        Yield full list of items (through pagination)
        """

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
                relative_uri, params={"start": items_found + 1, "count": page_size}
            )

            items = response.get(object_name)
            for item in items:
                yield item
                items_found += 1


@dataclasses.dataclass
class SmugmugRecord:
    record: Dict = dataclasses.field(repr=False)
    name: str = dataclasses.field(init=False)
    uri: str = dataclasses.field(init=False)

    def __post_init__(self):
        self.name = self.record["Name"]
        self.uri = self.record["Uri"]


@dataclasses.dataclass
class SmugmugImage(SmugmugRecord):
    size: int = dataclasses.field(init=False)
    is_video: bool = dataclasses.field(init=False)

    def __post_init__(self):
        self.name = self.record["FileName"]
        self.uri = self.record["Uri"]
        self.is_video = self.record["IsVideo"]
        self.size = self.record.get("OriginalSize", self.record.get("ArchivedSize"))


@dataclasses.dataclass
class SmugmugFolder(SmugmugRecord):
    sub_folders_uri: str | None = dataclasses.field(init=False)
    albums_uri: str = dataclasses.field(init=False)

    def __post_init__(self):
        super().__post_init__()

        folders_uri_info = self.record["Uris"].get("Folders")
        self.sub_folders_uri = folders_uri_info["Uri"] if folders_uri_info is not None else None

        self.albums_uri = self.record["Uris"]["FolderAlbums"]["Uri"]


@dataclasses.dataclass
class SmugmugAlbum(SmugmugRecord):
    DATE_ALBUM_FORMAT: ClassVar[str] = "%Y-%m-%dT%H:%M:%S%z"  # data format of smugmug dates

    images_uri: str = dataclasses.field(init=False)
    image_count: int = dataclasses.field(init=False)
    last_updated: float = dataclasses.field(init=False)

    def __post_init__(self):
        super().__post_init__()

        self.images_uri = self.record["Uris"]["AlbumImages"]["Uri"]
        self.image_count = self.record["ImageCount"]

        # Return the epoch of the last update (for easier saving in a json file)
        last_updated_date = datetime.strptime(self.record["LastUpdated"], self.DATE_ALBUM_FORMAT)
        images_last_updated_date = datetime.strptime(self.record["ImagesLastUpdated"], self.DATE_ALBUM_FORMAT)
        maximum_date = max(last_updated_date, images_last_updated_date)
        self.last_updated = calendar.timegm(maximum_date.timetuple())
