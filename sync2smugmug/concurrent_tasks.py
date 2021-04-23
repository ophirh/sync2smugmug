"""
Set of functions called concurrently (multi-process)

This is set in a python file of its own to properly control imports and dependencies
"""

import traceback


def image_download(connection, image, to_album):
    """

    :param SmugMugConnection connection: connection
    :param ImageOnSmugmug image: image to download
    :param AlbumOnDisk to_album: destination album
    """

    print(f'Downloading {image} to {to_album}')

    from .disk.image import ImageOnDisk
    image_on_disk = ImageOnDisk(album=to_album, relative_path=image.relative_path)

    with connection.request('GET', image.image_download_uri, stream=True) as r:
        with open(image_on_disk.disk_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=64 * 1024):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)


def image_upload(connection, to_album, image_on_disk, image_to_replace=None):
    """
    Upload an image to an album
    Uses the special upload API (https://api.smugmug.com/api/v2/doc/reference/upload.html)
    """

    try:
        print(f'Uploading {image_on_disk}')

        headers = {
            'X-Smug-AlbumUri': to_album.album_uri,
            'X-Smug-Title': image_on_disk.name,
            'X-Smug-Caption': image_on_disk.caption,
            'X-Smug-Keywords': image_on_disk.keywords,
            'X-Smug-ImageUri': image_to_replace.image_uri if image_to_replace else None,
            'X-Smug-ResponseType': 'JSON',
            'X-Smug-Version': 'v2',
        }

        # Read the entire file into memory (for the oauth signature to work)
        with open(image_on_disk.disk_path, 'rb') as f:
            image_data = f.read()

        r = connection.request('POST', 'https://upload.smugmug.com/',
                               files={'file': (image_on_disk.name, image_data)},
                               headers=headers)
        r.raise_for_status()

    except:
        traceback.print_exc()
        raise
