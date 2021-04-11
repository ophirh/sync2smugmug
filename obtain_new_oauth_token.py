import pprint
import sys
from urllib.parse import urlencode, urlunsplit, parse_qsl, urlsplit

from rauth import OAuth1Session, OAuth1Service

from sync2smugmug.config import config
from sync2smugmug.smugmug.connection import SmugMugConnection

OAUTH_ORIGIN = 'https://secure.smugmug.com'
REQUEST_TOKEN_URL = OAUTH_ORIGIN + '/services/oauth/1.0a/getRequestToken'
ACCESS_TOKEN_URL = OAUTH_ORIGIN + '/services/oauth/1.0a/getAccessToken'
AUTHORIZE_URL = OAUTH_ORIGIN + '/services/oauth/1.0a/authorize'


def get_service() -> OAuth1Service:
    service = OAuth1Service(name='sync2smugmug',
                            consumer_key=config.consumer_key,
                            consumer_secret=config.consumer_secret,
                            request_token_url=REQUEST_TOKEN_URL,
                            access_token_url=ACCESS_TOKEN_URL,
                            authorize_url=AUTHORIZE_URL,
                            base_url=SmugMugConnection.API_BASE_URL)
    return service


def add_auth_params(auth_url, access=None, permissions=None):
    if access is None and permissions is None:
        return auth_url

    parts = urlsplit(auth_url)
    query = parse_qsl(parts.query, True)
    if access is not None:
        query.append(('Access', access))

    if permissions is not None:
        query.append(('Permissions', permissions))

    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query, True), parts.fragment))


def main():
    """
    This example interacts with its user through the console, but it is
    similar in principle to the way any non-web-based application can obtain an
    OAuth authorization from a user.
    """
    service = get_service()

    # First, we need a request token and secret, which SmugMug will give us.
    # We are specifying "oob" (out-of-band) as the callback because we don't
    # have a website for SmugMug to call back to.
    rt, rts = service.get_request_token(params={'oauth_callback': 'oob'})

    # Second, we need to give the user the web URL where they can authorize our
    # application.
    auth_url = add_auth_params(service.get_authorize_url(rt), access='Full', permissions='Modify')
    print(f'Go to {auth_url} in a web browser.')

    # Once the user has authorized our application, they will be given a
    # six-digit verifier code. Our third step is to ask the user to enter that
    # code:
    verifier = input('Enter the six-digit code: ').strip()

    # Finally, we can use the verifier code, along with the request token and
    # secret, to sign a request for an access token.
    at, ats = service.get_access_token(rt, rts, params={'oauth_verifier': verifier})

    # The access token we have received is valid forever, unless the user
    # revokes it.  Let's make one example API request to show that the access
    # token works.
    print(f'Access token: {at}')
    print(f'Access token secret: {ats}')

    session = OAuth1Session(service.consumer_key,
                            service.consumer_secret,
                            access_token=at,
                            access_token_secret=ats)

    auth_user = session.get(f'{SmugMugConnection.API_BASE_URL}!authuser', headers={'Accept': 'application/json'}).text

    print('Auth User:')
    pprint.pprint(auth_user)


if __name__ == '__main__':
    main()
