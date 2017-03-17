import json
import requests
from socrata.http import headers, respond
from occamy import Socket

def get_token(auth, fourfour):
    response = requests.post(
        'http://{domain}/api/authenticate'.format(
            domain = auth.domain
        ),
        headers = headers({
          'Host': 'localhost'
        }),
        params = {
            'username': auth.username,
            'password': auth.password
        },
        auth = auth.basic,
        verify = auth.verify
    )

    return respond(requests.get(
        'https://{domain}/api/update/{fourfour}/token'.format(
            fourfour = fourfour,
            domain = auth.domain
        ),
        headers = headers(),
        verify = auth.verify,
        cookies = response.cookies
    ))

def connect(auth, fourfour):
      (ok, response) = get_token(auth, fourfour)
      assert ok, "Failed to get channel token %s" % response

      token = response['token']
      socket = Socket("wss://{domain}/api/update/socket".format(
          domain = auth.domain
      ),
      params = {
          'fourfour': fourfour,
          'token': token
      })
      socket.connect()

      return socket
