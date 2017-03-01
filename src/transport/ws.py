import json
import requests
from src.http import headers, respond
from occamy import Socket

def get_token(auth, fourfour):
    response = requests.post(
        'https://{domain}/api/authenticate'.format(
            domain = auth.domain
        ),
        headers = headers(),
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
      assert ok, "Failed to get channel token"

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
