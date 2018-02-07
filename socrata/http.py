import json
import os
import requests
import binascii
import logging
log = logging.getLogger(__name__)

class TimeoutException(Exception):
    pass

def noop(*args, **kwargs):
    pass

def generate_request_id(length = 32):
    return binascii.hexlify(os.urandom(int(length/2))).decode('utf8')

def gen_headers(extra = {}, auth = None):

    request_id_prefix = ''
    if auth:
        request_id_prefix = auth.request_id_prefix()
    d =  {
        'user-agent': 'publish-py',
        'content-type': 'application/json',
        'accept': 'application/json',
        'x-socrata-requestid': request_id_prefix + generate_request_id(32 - len(request_id_prefix))
    }
    d.update(extra)
    return d


def prepare(headers, auth):
    all_headers = gen_headers(headers, auth)
    return all_headers, all_headers['x-socrata-requestid']

def is_json(response):
    return 'application/json' in response.headers['Content-Type']

def respond(response, request_id = None):
    try:
        if response.status_code in [200, 201, 202]:
            if is_json(response):
                return (True, response.json())
            else:
                return (True, response)
        else:
            log.warning("Request failed with %s, request_id was %s", response.status_code, request_id)
            if is_json(response):
                return (False, response.json())
            else:
                return (False, response)
    except Exception: # json.decoder.JSONDecodeError isn't always a thing???? WHY PYTHON
        log.error("Request raised an exception, request_id was %s", request_id)
        return (False, {'error': 'json', 'content': response.content})

def post(path, auth = None, data = None, headers = {}):
    (headers, request_id) = prepare(headers, auth)
    log.info('POST %s %s', path, request_id)
    return respond(requests.post(
        path,
        headers = headers,
        auth = auth.basic,
        verify = auth.verify,
        data = data
    ), request_id = request_id)



def put(path, auth = None, data = None, headers = {}):
    (headers, request_id) = prepare(headers, auth)
    log.info('PUT %s %s', path, request_id)
    return respond(requests.put(
        path,
        headers = headers,
        auth = auth.basic,
        verify = auth.verify,
        data = data
    ), request_id = request_id)


def patch(path, auth = None, data = None, headers = {}):
    (headers, request_id) = prepare(headers, auth)
    log.info('PATCH %s %s', path, request_id)
    return respond(requests.patch(
        path,
        headers = headers,
        auth = auth.basic,
        verify = auth.verify,
        data = data
    ), request_id = request_id)

def get(path, auth = None, params = {}, headers = {}, **kwargs):
    (headers, request_id) = prepare(headers, auth)
    log.info('GET %s %s', path, request_id)
    return respond(requests.get(
        path,
        params = params,
        headers = headers,
        auth = auth.basic,
        verify = auth.verify,
        **kwargs
    ), request_id = request_id)

def delete(path, auth = None, headers = {}):
    (headers, request_id) = prepare(headers, auth)
    log.info('DELETE %s %s', path, request_id)
    return respond(requests.delete(
        path,
        headers = headers,
        auth = auth.basic,
        verify = auth.verify
    ), request_id = request_id)
