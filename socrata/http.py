import json
import os
import requests
import binascii
import logging
log = logging.getLogger(__name__)

def noop(*args, **kwargs):
    pass

def generate_request_id(length = 32):
    return binascii.hexlify(os.urandom(int(length/2))).decode('utf8')

def gen_headers(extra = {}):
    d =  {
        'user-agent': 'publish-py',
        'content-type': 'application/json',
        'accept': 'application/json',
        'x-socrata-requestid': generate_request_id()
    }
    d.update(extra)
    return d


def prepare(headers):
    all_headers = gen_headers(headers)
    return all_headers, all_headers['x-socrata-requestid']

def respond(response, request_id = None):
    try:
        if response.status_code in [200, 201, 202]:
            return (True, response.json())
        else:
            log.warning("Request failed with %s, request_id was %s", response.status_code, request_id)
            return (False, response.json())
    except Exception: # json.decoder.JSONDecodeError isn't always a thing???? WHY PYTHON
        log.error("Request raised an exception, request_id was %s", request_id)
        return (False, {'error': 'json', 'content': response.content})

def post(path, auth = None, data = None, headers = {}):
    (headers, request_id) = prepare(headers)
    log.info('POST %s %s', path, request_id)
    return respond(requests.post(
        path,
        headers = headers,
        auth = auth.basic,
        verify = auth.verify,
        data = data
    ), request_id = request_id)


def put(path, auth = None, data = None, headers = {}):
    (headers, request_id) = prepare(headers)
    log.info('PUT %s %s', path, request_id)
    return respond(requests.put(
        path,
        headers = headers,
        auth = auth.basic,
        verify = auth.verify,
        data = data
    ), request_id = request_id)


def patch(path, auth = None, data = None, headers = {}):
    (headers, request_id) = prepare(headers)
    log.info('PATCH %s %s', path, request_id)
    return respond(requests.patch(
        path,
        headers = headers,
        auth = auth.basic,
        verify = auth.verify,
        data = data
    ), request_id = request_id)

def get(path, auth = None, params = {}, headers = {}):
    (headers, request_id) = prepare(headers)
    log.info('GET %s %s', path, request_id)
    return respond(requests.get(
        path,
        params = params,
        headers = headers,
        auth = auth.basic,
        verify = auth.verify
    ), request_id = request_id)

def delete(path, auth = None, headers = {}):
    (headers, request_id) = prepare(headers)
    log.info('DELETE %s %s', path, request_id)
    return respond(requests.delete(
        path,
        headers = headers,
        auth = auth.basic,
        verify = auth.verify
    ), request_id = request_id)
