import json
import os
import requests
import binascii
import logging
log = logging.getLogger(__name__)

# No request that we send should ever take more than 5 minutes to process!
REQUEST_TIMEOUT = 5 * 60

class TimeoutException(Exception):
    pass

class UnexpectedResponseException(Exception):
    def __init__(self, status, body):
        super(UnexpectedResponseException, self).__init__("Unexpected status {status} {body}".format(status=status, body=body))
        self.body = body
        self.status = status

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
    if response.status_code in [200, 201, 202]:
        if is_json(response):
            return response.json()
        else:
            return response
    else:
        log.warning("Request failed with %s, request_id was %s", response.status_code, request_id)
        if is_json(response):
            raise UnexpectedResponseException(response.status_code, response.json())
        else:
            raise UnexpectedResponseException(response.status_code, response)

def pluck_resource(body):
    return body['resource']

def post(path, auth = None, data = None, headers = {}, params = {}):
    (headers, request_id) = prepare(headers, auth)
    log.info('POST %s %s', path, request_id)
    return respond(requests.post(
        path,
        headers = headers,
        auth = auth.basic,
        params = params,
        verify = auth.verify,
        data = data,
        timeout = REQUEST_TIMEOUT
    ), request_id = request_id)



def put(path, auth = None, data = None, headers = {}):
    (headers, request_id) = prepare(headers, auth)
    log.info('PUT %s %s', path, request_id)
    return respond(requests.put(
        path,
        headers = headers,
        auth = auth.basic,
        verify = auth.verify,
        data = data,
        timeout = REQUEST_TIMEOUT
    ), request_id = request_id)


def patch(path, auth = None, data = None, headers = {}):
    (headers, request_id) = prepare(headers, auth)
    log.info('PATCH %s %s', path, request_id)
    return respond(requests.patch(
        path,
        headers = headers,
        auth = auth.basic,
        verify = auth.verify,
        data = data,
        timeout = REQUEST_TIMEOUT
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
        timeout = REQUEST_TIMEOUT,
        **kwargs
    ), request_id = request_id)

def delete(path, auth = None, headers = {}):
    (headers, request_id) = prepare(headers, auth)
    log.info('DELETE %s %s', path, request_id)
    return respond(requests.delete(
        path,
        headers = headers,
        auth = auth.basic,
        verify = auth.verify,
        timeout = REQUEST_TIMEOUT
    ), request_id = request_id)
