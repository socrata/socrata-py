def noop(*args, **kwargs):
    pass

def headers(extra = {}):
    d =  {
        'user-agent': 'publish-py',
        'content-type': 'application/json',
        'accept': 'application/json'
    }
    d.update(extra)
    return d

def respond(response):
    if response.status_code in [200, 201, 202]:
        return (True, response.json())
    else:
        return (False, response.json())
