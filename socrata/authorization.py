from requests.auth import HTTPBasicAuth

class Authorization(object):
    def __init__(self, domain, username, password):
        self.domain = domain
        self.username = username
        self.password = password

        self.proto = 'https://'
        self.verify = True

        self.basic = HTTPBasicAuth(self.username, self.password)

    def live_dangerously(self):
        import requests
        from requests.packages.urllib3.exceptions import InsecureRequestWarning
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        self.verify = False
