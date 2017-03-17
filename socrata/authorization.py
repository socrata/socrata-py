from requests.auth import HTTPBasicAuth

class Authorization(object):
    def __init__(self, domain, username, password):
        self.domain = domain
        self.username = username
        self.password = password

        # This should be hardcoded to true - false now because
        # i'm testing against my local stack
        self.verify = False
        import requests
        from requests.packages.urllib3.exceptions import InsecureRequestWarning
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

        self.basic = HTTPBasicAuth(self.username, self.password)
