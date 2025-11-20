from requests.auth import HTTPBasicAuth

class Authorization(object):
    """
    Manages basic authorization for accessing the socrata API.
    This is passed into the `Socrata` object once, which is the entry
    point for all operations.

        auth = Authorization(
            "data.seattle.gov",
            os.environ['SOCRATA_USERNAME'],
            os.environ['SOCRATA_PASSWORD']
        )
        publishing = Socrata(auth)
    """
    def __init__(self, domain, username, password, request_id_prefix = ''):
        self.domain = domain
        self.username = username
        self.password = password

        self.proto = 'https://'
        self.verify = True

        self._request_id_prefix = request_id_prefix

        self.basic = HTTPBasicAuth(self.username, self.password)
        self.cookies = dict()

    def live_dangerously(self):
        """
        Disable SSL checking. Note that this should *only* be used while developing
        against a local Socrata instance.
        """
        import requests
        from requests.packages.urllib3.exceptions import InsecureRequestWarning
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        self.verify = False

    def request_id_prefix(self):
        return self._request_id_prefix

class CookieAuthorization(object):
    """
    Manages basic authorization for accessing the socrata API.
    This is passed into the `Socrata` object once, which is the entry
    point for all operations.

        auth = CookieAuthorization(
            "data.seattle.gov",
            dict(cookie_name="cookie-value")
        )
        publishing = Socrata(auth)
    """
    def __init__(self, domain, cookies, request_id_prefix = ''):
        self.domain = domain
        self.cookies = cookies

        self.proto = 'https://'
        self.verify = True

        self._request_id_prefix = request_id_prefix

        self.basic = None

    def live_dangerously(self):
        """
        Disable SSL checking. Note that this should *only* be used while developing
        against a local Socrata instance.
        """
        import requests
        from requests.packages.urllib3.exceptions import InsecureRequestWarning
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        self.verify = False

    def request_id_prefix(self):
        return self._request_id_prefix

