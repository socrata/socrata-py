from requests.auth import HTTPBasicAuth

class Authorization(object):
    """
    Manages basic authorization for accessing the socrata API.
    This is passed into the `Socrata` object once, which is the entry
    point for all operations.

    Can be used with either username/password or cookies. If a user
    name and password are provided, HTTP Basic Auth will be used. Otherwise,
    cookies will be used for authentication.

    Username/password example:
        auth = Authorization(
            "data.seattle.gov",
            username=os.environ['SOCRATA_USERNAME'],
            password=os.environ['SOCRATA_PASSWORD']
        )
        publishing = Socrata(auth)

    Cookies example:
        auth = Authorization(
            "data.seattle.gov",
            cookies=dict(cookie_name="cookie-value")
        )
        publishing = Socrata(auth)
    """
    def __init__(self, domain, /, username=None, password=None, request_id_prefix='', *, cookies=None):
        self.domain = domain

        if not ((username and password) or cookies):
            raise ValueError("Either username/password or cookies must be provided")

        if cookies and not isinstance(cookies, dict):
            raise TypeError("cookies must be a dictionary")

        self.username = username
        self.password = password

        self.proto = 'https://'
        self.verify = True

        self._request_id_prefix = request_id_prefix

        # Set up authentication method
        if username and password:
            self.basic = HTTPBasicAuth(self.username, self.password)
            self.cookies = dict()
        else:
            self.basic = None
            self.cookies = cookies or dict()

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
