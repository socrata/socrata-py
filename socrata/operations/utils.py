class SocrataException(Exception):
    def __init__(self, message, response):
        super(Exception, self).__init__(message + ':\n' + str(response))
        self.response = response

def get_filename(data, filename):
    if (filename is None) and getattr(data, 'name', False):
        filename = data.name
    elif filename is None:
        raise SocrataException("When creating without a file handle, you must include the filename as the second argument")
    return filename
