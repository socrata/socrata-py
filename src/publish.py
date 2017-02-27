import argparse
from src.resource import Collection
from src.revisions import Revisions

class Publish(Collection):
    def __init__(self, auth):
        super(Publish, self).__init__(auth)
        self.revisions = Revisions(auth)


