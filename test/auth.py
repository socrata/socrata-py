import os
from src.authorization import Authorization

auth = Authorization(
  "localhost",
  os.environ['SOCRATA_LOCAL_USER'],
  os.environ['SOCRATA_LOCAL_PASS']
)

fourfour = "ij46-xpxe"
