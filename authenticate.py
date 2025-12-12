import ee
import os
from ee import ServiceAccountCredentials

service_account = os.environ["EE_SERVICE_ACCOUNT"]
creds = ServiceAccountCredentials(
    email=service_account,
    key_file="ee-key.json"
)

ee.Initialize(credentials=creds)
print("Earth Engine authentication successful.")
