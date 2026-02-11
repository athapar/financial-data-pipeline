import os
import dotenv

dotenv.load_dotenv()

polygon_api_key = os.getenv("POLYGON_API_KEY")
fred_api_key = os.getenv("FRED_API_KEY")


POLYGON_REQUEST_URL = "https://api.massive.com/v3/"
FRED_REQUEST_URL = "https://api.stlouisfed.org/fred/"

SERIES_ID = "CPIAUCSL"

example_request_fred = f"https://api.stlouisfed.org/fred/series/observations?series_id={SERIES_ID}&api_key={fred_api_key}&file_type=json"
print(example_request_fred)