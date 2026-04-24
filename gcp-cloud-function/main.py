import functions_framework
import requests
import pandas as pd
import json
import logging
import os

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FMP-CloudFunction")

# API Key (Set in Google Cloud Environment Variables)
FMP_API_URL = "https://financialmodelingprep.com/api/v3/stock/full/real-time-price"
FMP_API_KEY = os.getenv("FMP_API_KEY", "EnrozQzRrrYmlPKAJkpoJlYFAi2PduM1")

@functions_framework.http
def fetch_fmp_data(request):
    """Cloud Function to fetch stock data from FMP API and return JSON"""

    try:
        # Fetch data from API
        response = requests.get(f"{FMP_API_URL}?apikey={FMP_API_KEY}")

        if response.status_code == 200:
            data = response.json()

            # Convert JSON response to DataFrame
            df = pd.DataFrame(data)

            # Convert DataFrame back to JSON
            json_data = df.to_json(orient="records")

            logger.info("Successfully fetched FMP data")

            return json_data, 200
        else:
            logger.error(f"Failed to fetch data: {response.status_code}, {response.text}")
            return json.dumps({"error": "Failed to fetch data"}), response.status_code

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return json.dumps({"error": str(e)}), 500