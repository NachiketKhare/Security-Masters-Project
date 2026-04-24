import requests
import json

def lambda_handler(event, context):
    # FMP API URL and API Key
    API_KEY = "EnrozQzRrrYmlPKAJkpoJlYFAi2PduM1"  # Replace with your FMP API key
    url = f"https://financialmodelingprep.com/api/v3/stock/full/real-time-price?apikey={API_KEY}"
    
    try:
        # API Call
        response = requests.get(url)
        
        # Validate response
        if response.status_code == 200:
            data = response.json()  # Parse JSON response
            
            # Log data (optional for debugging purposes)
            print("API Data Fetched Successfully")
            print(json.dumps(data, indent=4))
            
            # Return successful response
            return data
        else:
            # Handle API failure
            print(f"Failed to fetch data: {response.status_code}, {response.text}")
            return {
                "statusCode": response.status_code,
                "body": json.dumps({
                    "message": "Failed to fetch data",
                    "error": response.text
                })
            }
    
    except Exception as e:
        # Handle any exceptions
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "An error occurred while fetching data",
                "error": str(e)
            })
        }