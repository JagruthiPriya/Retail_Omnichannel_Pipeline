import requests
import json
from pathlib import Path
from datetime import datetime

# Project folder
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Raw API data folder
RAW_FOLDER = PROJECT_ROOT / "data" / "raw" / "api"
RAW_FOLDER.mkdir(parents=True, exist_ok=True)

# DummyJSON API endpoint
API_URL = "https://dummyjson.com/carts?limit=10"



def ingest_orders():

    print("Starting REST API ingestion...")

    try:
        # Send GET request to API
        response = requests.get(API_URL, timeout=30)

        # Check HTTP response
        response.raise_for_status()

        # Convert response into Python dictionary
        data = response.json()

        # Validate API response
        if "carts" not in data or not isinstance(data["carts"], list):
            raise ValueError("Invalid API response: carts list is missing")

        orders = data["carts"]

        if not orders:
            raise ValueError("API returned an empty carts list")

        # Generate unique file name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        file_path = RAW_FOLDER / f"api_orders_{timestamp}.json"

        # Save original API response
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)

        print("\nAPI ingestion successful!")
        print("Total orders received:", len(orders))
        print("Total records available:", data.get("total"))
        print("Saved file:", file_path)

        print("\nFirst order details:")
        print(json.dumps(orders[0], indent=4))

    except requests.exceptions.RequestException as error:
        print("API connection error:", error)
        raise

    except (ValueError, KeyError) as error:
        print("Data processing error:", error)
        raise


if __name__ == "__main__":
    ingest_orders()
