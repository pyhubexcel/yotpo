import os
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from sqlalchemy.exc import SQLAlchemyError

from sqlalchemy import select
from db import (
    engine,
    customer_history_table,
    history_items_table,
    orders_table
   
)
from utils import save_customer_history, save_history_items

load_dotenv()

# Define Yotpo API credentials
YOTPO_GUID = os.getenv("YOTPO_GUID")
YOTPO_API_KEY = os.getenv("YOTPO_API_KEY")
YOTPO_BASE_URL = os.getenv("YOTPO_BASE_URL")


app = FastAPI()

@app.get("/customer_history")
def get_customer_history(per_page: int = 100, page_info: str = None):
    try:
        params = {
            'guid': YOTPO_GUID,
            'api_key': YOTPO_API_KEY,
            'per_page': per_page
        }
        if page_info:
            params['page_info'] = page_info
        response = requests.get(
            f'{YOTPO_BASE_URL}/customers/recent',
            headers={
                'Authorization': f'Bearer {YOTPO_API_KEY}',
                'Content-Type': 'application/json'
            },
            params=params
        )
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.json())

        data = response.json()
        customer_history_data = data.get('customers', [])
        print(customer_history_data)
        next_page_info = data.get('metadata', {}).get('next_page_info', None)
        print(next_page_info)
        

        # Save customer data
        with engine.connect() as conn:
            save_customer_history(customer_history_data, customer_history_table, conn)
            print("Customer History Data Saved Successfully")

        return {"status": "success", "next_page_info": next_page_info}

    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except requests.RequestException as e:
        print(f"Request error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from Yotpo API.")



@app.get("/history_items")
def get_history_items():
    try:
        # Fetch customer data from the database (customer_id and email)
        with engine.connect() as conn:  
            result = conn.execute(select(customer_history_table.c.customer_id, customer_history_table.c.email)).mappings().all()
            customer_ids = {row['email']: row['customer_id'] for row in result}
        
        if not customer_ids:
            print("No customers found in the database.")
            return {"status": "no_customers_found"}

        customer_emails = list(customer_ids.keys())  # Get the emails of the customers already saved
        history_items_data = []

        # Step 2: Fetch detailed customer history for each customer based on emails
        for email in customer_emails:
            customer_response = requests.get(
                f'{YOTPO_BASE_URL}/customers',
                headers={
                    'Authorization': f'Bearer {YOTPO_API_KEY}',
                    'Content-Type': 'application/json'
                },
                params={
                    'guid': YOTPO_GUID,
                    'api_key': YOTPO_API_KEY,
                    'customer_email': email,
                    'with_history': 'true'  #mandatory
                }
            )

            if customer_response.status_code != 200:
                print(f"Error fetching history data for {email}: {customer_response.status_code}")
                continue

            customer_data = customer_response.json()
            print(f"Customer data for {email}:", customer_data)

            # Extract history items from the response
            history_items = customer_data.get('history_items', [])
            if history_items:  # Check if history_items are present
                for item in history_items:
                    # Ensure the customer_id is correctly mapped
                    customer_id = customer_ids.get(email)
                    if customer_id:
                        item['customer_id'] = customer_id 
                        history_items_data.append(item)

        

        # Save history items to the database if found
        if history_items_data:
            with engine.connect() as conn:  
                save_history_items(history_items_data, history_items_table, orders_table, conn) 
                print("History Items Data Saved Successfully")
        else:
            print("No History Items Found.")

    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Database error occurred.")
    except requests.RequestException as e:
        print(f"Request error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from Yotpo API.")

    return {"status": "success"}



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8009)
