import os
import json
import requests
import time
from dotenv import load_dotenv

load_dotenv()

# Configuration
CONNECT_REST_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "postgres-source-connector"

connector_config = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.getenv("POSTGRES_HOST"),
        "database.port": os.getenv("POSTGRES_PORT"),
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB"),
        # Topic name
        "topic.prefix": "banking_server",
        # Table include list
        "table.include.list": "public.customers, public.accounts, public.transactions, public.customers_accounts",
        # Plugin
        "plugin.name": "pgoutput",
        "slot.name": "banking_debezium_slot",
        "publication.autocreate.mode": "filtered", # Only replicate specified tables
        # Data type handling
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
        # Snapshot mode
        "snapshot.mode": "initial", # it will take all the existing data in the tables at the first run and then stream the changes
        # error handling
        "errors.log.enable": "true",
        "errors.log.include.messages": "true"
    }
}

# Helpers
def check_connector_status():
    """Check if the connector is already existing."""
    try:
        r = requests.get(f"{CONNECT_REST_URL}/{CONNECTOR_NAME}")
        if r.status_code == 200:
            return True, r.json()
        return False, None
    except requests.exceptions.RequestException as e:
        print(f"Can not connect: {e}")
        exit(1)

def delete_connector():
    """Delete the current connector if you want to reset."""
    print(f"Deleting connector {CONNECTOR_NAME}...")
    r = requests.delete(f"{CONNECT_REST_URL}/{CONNECTOR_NAME}")
    if r.status_code in [204, 200]:
        print("Connector deleted.")
    else:
        print(f"Failed to delete connector: {r.text}")
        exit(1)

if __name__ == "__main__":
    exists, _ = check_connector_status()
    if not exists:
        print(f"Creating connector {CONNECTOR_NAME}...")
        headers = {"Content-Type": "application/json"}

        try:
            response = requests.post(CONNECT_REST_URL, headers=headers, data=json.dumps(connector_config))
            if response.status_code == 201:
                print("Connector created successfully.")
                print(f"Data will start streaming from PostgreSQL to Kafka topics with prefix '{connector_config['config']['topic.prefix']}'")
            else:
                print(f"Failed to create connector: {response.text}")
        except Exception as e:
            print(f"Error occurred: {e}")
    else:
        print(f"Connector {CONNECTOR_NAME} already exists. No action taken")