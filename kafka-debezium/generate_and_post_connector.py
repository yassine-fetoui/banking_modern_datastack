import os
import json
import requests
from dotenv import load_dotenv
import time

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

# -----------------------------
# Debezium Connect API endpoint
# -----------------------------
CONNECT_URL = os.getenv("DEBEZIUM_CONNECT_URL", "http://localhost:8083")
CONNECTOR_NAME = "postgres-connector"

# -----------------------------
# Helper functions
# -----------------------------
def check_connect_health():
    """Check if Kafka Connect is running"""
    try:
        response = requests.get(f"{CONNECT_URL}/", timeout=5)
        if response.status_code == 200:
            print(f"✅ Kafka Connect is running at {CONNECT_URL}")
            return True
        else:
            print(f"⚠️ Kafka Connect returned status {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Cannot reach Kafka Connect at {CONNECT_URL}: {e}")
        return False

def list_connectors():
    """List all existing connectors"""
    try:
        response = requests.get(f"{CONNECT_URL}/connectors", timeout=5)
        if response.status_code == 200:
            connectors = response.json()
            print(f"📋 Existing connectors: {connectors if connectors else 'None'}")
            return connectors
        else:
            print(f"⚠️ Failed to list connectors: {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ Error listing connectors: {e}")
        return []

def delete_connector(name):
    """Delete an existing connector"""
    try:
        response = requests.delete(f"{CONNECT_URL}/connectors/{name}", timeout=5)
        if response.status_code == 204:
            print(f"🗑️ Deleted existing connector '{name}'")
            time.sleep(2)
            return True
        else:
            print(f"⚠️ Failed to delete connector: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error deleting connector: {e}")
        return False

def resume_connector(name):
    """Resume a paused connector"""
    try:
        response = requests.put(f"{CONNECT_URL}/connectors/{name}/resume", timeout=5)
        if response.status_code == 202:
            print(f"▶️ Resumed connector '{name}'")
            time.sleep(2)
            return True
        else:
            print(f"⚠️ Failed to resume connector: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error resuming connector: {e}")
        return False

def pause_connector(name):
    """Pause a running connector"""
    try:
        response = requests.put(f"{CONNECT_URL}/connectors/{name}/pause", timeout=5)
        if response.status_code == 202:
            print(f"⏸️ Paused connector '{name}'")
            return True
        else:
            print(f"⚠️ Failed to pause connector: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error pausing connector: {e}")
        return False

def restart_connector(name):
    """Restart a connector"""
    try:
        response = requests.post(f"{CONNECT_URL}/connectors/{name}/restart", timeout=5)
        if response.status_code in [202, 204]:
            print(f"🔄 Restarted connector '{name}'")
            time.sleep(2)
            return True
        else:
            print(f"⚠️ Failed to restart connector: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error restarting connector: {e}")
        return False

def get_connector_status(name):
    """Get connector status"""
    try:
        response = requests.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=5)
        if response.status_code == 200:
            status = response.json()
            state = status['connector']['state']
            
            # Color-code the state
            state_emoji = {
                'RUNNING': '✅',
                'PAUSED': '⏸️',
                'FAILED': '❌',
                'UNASSIGNED': '⚠️'
            }
            
            print(f"\n📊 Connector '{name}' status:")
            print(f"   State: {state_emoji.get(state, '❓')} {state}")
            print(f"   Worker: {status['connector']['worker_id']}")
            
            if 'tasks' in status and status['tasks']:
                for i, task in enumerate(status['tasks']):
                    task_state = task['state']
                    print(f"   Task {i}: {state_emoji.get(task_state, '❓')} {task_state} (worker: {task['worker_id']})")
                    if task_state == 'FAILED' and 'trace' in task:
                        print(f"      Error: {task['trace'][:200]}...")
            
            return status
        else:
            print(f"⚠️ Failed to get status: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Error getting status: {e}")
        return None

def create_connector(config, recreate=False):
    """Create or recreate a connector"""
    if recreate:
        print(f"\n🔄 Recreating connector '{CONNECTOR_NAME}'...")
        delete_connector(CONNECTOR_NAME)
    
    print(f"\n🚀 Creating connector '{CONNECTOR_NAME}'...")
    
    headers = {"Content-Type": "application/json"}
    response = requests.post(
        f"{CONNECT_URL}/connectors",
        headers=headers,
        data=json.dumps(config),
        timeout=10
    )
    
    if response.status_code == 201:
        print(f"✅ Connector '{CONNECTOR_NAME}' created successfully!")
        return True
    elif response.status_code == 409:
        print(f"⚠️ Connector '{CONNECTOR_NAME}' already exists.")
        return False
    else:
        print(f"❌ Failed to create connector ({response.status_code}):")
        try:
            error_detail = response.json()
            print(f"   {json.dumps(error_detail, indent=2)}")
        except:
            print(f"   {response.text}")
        return False

# -----------------------------
# Build connector JSON
# -----------------------------
connector_config = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        
        # Database connection
        "database.hostname": os.getenv("POSTGRES_HOST"),
        "database.port": os.getenv("POSTGRES_PORT", "5432"),
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB"),
        
        # Kafka topic settings
        "topic.prefix": "banking_server",
        "table.include.list": "public.customers,public.accounts,public.transactions",
        
        # PostgreSQL-specific settings
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "publication.name": "banking_publication",
        "publication.autocreate.mode": "filtered",
        
        # Data handling
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
        "time.precision.mode": "adaptive",
        
        # Snapshot settings
        "snapshot.mode": "initial",
        
        # Schema and transformations
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
    },
}

# -----------------------------
# Main execution
# -----------------------------
if __name__ == "__main__":
    print("=" * 60)
    print("🔧 Debezium PostgreSQL Connector Setup")
    print("=" * 60)
    
    # Step 1: Check Kafka Connect health
    if not check_connect_health():
        print("\n❌ Kafka Connect is not available. Please start it first.")
        exit(1)
    
    # Step 2: List existing connectors
    print()
    existing_connectors = list_connectors()
    
    # Step 3: Check if connector exists and its status
    if CONNECTOR_NAME in existing_connectors:
        print(f"\n📌 Connector '{CONNECTOR_NAME}' already exists.")
        status = get_connector_status(CONNECTOR_NAME)
        
        if status and status['connector']['state'] == 'PAUSED':
            print("\n⚠️ Connector is PAUSED. Resuming...")
            resume_connector(CONNECTOR_NAME)
            time.sleep(2)
            get_connector_status(CONNECTOR_NAME)
        elif status and status['connector']['state'] == 'FAILED':
            print("\n❌ Connector has FAILED. Restarting...")
            restart_connector(CONNECTOR_NAME)
            time.sleep(3)
            get_connector_status(CONNECTOR_NAME)
    else:
        # Step 4: Create connector
        success = create_connector(connector_config, recreate=False)
        
        if success:
            print("\n⏳ Waiting 3 seconds for connector to initialize...")
            time.sleep(3)
            get_connector_status(CONNECTOR_NAME)
    
    print("\n" + "=" * 60)
    print("✅ Setup complete!")
    print("=" * 60)
    
    # Print helpful commands
    print("\n📖 Useful commands:")
    print(f"   Check status: curl {CONNECT_URL}/connectors/{CONNECTOR_NAME}/status")
    print(f"   Resume:       curl -X PUT {CONNECT_URL}/connectors/{CONNECTOR_NAME}/resume")
    print(f"   Pause:        curl -X PUT {CONNECT_URL}/connectors/{CONNECTOR_NAME}/pause")
    print(f"   Restart:      curl -X POST {CONNECT_URL}/connectors/{CONNECTOR_NAME}/restart")
    print(f"   Delete:       curl -X DELETE {CONNECT_URL}/connectors/{CONNECTOR_NAME}")
    print(f"   View config:  curl {CONNECT_URL}/connectors/{CONNECTOR_NAME}")