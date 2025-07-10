import json, time, uuid, random, os
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv


load_dotenv() 

BLOB_CONNECTION_STR = os.getenv("BLOB_CONNECTION_STR")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")
EVENT_HUB_CONN_STR = os.getenv("EVENT_HUB_CONN_STR")
try:
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STR)
    event_producer = EventHubProducerClient.from_connection_string(conn_str=EVENT_HUB_CONN_STR)
except Exception as e:
    print(f"❌ Error inicializando servicios: {e}")
    exit(1)

def generar_transaccion():
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": f"CUST{random.randint(1000, 9999)}",
        "amount": round(random.uniform(5.0, 20000.0), 2),
        "timestamp": datetime.utcnow().isoformat(),
        "merchant": random.choice(["Amazon", "Spotify", "Starbucks", "Airbnb"]),
        "country": random.choice(["AR", "US", "BR", "MX"]),
        "channel": random.choice(["MOBILE", "WEB", "ATM"]),
        "category": random.choice(["SHOPPING", "ENTERTAINMENT", "TRAVEL", "FOOD"])
    }

# Loop de simulación
try:
    while True:
        transaccion = generar_transaccion()
        data_json = json.dumps(transaccion)
        print(f"\nNueva transacción generada: {transaccion}")

        # Subir a Azure Blob
        blob_name = f"transacciones/txn_{int(time.time())}_{str(uuid.uuid4())[:8]}.json"
        try:
            blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
            blob_client.upload_blob(data_json, overwrite=True)
            print(f"✅ Subida a Blob Storage: {blob_name}")
        except Exception as e:
            print(f"❌ Error subiendo a Blob: {e}")

        # Enviar a Event Hub
        try:
            event_data = EventData(data_json)
            with event_producer:
                batch = event_producer.create_batch()
                batch.add(event_data)
                event_producer.send_batch(batch)
            print("✅ Enviada a Event Hub")
        except Exception as e:
            print(f"❌ Error enviando a Event Hub: {e}")

        time.sleep(5)  # 5 seg
        
except KeyboardInterrupt:
    print("\n⏹ Simulación detenida por el usuario")
except Exception as e:
    print(f"❌ Error inesperado: {e}")
