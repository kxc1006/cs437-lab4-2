import json
import time
import pandas as pd
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

device_st = 0
device_end = 5
data_path = "dataset/vehicle{}.csv"
certificate_formatter = "certificates/device{}/certificate.pem.crt"
key_formatter = "certificates/device{}/private.pem.key"

clientEndPoint = "auwilrrwh5dln-ats.iot.us-east-2.amazonaws.com"
clientCredentials = "certificates/CA_file/AmazonRootCA1.pem"


class MQTTClient:
    def __init__(self, device_id, cert, key):
        self.device_id = str(device_id)
        self.client = AWSIoTMQTTClient(self.device_id)
        self.client.configureEndpoint(clientEndPoint, 8883)
        self.client.configureCredentials(clientCredentials, key, cert)
        self.client.configureOfflinePublishQueueing(-1)
        self.client.configureDrainingFrequency(2)
        self.client.configureConnectDisconnectTimeout(10)
        self.client.configureMQTTOperationTimeout(5)

    def publish(self, topic, payload):
        try:
            self.client.publishAsync(topic, payload, 0)
            print(f"Published to topic '{topic}': {payload}")
        except Exception as e:
            print(f"Failed to publish message: {e}")


data = [pd.read_csv(data_path.format(i)) for i in range(device_st, device_end)]

clients = []
for device_id in range(device_st, device_end):
    client = MQTTClient(
        device_id,
        certificate_formatter.format(device_id),
        key_formatter.format(device_id),
    )
    client.client.connect()
    clients.append(client)

counts = [0] * len(clients)

while True:
    command = input("Press 's' to send one row per device, or 'n' to stop: ").strip().lower()

    if command == "s":
        for i, client in enumerate(clients):
            if counts[i] < len(data[i]):
                co2_value = data[i].loc[counts[i], "vehicle_CO2"]
                payload = json.dumps({"vehicle_id": str(i), "vehicle_CO2": co2_value})
                
                client.publish("myVehicleTopic", payload)
                print(f"Device {i}, Row {counts[i]}, CO2: {co2_value}")
                
                counts[i] += 1
            else:
                print(f"No more data for device {i}")

            time.sleep(1)
    elif command == "n":
        for client in clients:
            client.client.disconnect()
        print("All devices disconnected")
        break
    else:
        print("Invalid command. Try again.")

