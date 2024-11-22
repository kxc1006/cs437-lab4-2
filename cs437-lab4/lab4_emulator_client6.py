import time
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

clientEndPoint = "auwilrrwh5dln-ats.iot.us-east-2.amazonaws.com"
clientCredentials = "certificates/CA_file/AmazonRootCA1.pem"
certificate_file = "certificates/device1/certificate.pem.crt"
private_key_file = "certificates/device1/private.pem.key"

class MQTTSubscriber:
    def __init__(self, client_id):
        self.client_id = client_id
        self.client = AWSIoTMQTTClient(self.client_id)
        self.client.configureEndpoint(clientEndPoint, 8883)
        self.client.configureCredentials(clientCredentials, private_key_file, certificate_file)
        self.client.configureOfflinePublishQueueing(-1)
        self.client.configureDrainingFrequency(2)
        self.client.configureConnectDisconnectTimeout(10)
        self.client.configureMQTTOperationTimeout(5)

    def connect(self):
        try:
            self.client.connect()
            print(f"Subscriber '{self.client_id}' connected")
        except Exception as e:
            print(f"Failed to connect subscriber '{self.client_id}': {e}")

    def subscribe(self, topic):
        try:
            self.client.subscribe(topic, 1, self.on_message)
            print(f"Subscribed to topic '{topic}'")
        except Exception as e:
            print(f"Failed to subscribe to topic '{topic}': {e}")

    @staticmethod
    def on_message(client, userdata, message):
        print(f"Received message from topic '{message.topic}': {message.payload.decode()}")

    def disconnect(self):
        try:
            self.client.disconnect()
            print(f"Subscriber '{self.client_id}' disconnected")
        except Exception as e:
            print(f"Failed to disconnect subscriber '{self.client_id}': {e}")


if __name__ == "__main__":
    topic_to_subscribe = "myVehicleTopicStatus"  
    subscriber = MQTTSubscriber(client_id="SubscriberClient")
    
    subscriber.connect()

    subscriber.subscribe(topic_to_subscribe)

    try:
        print("Waiting for messages...")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping subscriber...")
        subscriber.disconnect()

