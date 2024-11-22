import json
import boto3

device_state = {
    "device0": 0,
    "device1": 0,
    "device2": 0,
    "device3": 0,
    "device4": 0
}

iot_client = boto3.client("iot-data", region_name="us-east-2")

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))
    global device_state
    try:
        print("Received event:", json.dumps(event))

        vehicle_id = event.get("vehicle_id")
        co2_value = event.get("vehicle_CO2")
        
        if co2_value is None:
            co2_value =0

        if vehicle_id is None:
            raise ValueError("Missing 'vehicle_id' or 'vehicle_CO2' in the event")

        device_id = f"device{vehicle_id}"
        co2_value = float(co2_value)

        if device_id in device_state:
            if co2_value > device_state[device_id]:  
                device_state[device_id] = co2_value
        else:
            device_state[device_id] = co2_value  

        response_topic = "myVehicleTopicStatus"
        iot_client.publish(
            topic=response_topic,
            qos=0,
            payload=json.dumps(device_state) 
        )
        print(f"Published updated state to topic '{response_topic}': {device_state}")

        return {"status": "success", "device_state": device_state}
    except Exception as e:
        print(f"Error: {e}")
        return {"status": "error", "message": str(e)}

