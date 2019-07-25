import paho.mqtt.client as paho
import ast
import os


class MQTT:
    def __init__(self):
        self.client = paho.Client()
        self.queue_fac_up, self.queue_fac_down = None, None

    def start_listener(self, queue_fac_up, queue_fac_down):
        self.queue_fac_up, self.queue_fac_down = queue_fac_up, queue_fac_down

        def on_connect(client, userdata, flags, rc):
            print(f"[INFO] - MQTT - Connected with result code: {str(rc)}")
            self.client.subscribe("#")

        def on_message(client, userdata, msg):
            print(
                f"[INFO] - MQTT - Message received: {msg.topic} {msg.payload}")
            message = ast.literal_eval(msg.payload.decode())
            self.queue_fac_up.put(message)

        self.client.on_connect = on_connect
        self.client.on_message = on_message
        mqtt_host = os.environ.get('MQTT_URI', '0.0.0.0')
        self.client.connect(mqtt_host)
        self.client.loop_start()

    def stop_listener(self):
        self.client.loop_stop()
        self.client.disconnect()
