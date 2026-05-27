import json
import logging
import paho.mqtt.client as mqtt
import requests
import threading
import time
import websockets.sync.client as ws_client

log = logging.getLogger()


class HomeAssistant:
    websocket_reconnect_interval = 5
    ha_entity_state_subscription_id = 21
    connection_type_api = "api"

    def __init__(self, ha_config: dict, apply_lock_state: callable):
        self.apply_lock_state = apply_lock_state

        self.ha_config = ha_config
        self.connection_type = ha_config.get("connectionType")
        
        if self.connection_type == HomeAssistant.connection_type_api:
            self.ha_server_address = self.get_ha_server_address()
            self.ha_ws_address = self.get_ha_ws_address()
            self.ha_api_token = ha_config.get("apiToken")
            self.ha_entity_id = ha_config.get("entityId")

        self.start_listener_thread()

    def initialize_mqtt_with_reconnect(self):
        while True:
            try:
                self.initialize_mqtt()
            except Exception as e:
                log.error(f"Error initializing MQTT client: {e}. Reconnecting in {HomeAssistant.websocket_reconnect_interval} seconds...")
                time.sleep(HomeAssistant.websocket_reconnect_interval)  # Wait before reconnecting

    def initialize_mqtt(self):
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, reconnect_on_failure = True)
        if self.ha_config.get("mqttUseAuth"):
            self.mqtt_client.username_pw_set(
                username=self.ha_config.get("mqttUsername"),
                password=self.ha_config.get("mqttPassword"),
            )
        if self.ha_config.get("mqttUseSSL"):
            log.info("Using SSL for MQTT")
            self.mqtt_client.tls_set()
            self.mqtt_client.tls_insecure_set(True)

        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_message
        self.mqtt_lock_state_get_topic = self.ha_config.get("mqttLockStateGetTopic")
        self.mqtt_lock_state_set_topic = self.ha_config.get("mqttLockStateSetTopic")

        mqttHost = self.ha_config.get("mqttHost")
        mqttPort = int(self.ha_config.get("mqttPort"))
        log.info(f"Connecting to MQTT broker - Host: {mqttHost}, Port: {mqttPort}")
        self.mqtt_client.connect(mqttHost, mqttPort)
        self.mqtt_client.loop_forever(retry_first_connection=True)

    def on_mqtt_connect(self, client, userdata, flags, reason_code, properties):
        log.info(f"Connected to MQTT broker. result code {reason_code}")
        self.mqtt_client.subscribe(self.mqtt_lock_state_get_topic)

    def on_mqtt_message(self, client, userdata, msg):
        try:
            log.info(f"Received MQTT message on {msg.topic}: {msg.payload}")
            if msg.topic == self.mqtt_lock_state_get_topic:
                lock_state = msg.payload.decode()
                self.handle_lock_state(lock_state)
        except Exception as e:
            log.error(f"Error processing MQTT message: {e}")

    def send_lock_state_via_mqtt(self, lock_state):
        try:
            self.mqtt_client.publish(
                self.mqtt_lock_state_set_topic, payload=lock_state, qos=1
            )
            log.info(f"Published lock state '{lock_state}' to topic {self.mqtt_lock_state_set_topic}")
        except Exception as e:
            log.error(f"Error publishing lock state to MQTT: {e}")

    def get_ha_server_address(self):
        scheme = "https" if self.ha_config.get("useSSL") else "http"
        return f"{scheme}://{self.ha_config.get('serverAddress')}"
    
    def get_ha_ws_address(self):
        scheme = "wss" if self.ha_config.get("useSSL") else "ws"
        return f"{scheme}://{self.ha_config.get('serverAddress')}"

    def get_lock_state_from_ha(self):
        try:
            # Fetch the current lock state from Home Assistant
            url = f"{self.ha_server_address}/api/states/{self.ha_entity_id}"
            headers = {
                "Authorization": f"Bearer {self.ha_api_token}",
                "Content-Type": "application/json",
            }
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                state = response.json()
                return state.get("state")
            else:
                log.error(f"Non 200 response when getting lock state from HA: {response.text}")
                raise ConnectionError(f"Non 200 response when getting lock state from HA: {response.text}")
        except Exception as e:
            log.error(f"Error fetching lock state from HA: {e}")
            raise ConnectionError(f"Error fetching lock state from HA: {e}")

    # Method to set the lock state in Home Assistant (locked = closed, unlocked = open)
    def set_lock_state_in_ha(self, lock_target_state):
        lock_target_state = "locked" if lock_target_state == 1 else "unlocked"
        log.info(f"Setting lock state to {lock_target_state} in HA")
        if self.connection_type == HomeAssistant.connection_type_api:
            self.set_lock_state_in_ha(lock_target_state)
        else:
            self.send_lock_state_via_mqtt(lock_target_state)

    def set_lock_state_in_ha(self, lock_state):
        log.info(f"Setting lock state to {lock_state} in HA")
        service = "lock" if lock_state == 'locked' else "unlock"
        try:
            url = f"{self.ha_server_address}/api/services/lock/{service}"
            headers = {
                "Authorization": f"Bearer {self.ha_api_token}",
                "Content-Type": "application/json",
            }
            payload = {
                "entity_id": self.ha_entity_id
            }
            response = requests.post(url, json=payload, headers=headers)
            if response.status_code == 200:
                log.info(f"Successfully set lock state to {lock_state}")
            else:
                log.error(f"Failed to set lock state to {lock_state}: {response.text}")
                raise ConnectionError(f"Failed to set lock state to {lock_state}: {response.text}")
        except Exception as e:
            log.error(f"Error setting lock state in Home Assistant: {e}")

    def start_listener_thread(self):
        # Start the listener in a separate thread
        target = self.websocket_listener_with_reconnect if self.connection_type == HomeAssistant.connection_type_api else self.initialize_mqtt_with_reconnect
        thread = threading.Thread(
            target=target,
            daemon=True
        )
        thread.start()

    def websocket_listener_with_reconnect(self):
        while True:
            try:
                self.websocket_listener()
            except Exception as e:
                log.error(f"WebSocket listener encountered an error: {e}. Reconnecting in {HomeAssistant.websocket_reconnect_interval} seconds...")
                time.sleep(HomeAssistant.websocket_reconnect_interval)  # Wait before reconnecting

    def websocket_listener(self):
        uri = f"{self.ha_ws_address}/api/websocket"
        log.info(f"HA WebSocket URL: {uri}")

        with ws_client.connect(uri) as websocket:
            # Wait for "auth_required" message
            initial_message = websocket.recv()
            initial_data = json.loads(initial_message)
            if initial_data.get("type") == "auth_required":
                log.info("HA Server requires authentication")
                self.websocket_authenticate(websocket)
            else:
                log.error("Unexpected initial message from server")
                raise ConnectionError("Unexpected initial message from server")

            self.subscribe_to_lock_state(websocket)
            self.handle_lock_state(self.get_lock_state_from_ha())

            while True:
                message = websocket.recv()
                message_data = json.loads(message)
                if message_data.get("type") == "event" and message_data.get("id") == HomeAssistant.ha_entity_state_subscription_id:
                    self.process_lock_state_update_message(message_data)

    def websocket_authenticate(self, websocket):
        log.info(f"HA WebSocket Authenticating")
        auth_message = {
            "type": "auth",
            "access_token": self.ha_api_token
        }
        websocket.send(json.dumps(auth_message))
        auth_response = websocket.recv()
        auth_data = json.loads(auth_response)
        if auth_data.get("type") == "auth_ok":
            log.info("WebSocket authentication successful")
        else:
            log.error("WebSocket authentication failed")
            raise ConnectionError("Failed to authenticate with WebSocket")

    def subscribe_to_lock_state(self, websocket):
        subscription_message = {
            "id": HomeAssistant.ha_entity_state_subscription_id,
            "type": "subscribe_trigger",
            "trigger": {
                "platform": "state",
                "entity_id": self.ha_entity_id
            }
        }
        websocket.send(json.dumps(subscription_message))
        log.info(f"Subscribed to state changes for entity: {self.ha_entity_id}")

    def process_lock_state_update_message(self, message_data):
        new_state = message_data["event"]["variables"]["trigger"]["to_state"]["state"]
        self.handle_lock_state(new_state)

    def handle_lock_state(self, lock_state):
        log.info(f"Lock state changed to: {lock_state}")
        if lock_state == "locked":
            self.apply_lock_state(1, 1)
        if lock_state == "unlocked":
            self.apply_lock_state(0, 0)
        if lock_state == "closing":
            self.apply_lock_state(0, 1)
        if lock_state == "opening":
            self.apply_lock_state(1, 0)
        # Don't apply updates in case of "opening" and "closing"
