from queue import Queue
from paho.mqtt import MQTTException
from MAVProxy.modules.lib import mp_settings
from MAVProxy.modules.lib import mp_module
import paho.mqtt.client as mqtt
import json
import numbers


class MqttModule(mp_module.MPModule):
    def __init__(self, mpstate):
        super(MqttModule, self).__init__(mpstate, "mqtt", "MQTT publisher and subscriber using Unix sockets")
        self.client = mqtt.Client(transport="unix")  # Use Unix socket transport
        self.device_prefix = ''
        self.mqtt_settings = mp_settings.MPSettings(
            [('socket_path', str, '/tmp/mosquitto.sock'),  # Default path to Unix socket
             ('name', str, 'mavproxy'),
             ('prefix', str, 'ardupilot/gcs'),
             ('subscribe_topic', str, 'ardupilot/cmd')  # Default topic for subscribing
             ])
        self.add_command('mqtt', self.mqtt_command, "mqtt module", ['connect', 'set (MQTTSETTING)'])
        self.add_completion_function('(MQTTSETTING)', self.mqtt_settings.completion)
        self.client.on_message = self.on_mqtt_message
        self.command_queue = Queue()

    def mavlink_packet(self, m):
        """Handle an incoming MAVLink packet"""
        try:
            data = self.convert_to_dict(m)
            self.client.publish(f'{self.mqtt_settings.prefix}/{m.get_type()}', json.dumps(data))
        except MQTTException as e:
            print(f'mqtt: Exception occurred: {e}')

    def connect(self):
        """Connect to MQTT broker and subscribe to topics"""
        try:
            socket_path = self.mqtt_settings.socket_path
            print(f'Connecting to Unix socket: {socket_path}')
            self.client.connect(socket_path)
            self.client.subscribe(self.mqtt_settings.subscribe_topic)
            print(f'Subscribed to topic: {self.mqtt_settings.subscribe_topic}')
            self.client.loop_start()  # Start a separate thread to handle MQTT messages
        except MQTTException as e:
            print(f'mqtt: Could not establish connection: {e}')
            return
        print('Connected and subscribing...')

    def mqtt_command(self, args):
        """Control behavior of the module"""
        if len(args) == 0:
            print(self.usage())
        elif args[0] == 'set':
            self.mqtt_settings.command(args[1:])
        elif args[0] == 'connect':
            self.connect()

    def usage(self):
        """Show help on command line options"""
        return "Usage: mqtt <set|connect>"

    def convert_to_dict(self, message):
        """Converts MAVLink message to Python dict"""
        if hasattr(message, '_fieldnames'):
            result = {}
            for field in message._fieldnames:
                result[field] = self.convert_to_dict(getattr(message, field))
            return result
        if isinstance(message, numbers.Number):
            return message
        return str(message)

    def on_mqtt_message(self, client, userdata, msg):
        """Handle incoming MQTT messages"""
        try:
            print(f'Received MQTT message on topic {msg.topic}')
            message_data = json.loads(msg.payload.decode('utf-8'))
            mavlink_message = self.convert_to_mavlink(message_data)
            if mavlink_message:
                # Add the MAVLink message to the queue for processing
                self.command_queue.put(mavlink_message)
        except Exception as e:
            print(f'mqtt: Error processing incoming message: {e}')

    def convert_to_mavlink(self, data):
        """Convert a dictionary to a MAVLink COMMAND_LONG message"""
        try:
            # Assume all messages fit the COMMAND_LONG format
            return self.master.mav.command_long_encode(
                data.get('target_system', 1),        # Default to system ID 1
                data.get('target_component', 1),    # Default to component ID 1
                data.get('command', 0),             # MAVLink command (required)
                data.get('confirmation', 0),        # Confirmation (default to 0)
                data.get('param1', 0),              # Param1
                data.get('param2', 0),              # Param2
                data.get('param3', 0),              # Param3
                data.get('param4', 0),              # Param4
                data.get('param5', 0),              # Param5
                data.get('param6', 0),              # Param6
                data.get('param7', 0)               # Param7
            )
        except Exception as e:
            print(f"mqtt: Failed to create MAVLink COMMAND_LONG message: {e}")
            return None

    def idle_task(self):
        """Process commands from the queue in the main MAVProxy thread."""
        if not self.command_queue.empty():
            mavlink_message = self.command_queue.get()
            try:
                self.master.mav.send(mavlink_message)
                print("MAVLink command sent successfully.")
            except Exception as e:
                print(f"Error sending MAVLink command: {e}")

def init(mpstate):
    """Initialize module"""
    return MqttModule(mpstate)
