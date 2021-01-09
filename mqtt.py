"""Interface with SMA inverters using JSON."""

import atexit
import sys
import socket
import random
import string
import time
import logging

import json
import paho.mqtt.client as mqtt

from configuration import (
    APPLICATION_LOG_LOGGER_NAME,
    MQTT_ENABLE,
    MQTT_CLIENT,
    MQTT_BROKER_IPADDR,
    MQTT_BROKER_PORT,
    MQTT_USERNAME,
    MQTT_PASSWORD,
)


logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)
local_vars = {}


def error_msg(code):
    """Convert a result code to string."""
    error_messages = {
        "0": "MQTT_ERR_SUCCESS",
        "1": "MQTT_ERR_NOMEM",
        "2": "MQTT_ERR_PROTOCOL",
        "3": "MQTT_ERR_INVAL",
        "4": "MQTT_ERR_NO_CONN",
        "5": "MQTT_ERR_CONN_REFUSED",
    }
    return error_messages.get(str(code), "unknown error code: " + str(code))


def on_disconnect(client, userdata, result_code):
    """Process the on_disconnect callback."""
    # pylint: disable=unused-argument
    client.connected = False
    client.disconnect_failed = False
    if result_code == mqtt.MQTT_ERR_SUCCESS:
        logger.info(f"MQTT client successfully disconnected")
    else:
        client.disconnect_failed = True
        logger.info(
            f"MQTT client unexpectedly disconnected: {error_msg(result_code)}, trying reconnect()")


def on_connect(client, userdata, flags, result_code):
    """Process the on_connect callback."""
    # pylint: disable=unused-argument
    if result_code == mqtt.MQTT_ERR_SUCCESS:
        client.connected = True
        logger.info(f"MQTT {userdata['Type']} client successfully connected to {userdata['IP']}:{userdata['Port']}")
    else:
        client.connection_failed = True
        logger.info(f"MQTT client connection failed: {error_msg(result_code)}")


def mqtt_exit():
    """Close the MQTT connection when exiting using atexit()."""
    # Disconnect the MQTT client from the broker
    local_vars['mqtt_client'].loop_stop()
    logger.info(f"MQTT client disconnect being called")
    local_vars['mqtt_client'].disconnect()


#
# Public
#

def publish(sensors):
    """Publish a dictionary of sensor keys amd values using MQTT."""
    # Check if MQTT is not connected to a broker or the sensor list is empty
    if 'mqtt_client' not in local_vars or not sensors:
        return

    # Separate out the sensor dictionaries
    for original_sensor in sensors:
        sensor = original_sensor.copy()
        if 'topic' not in sensor:
            logger.warning(f"'topic' not in sensor dictionary: {str(sensor)}")
            continue

        # Extract the topic and precision from the dictionary
        topic = sensor.pop('topic')
        precision = sensor.pop('precision', None)

        # Limit floats to the requested precision
        for key, value in sensor.items():
            if isinstance(value, dict):
                for dict_key, dict_value in value.items():
                    value[dict_key] = round(dict_value, precision) if precision else dict_value
            if isinstance(value, float):
                sensor[key] = round(value, precision) if precision else value

        # Encode each sensor in JSON and publish
        sensor_json = json.dumps(sensor)
        message_info = local_vars['mqtt_client'].publish(
            MQTT_CLIENT + "/" + topic, sensor_json
        )
        if message_info.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.warning(
                f"MQTT message topic '{topic}'' failed to publish: {error_msg(message_info.rc)}",
            )


def start():
    """Tests and caches the client MQTT broker connection."""
    if not MQTT_ENABLE:
        return True

    # Create a unique client name
    local_vars['clientname'] = (
        MQTT_CLIENT
        + "_"
        + "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
    )

    # Check if MQTT is configured properly, create the connection
    port = (1883, MQTT_BROKER_PORT)[MQTT_BROKER_PORT > 0]
    connection_type = ('authenticated', 'anonymous')[len(MQTT_USERNAME) == 0]
    client = mqtt.Client(
        local_vars['clientname'],
        userdata={'IP': MQTT_BROKER_IPADDR, 'Port': port, 'Type': connection_type},
    )

    # Setup and try to connect to the broker
    logger.info(
        f"Attempting {connection_type} MQTT client connection to {MQTT_BROKER_IPADDR}:{port}")

    client.on_connect = on_connect
    client.username_pw_set(username=MQTT_USERNAME, password=MQTT_PASSWORD)
    try:
        # Initialize flags for connection status
        client.connected = client.connection_failed = False
        time_limit = 4.0
        sleep_time = 0.1
        client.loop_start()
        client.connect(MQTT_BROKER_IPADDR, port=port)

        # Wait for the connection to occur or timeout
        while not client.connected and not client.connection_failed and time_limit > 0:
            time.sleep(sleep_time)
            time_limit -= sleep_time
        if not client.connected and not client.connection_failed:
            logger.error("MQTT timeout error, no response")

    except socket.gaierror:
        client.loop_stop()
        logger.error(f"Connection failed: {sys.exc_info()[0]}")

    except ConnectionRefusedError:
        client.loop_stop()
        logger.error(f"Connection refused: {sys.exc_info()[0]}")

    except KeyboardInterrupt:
        client.loop_stop()
        raise

    except Exception:
        client.loop_stop()
        logger.error(f"MQTT connection failed with exception: {sys.exc_info()[0]}")
        raise

    # Close the connection and return success
    if client.connected:
        client.on_disconnect = on_disconnect
        client.reconnect_delay_set(min_delay=1, max_delay=120)
        local_vars['mqtt_client'] = client
        atexit.register(mqtt_exit)
        return True

    # Some sort of error occurred
    return False


if __name__ == '__main__':
    test_msg = [{'Topic': 'test', 'Value': 'Test message'}]
    # Test connection and if successful publish a test message
    if start():
        publish(test_msg)
