from re import I
import paho.mqtt.client as mqtt 
import time 

def on_connect(client, userdata, flags, return_code):
    if return_code == 0:
        print("connected")
    else:
        print("could not connect, return code:", return_code)

def main():
    broker_hostname = "localhost"
    port = 1883 

    client = mqtt.Client("Client1")
    # client.username_pw_set(username="user_name", password="password") # uncomment if you use password auth
    client.on_connect = on_connect

    client.connect(broker_hostname, port)
    client.loop_start()

    topic = "Test"
    msg_count = 0

    try:
        while msg_count < 11:
            time.sleep(1)
            msg_count += 1
            result = client.publish(topic, msg_count)
            status = result[0]
            if status == 0:
                print("Message "+ str(msg_count) + " is published to topic " + topic)
            else:
                print("Failed to send message to topic " + topic)
                if not client.is_connected():
                    print("Client not connected, exiting...")
                    break
    finally:
        client.disconnect()
        client.loop_stop()

if __name__ == "__main__":
    main()