from flask import Flask, render_template, request
from google.cloud import pubsub_v1
import json
import logging
import configparser

config = configparser.ConfigParser()
config.read('cred/config.ini')
json_key_path = config.get('DEFAULT', 'JSON_KEY')

app = Flask(__name__)
publisher = pubsub_v1.PublisherClient.from_service_account_json(json_key_path)

@app.route('/index_A')
def index_A():
    return render_template('index_A.html')

@app.route('/index_B')
def index_B():
    return render_template('index_B.html')

@app.route('/publish', methods=['POST'])
def publish_to_pubsub():
    try:
        data = request.json  # Assuming JSON data is sent in the request
        topic_path = 'YOUR_TOPIC_PATH'

        # Convert data to JSON string
        message_data = json.dumps(data)
        # Publish message to Pub/Sub topic
        future = publisher.publish(topic_path, data=message_data.encode('utf-8'))
        message_id = future.result()  # Get message ID (optional)
        
        return 'Message published to Pub/Sub'
    except Exception as e:
        logging.error('Error publishing message to Pub/Sub: %s', e)

if __name__ == '__main__':
    app.run(debug=True)
