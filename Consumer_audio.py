import datetime
from flask import Flask, Response, render_template
from kafka import KafkaConsumer

# Fire up the Kafka Consumer
TOPIC_V = "distributed-video1"
TOPIC_A = "distributed-audio1"

consumer_v = KafkaConsumer(
    TOPIC_V, 
    bootstrap_servers=['localhost:9092'])

consumer_a = KafkaConsumer(
    TOPIC_A,
    bootstrap_servers=['localhost:9092']
)

# Set the consumer in a Flask App
app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/video_feed', methods=['GET'])
def video_feed():
    return Response(
        get_video_stream(), 
        mimetype='multipart/x-mixed-replace; boundary=frame')
def get_video_stream():
    for msg in consumer_v:
        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + msg.value + b'\r\n\r\n')

# @app.route('/audio_feed', methods=['GET'])
# def audio_feed():
#     play(song)
#     return Response(generate(), mimetype="audio/mp3")
# def generate():
#     with open("time.wav", "rb") as fmp3:
#         data = fmp3.read()
#         while data:
#             yield data
#             data = fmp3.read()
    

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)