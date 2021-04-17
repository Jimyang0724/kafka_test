import datetime
from flask import Flask, Response, render_template
from kafka import KafkaConsumer

# Fire up the Kafka Consumer
topic = "distributed-video1"

consumer = KafkaConsumer(
    topic, 
    bootstrap_servers=['localhost:9092'])


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
    for msg in consumer:
        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + msg.value + b'\r\n\r\n')

# @app.route('/audio_feed', methods=['GET'])
# def audio_feed():
#     return Response(generate(), mimetype="audio/mp3")
# def generate():
#     with open("yuru_camp_01.mp3", "rb") as fmp3:
#         data = fmp3.read(1024)
#         while data:
#             yield data
#             data = fmp3.read(1024)
    

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)