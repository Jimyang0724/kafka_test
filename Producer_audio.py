import sys
import time
import random
import wave
import cv2
import pyaudio
# import numpy as np
from kafka import KafkaProducer
from multiprocessing import Process
from threading import Thread

TOPIC_V = "distributed-video1"
TOPIC_A = "distributed-audio1"
VIDEO_FILE = "time.mp4"
AUDIO_FILE = "time.wav"
CHUNK = 1024

# producer = KafkaProducer(bootstrap_servers='localhost:9092')

def publish_video(video_file):
    # Start up producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    # Open file
    video = cv2.VideoCapture(video_file)

    print('publishing video...')

    frame_no = 0
    while(video.isOpened()):
        success, frame = video.read()
        if(frame_no % 3 == 0):
            frame = cv2.resize(frame, (1280, 720))

            # Ensure file was read successfully
            if not success:
                print("bad read!")
                break
            # Convert image to png
            ret, buffer = cv2.imencode('.jpg', frame)
            # Convert to bytes and send to kafka
            producer.send(TOPIC_V, buffer.tobytes())

        # fps = 48
        time.sleep(1/45)

    video.release()
    print('publish complete')

def publish_audio(audio_file):
    # Start up producer
    # producer = KafkaProducer(bootstrap_servers='localhost:9092')
    p = pyaudio.PyAudio()

    # Open file
    wf = wave.open(audio_file, 'rb')
    stream = p.open(format=p.get_format_from_width(wf.getsampwidth()),
        channels=wf.getnchannels(),
        rate=wf.getframerate(),
        output=True)
    
    stream.start_stream()
    print('publishing audio...')

    data = wf.readframes(CHUNK)
    while data != '':
        stream.write(data)
        # producer.send(TOPIC_A, data)
        data = wf.readframes(CHUNK)

    stream.stop_stream()
    stream.close()
    p.terminate()

def main():
    # synchronizer = Barrier(1)
    Process(target=publish_video, args=(VIDEO_FILE,)).start()
    Process(target=publish_audio, args=(AUDIO_FILE,)).start()
    # publish_video(VIDEO_FILE, AUDIO_FILE)

if __name__ == '__main__':
    main()
