import sys
import time
import random
import cv2
from kafka import KafkaProducer

TOPIC = "distributed-video1"
VIDEO_FILE = "time.mp4"
# AUDIO_FILE = "yuru_camp_01.mp3"


def publish_video(video_file):
    # Start up producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    # Open file
    video = cv2.VideoCapture(video_file)

    print('publishing video...')

    frame_no = 1
    while(video.isOpened()):
        success, frame = video.read()
        frame = cv2.resize(frame, (1280, 720))

        # Ensure file was read successfully
        if not success:
            print("bad read!")
            break
        
        # drop half frames
        # if frame_no % random.randint(1, 2) == 0:
            # Convert image to png
        ret, buffer = cv2.imencode('.jpg', frame)

        # Convert to bytes and send to kafka
        producer.send(TOPIC, buffer.tobytes())

        # fps = 25
        time.sleep(0.01)
        frame_no += 1

    video.release()
    print('publish complete')

def main():
    publish_video(VIDEO_FILE)

if __name__ == '__main__':
    main()