import math
from pydub import AudioSegment

class SplitWavAudio():
    def __init__(self, file_path):
        self.audio = AudioSegment.from_wav(file_path)

    def get_duration(self):
        return self.audio.duration_seconds

    def single_split(self, from_millisec, to_millisec):
        t1 = from_millisec
        t2 = to_millisec
        return self.audio[t1:t2]

    def mul_split(self, millisec_per_split):
        audio_len = int(self.get_duration()*1000)
        segments = []
        for i in range(0, audio_len, millisec_per_split):
            segments.append(self.single_split(i, i+millisec_per_split))
            if audio_len - i <1:
                break
        return segments
