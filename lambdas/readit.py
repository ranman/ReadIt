from __future__ import print_function
import hashlib
import os
import tempfile
import time

import boto3
from pydub import AudioSegment

from goose import Goose
g = Goose()

polly = boto3.client("polly")
s3 = boto3.client('s3')
ddb = boto3.resource("dynamodb").Table(os.getenv("TABLE_NAME", "readit"))
DEFAULT_VOICE = os.getenv("DEFAULT_VOICE", "Emma")
SAMPLE_RATE = os.getenv("SAMPLE_RATE", "8000")
BUCKET_NAME = os.getenv("BUCKET_NAME", "polly-readit")
MAX_CHARS = os.getenv("MAX_CHARS", 1500)
CACHE_TIME = os.getenv("CACHE_TIME", 3600)
AudioSegment.converter = os.path.join(os.getcwd(), "ffmpeg")


def build_composite_array(content, max_chars=MAX_CHARS):
    """Split text content into subarrays of len < max_chars"""
    # TODO: deal with unicode, deal with words > max_chars
    composite = ['']
    index = 0
    curr_len = 0
    for word in content.split(' '):
        if curr_len + len(word) > max_chars:
            index += 1
            curr_len = len(word) + 1
            composite.append(word + ' ')
        else:
            curr_len += len(word) + 1
            composite[index] += word + ' '
    return composite


def build_sound(content, voice=DEFAULT_VOICE, sample_rate=SAMPLE_RATE):
    composite = build_composite_array(content)
    sound = AudioSegment.empty()
    for text in composite:
        resp = polly.synthesize_speech(
            OutputFormat="mp3",
            SampleRate=sample_rate,
            Text=text,
            TextType="text",
            VoiceId=voice
        )
        pth = "/tmp/{}".format(resp['ResponseMetadata']['RequestId'])
        with open(pth, "wb") as f:
            f.write(resp['AudioStream'].read())
        time.sleep(1)
        sound += AudioSegment.from_mp3(pth)
    _, tmp_path = tempfile.mkstemp(dir="/tmp")
    sound.export(tmp_path, format="mp3")
    with open(tmp_path, "rb") as fp:
        return fp.read()


def generate_hash(content):
    hasher = hashlib.md5()
    hasher.update(content)
    return hasher.hexdigest()


def lambda_handler(event, content):
    url = event.get('url')
    voice = event.get('voice', DEFAULT_VOICE)
    if not url:
        raise ValueError("Bad Request: Missing parameter url")
    resp = ddb.get_item(Key={"url": url})
    if resp.get('Item') and all([
        resp['Item']['voice'] == voice,
        resp['Item']['ts'] + CACHE_TIME < time.time()
    ]):
        return resp
    article = g.extract(url=url)
    text_md5 = generate_hash(article.cleaned_text)
    if resp.get('Item') and all([
        resp['Item'].get('text_md5') == text_md5,
        resp['Item'].get('voice') == voice
    ]):
        return resp['Item']
    sound_data = build_sound(article.cleaned_text, voice=voice)
    sound_md5 = generate_hash(sound_data)
    fname = sound_md5 + '.mp3'
    s3.put_object(
        Bucket=BUCKET_NAME,
        ACL='public-read',
        Body=sound_data,
        Key=fname
    )
    item = {
        "url": url,
        "voice": voice,
        "text_md5": text_md5,
        "sound_md5": sound_md5,
        "ts": int(time.time()),
        "s3": "{}/{}/{}".format(s3.meta.endpoint_url, BUCKET_NAME, fname)
    }
    ddb.put_item(Item=item)
    return item
