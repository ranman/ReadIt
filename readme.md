# Building

Grab static ffmpeg from john van sickle site

Build with:

`docker build -t readit-build .`

`docker run -t -v $(pwd):/app readit-build ./build.sh readit.zip ffmpeg readit.py`


upload to lambda

and have a good time
