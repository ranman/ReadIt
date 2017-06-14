FROM amazonlinux

RUN yum install -y python27-pip zip

RUN pip install awscli virtualenv

RUN mkdir /app
WORKDIR /app

COPY . /app
