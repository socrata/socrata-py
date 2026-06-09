# syntax=docker/dockerfile:1

FROM socrata/python-jammy:3.11

ENV DEBIAN_FRONTEND noninteractive
ARG PASSWORD
ENV TEST_PASSWORD=$PASSWORD
WORKDIR /app

COPY ./. /app/

RUN <<EOF bash
pip install --no-cache-dir -r requirements.txt
/app/bin/test-staging
EOF
