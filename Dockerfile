# syntax=docker/dockerfile:1

FROM socrata/python-jammy:3.11

ENV DEBIAN_FRONTEND noninteractive
ARG PASSWORD
ENV TEST_PASSWORD=$PASSWORD
WORKDIR /app

RUN <<EOF bash

EOF

COPY ./. /app/

RUN <<EOF bash
pwd
ls
pip install --no-cache-dir -r requirements.txt
/app/bin/test-staging
EOF

ENV PATH="/app/.venv/bin:$PATH"

COPY . .
