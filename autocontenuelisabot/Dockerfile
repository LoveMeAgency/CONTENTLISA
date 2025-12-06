FROM python:3.11.3-alpine

RUN apk add --no-cache ca-certificates tzdata ffmpeg && update-ca-certificates

WORKDIR /app
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/database /app/session
COPY . /app/
ENV TZ=Europe/Malta

CMD ["python3", "bot.py"]
