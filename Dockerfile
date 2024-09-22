FROM python:3.9

WORKDIR /app

RUN apt-get update && apt-get install -y netcat-openbsd

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["pytest"]
