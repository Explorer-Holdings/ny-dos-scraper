FROM python:3.12-slim

WORKDIR /usr/src/app

RUN pip install --no-cache-dir --upgrade pip

COPY ../../../Downloads/files ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ../../../Downloads/files ./

CMD ["python", "main.py"]
