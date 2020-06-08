FROM python:3.8

WORKDIR /app
COPY src/requirements.txt ./

RUN pip install -r requirements.txt

# Bundle app source
COPY src /app

ENV workers: auto

EXPOSE 8080
CMD [ "python", "sanic_server.py" ]