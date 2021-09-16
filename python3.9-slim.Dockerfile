# Especificamos que la imagen se va a basar en la ultima version oficial disponible de Ubuntu
FROM python:3.9-slim-buster

# set a directory for the app
WORKDIR /usr/src/kemokrw

# set environment variables
RUN apt-get install unixodbc-dev
RUN pip install --upgrade pip

# copy all the files to the container
COPY . .

# install dependencies
RUN pip3 install -r requirements.txt

