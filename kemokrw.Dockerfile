# Especificamos que la imagen se va a basar en la ultima version oficial disponible de Ubuntu
FROM ubuntu:bionic-20210723
 
# Identificamos quien se encarga de la gestion de la imagen
LABEL maintainer="felix@kemok.io"
 
RUN apt-get update && apt-get install -y \
        software-properties-common
    RUN add-apt-repository ppa:deadsnakes/ppa
    RUN apt-get install -y \
        python3.7 \
        python3-pip
    RUN python3 -m pip install pip
    RUN apt-get install -y python3.7-distutils \
        python3.7-dev libpq-dev \
        python3-setuptools \
	unixodbc-dev \
	python3-psycopg2 \
        curl  

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
RUN python3.7 get-pip.py --force-reinstall
RUN ln /usr/bin/python3.7 /usr/bin/python
workdir /usr/src/kemokrw
copy . .
RUN pip install -r requirements.txt

CMD ["bash"]

