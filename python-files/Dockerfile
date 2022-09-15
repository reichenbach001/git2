FROM python:3.10

WORKDIR /app

COPY ./to_be_copied/requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY ./to_be_copied/ .


CMD ["sleep" ,"infinity"]

