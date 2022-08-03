FROM python:3.8

ENV PYTHONUNBUFFERED 1
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
COPY . .
ENTRYPOINT [ "python" ]
CMD [ "main.py"]