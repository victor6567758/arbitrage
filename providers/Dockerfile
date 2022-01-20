FROM python:3.8

WORKDIR /service
COPY . ./

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
CMD ["--version"]
