FROM python:3.10-slim

RUN apt-get update && apt-get install --no-install-recommends -y bash nano mc

ENV PYTHONUNBUFFERED 1
# RUN apk update && apk add bash && apk add nano && apk add mc

RUN mkdir /src
WORKDIR /src
COPY . /src/
RUN pip install -r requirements.txt
CMD ["uvicorn", "api.app:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "8000"]

# build the image like this:
# docker build -t my-energy-connector:latest .

# log into the container
# docker exec -it my-energy-connector sh
