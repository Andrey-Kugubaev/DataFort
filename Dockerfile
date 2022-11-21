FROM python:3.9-slim
RUN mkdir /app
COPY . /app
COPY entrypoint /bin
WORKDIR /bin
RUN chmod +x entrypoint
WORKDIR /app
ENTRYPOINT entrypoint