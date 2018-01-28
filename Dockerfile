FROM golang:1.9-alpine

RUN mkdir /app/

# Copy executable binary.
COPY ./lsync.linux.amd64 /app/

EXPOSE 53547

USER root

# Start app.
CMD ["/app/lsync.linux.amd64"]


