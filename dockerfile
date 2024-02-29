FROM golang:1.21.4-alpine3.18
RUN mkdir /app
WORKDIR /app

# makes build faster. caches dependencies.
COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN go build -o main .
CMD ["/app/main"]