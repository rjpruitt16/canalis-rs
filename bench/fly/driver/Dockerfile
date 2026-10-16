FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod main.go ./
RUN go build -o /driver .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates curl
COPY --from=build /driver /driver
CMD ["/driver", "serve"]
