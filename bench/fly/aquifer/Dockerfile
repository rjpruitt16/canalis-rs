# Aquifer from GitHub (AQUIFER_REF), with a pacing ceiling high enough that
# the benchmark measures Canalis and Aquifer, not Aquifer's default
# 2 rps per target API.
FROM golang:1.25-alpine AS build
RUN apk add --no-cache git
ARG AQUIFER_REF=main
RUN git clone --depth 1 --branch ${AQUIFER_REF} https://github.com/rjpruitt16/aquifer.git /src
WORKDIR /src
RUN go build -o /aquifer ./cmd/aquifer

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
COPY --from=build /aquifer /aquifer
COPY bench.yml /bench.yml
ENV AQUIFER_ADAPTER=http CONFIG_PATH=/bench.yml DB_PATH=/tmp/aquifer.db
EXPOSE 8080
CMD ["/aquifer"]
