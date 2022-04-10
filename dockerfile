# FROM rust:1.59-alpine as builder
FROM rust:1.59 as builder

ENV RUST_BACKTRACE full
WORKDIR /build
ADD . /build
# RUN apk upgrade --update-cache --available && apk add openssl pkgconfig openssl-dev gcc musl-dev protobuf-dev protoc
# RUN rustup target add x86_64-unknown-linux-musl
# RUN cargo build --release --target x86_64-unknown-linux-musl
RUN cargo build --release


# FROM alpine:latest
# FROM rust:1.59-alpine
# FROM debian:buster-slim
FROM rust:1.59

WORKDIR /app
# RUN apk upgrade --update-cache --available && apk add openssl pkgconfig openssl-dev gcc musl-dev protobuf-dev protoc
# RUN apt-get update && apt-get install -y openssl

ENV RUST_BACKTRACE full
# COPY --from=builder /build/target/x86_64-unknown-linux-musl/release/sminer /app/sminer
COPY --from=builder /build/target/release/sminer /app/sminer
COPY --from=builder /build/config.yaml /app/config.yaml
# ENTRYPOINT ["/app/sminer"]
CMD ["/app/sminer", "consume", "-f", "/app/config.yaml"]