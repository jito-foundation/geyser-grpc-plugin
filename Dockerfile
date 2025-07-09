FROM rust:1.84-slim-bullseye
# keep rust version in sync to avoid re-downloading rust
# use https://github.com/solana-labs/solana/blob/db9fdf5811ecd8a84ea446591854974d386681ef/ci/rust-version.sh#L21

RUN set -x \
    && apt-get -qq update \
    && apt-get -qq -y install \
    clang \
    cmake \
    libudev-dev \
    libssl-dev \
    pkg-config \
    zlib1g-dev \
    libelf-dev \
    unzip \
    curl \
    libudev-dev

ENV PROTOC_VERSION=23.4
ENV PROTOC_ZIP=protoc-$PROTOC_VERSION-linux-x86_64.zip

RUN curl -Ss -OL https://github.com/google/protobuf/releases/download/v$PROTOC_VERSION/$PROTOC_ZIP \
 && unzip -o $PROTOC_ZIP -d /usr/local bin/protoc \
 && unzip -o $PROTOC_ZIP -d /usr/local include/* \
 && rm -f $PROTOC_ZIP


WORKDIR /geyser-grpc-plugin
COPY . .
RUN mkdir -p container-output

ARG ci_commit
ENV CI_COMMIT=$ci_commit

ARG features

# Uses docker buildkit to cache the image.
# /usr/local/cargo/git needed for crossbeam patch
RUN --mount=type=cache,mode=0777,target=/geyser-grpc-plugin/target \
    --mount=type=cache,mode=0777,target=/usr/local/cargo/registry \
    --mount=type=cache,mode=0777,target=/usr/local/cargo/git \
    if [ -z "$features" ] ; then \
      cargo build --release && cp target/release/libgeyser* ./container-output; \
    else \
      cargo build --release --features "$features" && cp target/release/libgeyser* ./container-output; \
    fi
