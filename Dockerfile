FROM rust:1.64.0

RUN set -x \
 && apt update \
 && apt install -y \
      clang \
      cmake \
      libudev-dev \
      make \
      unzip \
      libssl-dev \
      pkg-config \
      zlib1g-dev \
      make \
 && rustup component add rustfmt \
 && rustup component add clippy \
 && rustc --version \
 && cargo --version

ENV PROTOC_VERSION 21.12
ENV PROTOC_ZIP protoc-$PROTOC_VERSION-linux-x86_64.zip

RUN curl -OL https://github.com/google/protobuf/releases/download/v$PROTOC_VERSION/$PROTOC_ZIP \
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
      ./cargo build --release && cp target/release/libgeyser* ./container-output; \
    else \
      ./cargo build --release --features "$features" && cp target/release/libgeyser* ./container-output; \
    fi
