# Shopping Sessions: Rust pipeline.
# Build stage: compile the release binary. Scorer builds linux/amd64 natively;
# no FROM line pins a platform. Network is available during build for crates.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

# Runtime: just the static-ish glibc binary.
FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/shopping-sessions /app/pipeline
WORKDIR /app
CMD ["/app/pipeline"]
