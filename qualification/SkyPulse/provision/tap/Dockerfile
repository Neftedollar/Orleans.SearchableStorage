FROM golang:1.26.1-bookworm@sha256:ab3d6955bbc813a0f3fdf220c1d817dd89c0b3f283777db8ece4a32fe7858edd AS build

ENV GOTOOLCHAIN=local
WORKDIR /src/indigo

RUN test "$(go env GOVERSION)" = "go1.26.1" \
    && git init . \
    && git remote add origin https://github.com/bluesky-social/indigo.git \
    && git fetch --depth=1 origin 52c38ce3daca2e85a9f70cf052b475506463018e \
    && git checkout --detach FETCH_HEAD \
    && test "$(git rev-parse HEAD)" = "52c38ce3daca2e85a9f70cf052b475506463018e"

COPY indigo-52c38ce3-skypulse-metadata.patch /tmp/skypulse-tap.patch
COPY qualification-startup-hardening.patch /tmp/skypulse-tap-hardening.patch
COPY qualification-privacy-logging.patch /tmp/skypulse-tap-privacy.patch

RUN printf '%s  %s\n' 17575e48b5762616fe0e7c6fc56ebe23d442df3a4cf60d35d5377193b6a36056 /tmp/skypulse-tap.patch | sha256sum --check --strict \
    && printf '%s  %s\n' 2b8be0ceb8e2a71d15710199e545579a82a70ac2428d89bb88d1e74825c20101 /tmp/skypulse-tap-hardening.patch | sha256sum --check --strict \
    && printf '%s  %s\n' 63ff8131d6fe838f92464f4b133b798bbb29e902e954ee42cb1660af5cf9ceb0 /tmp/skypulse-tap-privacy.patch | sha256sum --check --strict \
    && git apply --check /tmp/skypulse-tap.patch \
    && git apply /tmp/skypulse-tap.patch \
    && git apply --check /tmp/skypulse-tap-hardening.patch \
    && git apply /tmp/skypulse-tap-hardening.patch \
    && git apply --check /tmp/skypulse-tap-privacy.patch \
    && git apply /tmp/skypulse-tap-privacy.patch \
    && go test ./cmd/tap \
    && go test ./atproto/atdata \
    && go vet ./cmd/tap \
    && CGO_ENABLED=1 go build \
        -trimpath \
        -buildvcs=false \
        -ldflags='-buildid=' \
        -o /out/tap \
        ./cmd/tap

FROM debian:bookworm-slim@sha256:abd67ffcfa541b485a3dff59865ab629aa048a6c613e639d36e7456b0b229241

LABEL org.opencontainers.image.title="SkyPulse metadata-only TAP overlay" \
      org.opencontainers.image.source="https://github.com/bluesky-social/indigo" \
      org.opencontainers.image.revision="52c38ce3daca2e85a9f70cf052b475506463018e"

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/tap /usr/local/bin/tap

USER 65534:65534
EXPOSE 2480

ENTRYPOINT ["/usr/local/bin/tap"]
CMD ["run"]
