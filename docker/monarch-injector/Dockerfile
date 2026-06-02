FROM ubuntu:22.04

ARG TARGETARCH

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    socat \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /var/log/monarch

# Monarch binary — same artifact used by automation and OM for testing.
#
# The SHA256 of each tarball is pinned to prevent silent content drift at the
# upstream URL. Internal-downloads has in the past republished the same URL
# with a rebuilt binary (e.g. after the monarchApiPort → monarchApiEndpoint
# rename). Without a checksum check, Docker's layer cache will happily reuse
# the old image even when the upstream content has changed, causing hard-to-
# diagnose binding/flag mismatches downstream.
#
# To adopt a newer binary: update BOTH the commit hash in the URLs AND the
# SHA256 values below. A mismatched checksum fails the build with a clear
# diff, forcing an intentional audit of what changed.
ARG MONARCH_COMMIT=fbae5c69ee62e98efff3fbe7a569379c5a12823c
ARG MONARCH_AMD64_SHA256=684b969af518b1f1ff2fdfb94c1ddbe0b59cb20882b5c8b7e53d6eac479168b4
ARG MONARCH_ARM64_SHA256=6a7f3653ec3e0006690059f53c256a647034f6419b7901bd3ce870b9f12aabb4
RUN case "${TARGETARCH}" in \
        amd64) MONARCH_URL="https://internal-downloads.mongodb.com/monarch-dev/monarch_${MONARCH_COMMIT}/amazon2-x86/monarch.tgz"; \
               EXPECTED_SHA="${MONARCH_AMD64_SHA256}" ;; \
        arm64) MONARCH_URL="https://internal-downloads.mongodb.com/monarch-dev/monarch_${MONARCH_COMMIT}/amazon2023-arm64/monarch.tgz"; \
               EXPECTED_SHA="${MONARCH_ARM64_SHA256}" ;; \
        *) echo "Unsupported architecture: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    curl -fsSL "${MONARCH_URL}" -o /tmp/monarch.tgz && \
    echo "${EXPECTED_SHA}  /tmp/monarch.tgz" | sha256sum -c - && \
    tar -xz -C /usr/local/bin/ -f /tmp/monarch.tgz && \
    rm /tmp/monarch.tgz && \
    chmod +x /usr/local/bin/monarch

COPY entrypoint.sh /usr/local/bin/monarch-entrypoint.sh
RUN chmod +x /usr/local/bin/monarch-entrypoint.sh

EXPOSE 8080
EXPOSE 9995

ENTRYPOINT ["/usr/local/bin/monarch-entrypoint.sh"]
