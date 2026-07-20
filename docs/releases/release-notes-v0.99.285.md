# sluice v0.99.285

**Dependency and base-image maintenance — no functional change.** Routine updates to third-party Go dependencies and the runtime container's base image. sluice's own behavior is identical to v0.99.284; this release exists so the published binaries and the multi-arch GHCR runtime image carry the current dependency set and the latest base-OS security patches.

## Dependencies

- **Go modules:** `google.golang.org/grpc` → 1.82.0; the `aws-sdk-go-v2` group (`aws-sdk-go-v2` 1.42.1, `config` 1.32.29, `credentials` 1.19.28, `service/kms` 1.54.0, `service/s3` 1.105.0); `github.com/klauspost/compress` → 1.19.0; `golang.org/x/crypto` → 0.54.0; `golang.org/x/sync` → 0.22.0; `github.com/mattn/go-isatty` → 0.0.22.
- **Runtime image:** refreshed the `distroless/static-debian12` base image digest, so `ghcr.io/sluicesync/sluice` picks up the latest base-OS patches.

## Compatibility

**No behavior change.** All updates are patch/minor dependency bumps plus a base-image digest refresh; every code path is identical to v0.99.284. The full test + integration suite (including the Vitess VStream and filtered-move-OUT cluster gates, which exercise the bumped grpc transport) passed on this dependency set.

## Who needs this

Anyone who wants the current dependency set or the refreshed container base image — in particular, if you run the Docker image (`ghcr.io/sluicesync/sluice`) and want the latest base-OS security patches. If you're on v0.99.284 and don't use the container image, there's no functional reason to upgrade.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.285
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.285`
