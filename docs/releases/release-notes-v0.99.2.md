# sluice v0.99.2

**sluice is now a one-liner to install.** This is a distribution release — Homebrew, Scoop, WinGet, and native Debian/RedHat packages are now published automatically on every release. No engine, API, or runtime changes from v0.99.1.

## Install

| Platform | Command |
|---|---|
| **macOS / Linux** (Homebrew) | `brew install sluicesync/tap/sluice` |
| **Windows** (Scoop) | `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice` |
| **Windows** (WinGet) | `winget install sluicesync.sluice` &nbsp;*(pending `microsoft/winget-pkgs` review)* |
| **Debian / Ubuntu** | download `sluice_0.99.2_linux_amd64.deb` → `sudo dpkg -i sluice_0.99.2_linux_amd64.deb` |
| **RHEL / Fedora** | download `sluice_0.99.2_linux_amd64.rpm` → `sudo rpm -i sluice_0.99.2_linux_amd64.rpm` |
| **Go** | `go install sluicesync.dev/sluice/cmd/sluice@v0.99.2` |
| **Container** | `ghcr.io/sluicesync/sluice:0.99.2` |

`.deb` / `.rpm` / `.apk` packages are attached below for both `amd64` and `arm64`.

## Also in this release

- **CI:** external fork PRs now pull the public pre-baked test images anonymously (no GHCR login when the token is absent), so contributor PRs pass the integration suite out of the box.

---

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
