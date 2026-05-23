# sluice v0.34.0 — Phase 6.3: GCP Cloud KMS + Azure Key Vault

**Logical backups Phase 6.3 closes the v1 KMS shortlist.** Operators outside AWS can now use envelope encryption with native cloud KMS instead of falling back to the passphrase-mode escape hatch. Same `EnvelopeEncryption` interface that Phase 6.1 (passphrase) and Phase 6.2 (AWS KMS) shipped against — chunk writer/reader paths are unchanged; the only per-provider bits are the KEKMode tag in the manifest and the per-cloud wrap/unwrap RPCs.

## Added

- **GCP Cloud KMS envelope encryption** (`internal/crypto/gcp_kms.go`). New CLI flag `--gcp-kms-key-resource=projects/PROJECT/locations/LOCATION/keyRings/RING/cryptoKeys/KEY` (with optional `/cryptoKeyVersions/VERSION`). Manifest carries `KEKMode="gcp-kms"`. Wrap routes through Cloud KMS's `Encrypt` RPC; unwrap through `Decrypt`. Construction-time `GetCryptoKey` preflight surfaces auth / not-found errors before the backup starts. Auth via Application Default Credentials (`gcloud auth application-default login` or `GOOGLE_APPLICATION_CREDENTIALS`). Operator-actionable error translation for the five canonical gRPC codes (`NotFound`, `PermissionDenied`, `FailedPrecondition`, `InvalidArgument`, `Unauthenticated`) with role hints (`roles/cloudkms.cryptoKey{Encrypter,Decrypter,Viewer}`).

- **Azure Key Vault envelope encryption** (`internal/crypto/azure_kms.go`). New CLI flags `--azure-key-vault-id=https://VAULT.vault.azure.net/keys/KEY[/VERSION]` (also accepts `managedhsm.azure.net` for HSM-backed vaults) and `--azure-wrap-algorithm` (defaults to `RSA-OAEP-256`; pass `A256KW` for HSM-backed AES keys). Manifest carries `KEKMode="azure-kms"`. Wrap/unwrap use Key Vault's `WrapKey` / `UnwrapKey` RPCs — Key Vault's recommended pattern for symmetric-key wrapping. Auth via `DefaultAzureCredential` (env vars, managed identity, Azure CLI cached login). Operator-actionable error translation covers `KeyNotFound`, `Forbidden`/`AccessDenied`, `BadParameter`, `KeyDisabled`, plus HTTP status fallbacks (401/403/404) for errors that omit the SDK's `ErrorCode` field.

- **All-provider mutual exclusion**. The four key sources — passphrase, AWS KMS, GCP KMS, Azure Key Vault — are now pairwise mutually exclusive at flag-parse time. `validateKeySources` enforces this with a clear error message naming all four flag families. Test coverage in `TestEncryptionFlags_AllProvidersMutuallyExclusive` covers seven pair combinations plus the all-four-at-once case; `TestEncryptionFlags_GCPAzureWithoutEncrypt` mirrors the passphrase-without-encrypt / kms-without-encrypt sanity checks for the two new providers.

## Compatibility

- **Drop-in upgrade from v0.33.x.** Existing passphrase-mode and AWS KMS chains continue to restore unchanged. No on-disk format changes; the new `KEKMode` values (`gcp-kms`, `azure-kms`) are recognised additions, not breaking renames.
- **No new direct dependencies that add operator-visible binary-size cost.** Both `cloud.google.com/go/kms` and `github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys` were already in the module graph as indirect deps; this release promotes them to direct deps without changing the closure.
- **Operators outside AWS can now skip the passphrase escape hatch.** Pre-v0.34.0 the choice on GCP / Azure was either passphrase mode (operator-managed secrets) or bucket-level SSE (provider-managed). v0.34.0 closes the gap with native KMS support.

## Who needs this release

- **Operators on GCP whose compliance posture requires customer-managed key material:** **upgrade** — `--gcp-kms-key-resource` ships envelope encryption without the passphrase storage problem.
- **Operators on Azure with the same constraint:** **upgrade** — `--azure-key-vault-id` ships the equivalent path.
- **Operators on AWS** (Phase 6.2 path, v0.23.0): drop-in; no behaviour change.
- **Operators on the passphrase path** (Phase 6.1, v0.22.0): drop-in; no behaviour change. The expanded mutual-exclusion check error message now names all four flag families instead of two.
- **Operators not using encryption:** drop-in; no behaviour change.

## Verification surface

- **Unit tests with deterministic stubs** for both providers. GCP: 6 cases covering round-trip, empty-resource validation, preflight not-found, permission-denied error messages per op, unauthenticated GOOGLE_APPLICATION_CREDENTIALS hint, wrong-key InvalidArgument, plus a 4-case `parseCryptoKeyForResource` helper test. Azure: 9 cases covering round-trip, empty key-ID validation, preflight not-found, wrong-key BadParameter, Forbidden + Crypto User role hint, Describe Forbidden + Crypto Reader role hint, HTTP status-code fallback (401/403/404), non-ResponseError fallthrough, plus a 7-case `parseAzureKeyID` helper test.
- **CLI integration tests**: `TestEncryptionFlags_AllProvidersMutuallyExclusive` (7 pair combinations + all-four), `TestEncryptionFlags_GCPAzureWithoutEncrypt`, `TestEncryptionFlags_EncryptWithoutAnyKey` (extended to name all four flag families).
- All existing AWS KMS + passphrase tests remain green. `gofumpt`, `go vet`, `golangci-lint` all clean.

## Next in the Phase 6 track

Phase 6.4 (operator-facing key-management guide + ADR) remains queued. The shipped surface (4 providers × write/read paths × per-chain/per-chunk modes × CEK caching) is enough to use today; the docs chunk lands when operator-facing key-management questions surface in real deployments.
