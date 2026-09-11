//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GCS and Azure coverage for [blobcodec.BlobStore], against emulators.
//
// # Why this exists
//
// blobcodec is built on `gocloud.dev/blob` with four drivers registered
// (s3blob, gcsblob, azureblob, fileblob), and until this file the S3 driver
// was the only one anything booted — MinIO, four tests, one file. The
// abstraction genuinely removes the per-provider CODE risk: there is no
// "GCS path" through sluice, only a different URL scheme. What it cannot
// remove is provider BEHAVIOUR, and one behaviour is load-bearing:
//
//	PutIfAbsent (the ADR-0160 chain concurrent-writer guard) maps gocloud's
//	IfNotExist onto each backend's NATIVE precondition — S3 `If-None-Match: *`,
//	GCS generation-0, Azure `If-None-Match: *`, fileblob O_EXCL.
//
// That mapping is where a backup chain's protection against two concurrent
// writers lives, and on two of the four backends it was asserted in a comment
// and exercised by nothing. A provider that IGNORES the precondition behaves
// like a plain Put, and the guard's degrade path turns that into a WARN — so
// the failure mode is quiet by design, which is exactly the kind that needs a
// real server to catch.
//
// # What these tests prove, and what they do not
//
// They prove the CONTRACT on each driver: that a second create-only write is
// refused, that the refusal reaches sluice as `irbackup.ErrPathExists` through
// its own classifier rather than as some unmapped error, and that the stored
// bytes are the FIRST writer's. That last assertion is the independent
// expected value — an error alone would not prove the loser's payload was
// kept out.
//
// They do NOT prove that real GCS and real Azure agree with their emulators,
// and they cannot: credential resolution (ADC, managed identity), bucket
// policy, multi-region, and the real services' own conditional-write
// implementations are outside an emulator's reach. That remains an
// operator-run pass with real credentials. The emulator half is what can run
// on every PR for free.
//
// Deliberately NARROWER than the MinIO file, and that is the abstraction
// paying off: the full backup → restore round-trip is sluice's own code and is
// already covered there. Re-running it through GCS would re-test the same
// orchestrator against the same gocloud interface. Only the backend-specific
// layer is worth duplicating.
//
// # Measured, 2026-09-11, before this file was written
//
//	fake-gcs-server 1.56.1  second IfNotExist write -> 412 conditionNotMet
//	azurite 3.37.0          second IfNotExist write -> 409 BlobAlreadyExists
//
// Both surface as gocloud `FailedPrecondition`, and both left the first
// writer's bytes in place. The Azure code is worth knowing: 409, not 412.
// sluice's `isConditionalRequestConflict` retry path keys on S3's *smithy*
// `ConditionalRequestConflict` code, so Azure's 409 does not enter the retry
// arm — it falls through to `isPreconditionFailed`, whose gocloud-classified
// half catches it. Correct, and not obviously so from reading either side
// alone, which is the argument for pinning it here.

package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

const (
	// Pinned, for the reason the MinIO image's own comment gives: a floating
	// tag is how an external registry's roll becomes an unexplained CI failure
	// on an unrelated push. These are the versions the behaviour above was
	// measured on.
	fakeGCSImage  = "fsouza/fake-gcs-server:1.56.1"
	azuriteImage  = "mcr.microsoft.com/azure-storage/azurite:3.37.0"
	fakeGCSPort   = "4443/tcp"
	azuriteBlobPt = "10000/tcp"

	// The universally published Azurite development credentials. Not a
	// secret — they are in Microsoft's own documentation and are the only
	// account the emulator serves.
	azuriteAccount = "devstoreaccount1"
	azuriteKey     = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

// startFakeGCS boots fake-gcs-server, creates a bucket, and points gocloud's
// gcsblob at it for the test's duration.
//
// Worth knowing before copying this elsewhere: with STORAGE_EMULATOR_HOST set,
// gcsblob needs NO credentials at all — no dummy service-account JSON, no ADC.
// That was measured rather than assumed, and it is why this helper is as short
// as it is.
//
// Uses t.Setenv, which means NO t.Parallel in any test calling this: the
// emulator address is process-global state. The same hazard produced a data
// race in this package today by a different route (a global slog default), so
// it is named here rather than left to be rediscovered.
func startFakeGCS(t *testing.T) (bucket string) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// The host port must be known BEFORE the container starts, which is
	// unusual here and is the emulator's doing rather than a choice.
	// fake-gcs-server rewrites the URLs it hands back — notably the
	// media-download URL — using the `-public-host` / `-external-url` it was
	// booted with, and both are boot flags.
	//
	// Left to a random mapped port, the failure is precise and thoroughly
	// misleading (measured before this was fixed): Put succeeds, Exists
	// reports true, the conditional-write precondition fires correctly, and
	// only Get fails, with "object doesn't exist" — because the download is
	// the one path that goes through the rewritten host. A reader who saw
	// that would go looking at the key handling, which is fine.
	//
	// chaosFixedPortModifier reserves a FREE port and binds it, so this needs
	// no hardcoded number and cannot collide with whatever else the runner is
	// holding. Its name is from the chaos-restart suite it was written for;
	// the mechanism is general and this is its second caller.
	fixedPort, hostPort := chaosFixedPortModifier(t, fakeGCSPort)
	endpoint := "localhost:" + hostPort

	req := testcontainers.ContainerRequest{
		Image:              fakeGCSImage,
		ExposedPorts:       []string{fakeGCSPort},
		HostConfigModifier: fixedPort,
		Cmd: []string{
			"-scheme", "http",
			"-backend", "memory",
			"-public-host", endpoint,
			"-external-url", "http://" + endpoint,
		},
		WaitingFor: wait.ForListeningPort(fakeGCSPort).WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start fake-gcs-server: %v", err)
	}
	t.Cleanup(func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	})

	// Assert the binding actually took. If the daemon ever handed back a
	// different port, the client would still reach the server (via the mapped
	// port) while every URL the server rewrote pointed at the reserved one —
	// reproducing the exact download-only failure this binding exists to
	// avoid, but with nothing on screen to say why.
	port, err := container.MappedPort(ctx, fakeGCSPort)
	if err != nil {
		t.Fatalf("fake-gcs port: %v", err)
	}
	if port.Port() != hostPort {
		t.Fatalf("fake-gcs bound to %s but its rewritten URLs name %s — the download path would 404",
			port.Port(), hostPort)
	}

	bucket = "sluice-gcs-" + randomSuffix()
	if err := createFakeGCSBucket(ctx, "http://"+endpoint, bucket); err != nil {
		t.Fatalf("create fake-gcs bucket: %v", err)
	}

	// The Google storage client honours this; gocloud's gcsblob builds on it.
	t.Setenv("STORAGE_EMULATOR_HOST", endpoint)
	return bucket
}

// createFakeGCSBucket creates a bucket through fake-gcs-server's JSON API,
// which is unauthenticated — hence plain net/http rather than a cloud SDK.
func createFakeGCSBucket(ctx context.Context, base, name string) error {
	body := strings.NewReader(fmt.Sprintf(`{"name":%q}`, name))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/storage/v1/b?project=sluice-test", body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("create bucket: HTTP %d: %s", resp.StatusCode, msg)
	}
	return nil
}

// startAzurite boots Azurite's blob service, creates a container, and points
// gocloud's azureblob at it. gocloud has first-class emulator support via
// AZURE_STORAGE_IS_LOCAL_EMULATOR, so no URL surgery is needed.
//
// Same t.Setenv / no-t.Parallel constraint as startFakeGCS.
func startAzurite(t *testing.T) (container string) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        azuriteImage,
		ExposedPorts: []string{azuriteBlobPt},
		Cmd:          []string{"azurite-blob", "--blobHost", "0.0.0.0"},
		WaitingFor:   wait.ForListeningPort(azuriteBlobPt).WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start azurite: %v", err)
	}
	t.Cleanup(func() {
		shutdown, cf := context.WithTimeout(context.Background(), 30*time.Second)
		defer cf()
		_ = c.Terminate(shutdown)
	})

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("azurite host: %v", err)
	}
	port, err := c.MappedPort(ctx, azuriteBlobPt)
	if err != nil {
		t.Fatalf("azurite port: %v", err)
	}
	domain := fmt.Sprintf("%s:%s", host, port.Port())

	container = "sluice-az-" + randomSuffix()
	if err := createAzuriteContainer(ctx, domain, container); err != nil {
		t.Fatalf("create azurite container: %v", err)
	}

	t.Setenv("AZURE_STORAGE_ACCOUNT", azuriteAccount)
	t.Setenv("AZURE_STORAGE_KEY", azuriteKey)
	t.Setenv("AZURE_STORAGE_IS_LOCAL_EMULATOR", "true")
	t.Setenv("AZURE_STORAGE_PROTOCOL", "http")
	t.Setenv("AZURE_STORAGE_DOMAIN", domain)
	return container
}

// createAzuriteContainer creates the blob container. gocloud's azureblob
// opens an EXISTING container and has no create API, so this goes through the
// Azure SDK — which the binary already links transitively via that same
// driver, so it is not new surface, only a new import.
func createAzuriteContainer(ctx context.Context, domain, name string) error {
	cred, err := azblob.NewSharedKeyCredential(azuriteAccount, azuriteKey)
	if err != nil {
		return fmt.Errorf("shared key credential: %w", err)
	}
	svc := fmt.Sprintf("http://%s/%s", domain, azuriteAccount)
	client, err := azblob.NewClientWithSharedKeyCredential(svc, cred, nil)
	if err != nil {
		return fmt.Errorf("azblob client: %w", err)
	}
	if _, err := client.CreateContainer(ctx, name, nil); err != nil {
		return fmt.Errorf("create container %q: %w", name, err)
	}
	return nil
}

// TestBlobStore_FakeGCS_ContractAndChainGuard runs the backend-specific
// contract against gocloud's gcsblob driver.
func TestBlobStore_FakeGCS_ContractAndChainGuard(t *testing.T) {
	// No t.Parallel: startFakeGCS sets process-global env.
	bucket := startFakeGCS(t)
	blobStoreContract(t, "gs://"+bucket+"/chain-prefix")
}

// TestBlobStore_Azurite_ContractAndChainGuard runs the same contract against
// gocloud's azureblob driver.
func TestBlobStore_Azurite_ContractAndChainGuard(t *testing.T) {
	// No t.Parallel: startAzurite sets process-global env.
	container := startAzurite(t)
	blobStoreContract(t, "azblob://"+container+"/chain-prefix")
}

// blobStoreContract is the shared body. One function rather than two so the
// two drivers are held to the IDENTICAL assertions — a per-backend copy is
// how one of them quietly ends up checking less than the other.
func blobStoreContract(t *testing.T, url string) {
	t.Helper()
	ctx := context.Background()

	store, err := blobcodec.OpenBlobStore(ctx, url, blobcodec.BlobStoreOptions{})
	if err != nil {
		t.Fatalf("OpenBlobStore(%s): %v", url, err)
	}
	defer func() { _ = store.Close() }()

	t.Run("round-trip through the prefix", func(t *testing.T) {
		want := []byte("gcs/azure round-trip payload")
		if err := store.Put(ctx, "manifest.json", bytes.NewReader(want)); err != nil {
			t.Fatalf("Put: %v", err)
		}
		exists, err := store.Exists(ctx, "manifest.json")
		if err != nil {
			t.Fatalf("Exists: %v", err)
		}
		if !exists {
			t.Error("Exists after Put = false")
		}
		rc, err := store.Get(ctx, "manifest.json")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		got, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("round-trip mismatch: got %q want %q", got, want)
		}

		// List must report paths RELATIVE to the configured prefix — the
		// LocalStore contract every caller is written against. A driver whose
		// key handling leaked the prefix would break chain traversal without
		// breaking Put/Get.
		paths, err := store.List(ctx, "")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		found := false
		for _, p := range paths {
			if p == "manifest.json" {
				found = true
			}
			if strings.Contains(p, "chain-prefix") {
				t.Errorf("List leaked the configured prefix into %q — callers address paths relative to it", p)
			}
		}
		if !found {
			t.Errorf("List did not return the key just written; got %v", paths)
		}

		if err := store.Delete(ctx, "manifest.json"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		exists, err = store.Exists(ctx, "manifest.json")
		if err != nil {
			t.Fatalf("Exists after Delete: %v", err)
		}
		if exists {
			t.Error("Exists after Delete = true")
		}
	})

	t.Run("PutIfAbsent refuses the second writer and keeps the first's bytes", func(t *testing.T) {
		// This is the ADR-0160 chain concurrent-writer guard's whole premise
		// on this backend. A provider that ignored the precondition would
		// behave like a plain Put: no error, and the SECOND payload stored.
		const key = "chain-gen.json"
		first := []byte("first writer wins")
		if err := store.PutIfAbsent(ctx, key, bytes.NewReader(first)); err != nil {
			t.Fatalf("first PutIfAbsent must succeed on an absent key: %v", err)
		}

		err := store.PutIfAbsent(ctx, key, bytes.NewReader([]byte("second writer must lose")))
		if err == nil {
			t.Fatal("the second conditional write SUCCEEDED — this backend does not enforce the " +
				"create-only precondition the chain guard rides on, so two concurrent writers would " +
				"both believe they own the chain generation")
		}
		if !errors.Is(err, irbackup.ErrPathExists) {
			t.Fatalf("the second write failed, but not as ErrPathExists, so callers cannot tell a lost "+
				"race from a transport failure: %v", err)
		}

		// The independent expected value: an error alone does not prove the
		// loser's payload stayed out of the object.
		rc, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get after the refused write: %v", err)
		}
		got, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(got, first) {
			t.Errorf("the refused write still changed the object: got %q, want %q — the guard reported a "+
				"loss it did not actually prevent", got, first)
		}
	})
}
