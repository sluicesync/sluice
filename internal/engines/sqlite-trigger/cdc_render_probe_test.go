// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// Unit matrix for the render-fidelity door ([verifyRealRenderHonoured]): the
// CDC-open probe that grades whether the CONNECTED SQLite honours the `!`
// alternate-form-2 precision flag the REAL capture expression rests on. The
// door must ACCEPT a lossless render, REFUSE the 16-digit clamp an
// `!`-ignoring engine produces (naming the premise), and REFUSE — fail
// closed — when the probe cannot run at all. Transport wiring is pinned
// elsewhere: TestD1CDC_LossyRenderProbeRefused (D1, end to end through
// openCDCReaderBackend) and every local openCDCReader test (real modernc).

// stubRenderProber scripts one probe answer.
type stubRenderProber struct {
	rendered string
	err      error
}

func (s stubRenderProber) realRenderProbe(context.Context) (string, error) {
	return s.rendered, s.err
}

func TestVerifyRealRenderHonoured(t *testing.T) {
	ctx := context.Background()

	t.Run("lossless_render_accepted", func(t *testing.T) {
		// The render modernc/D1 actually produce for the probe double.
		if err := verifyRealRenderHonoured(ctx, stubRenderProber{rendered: "0.300000000000000044"}); err != nil {
			t.Fatalf("lossless render must pass the door; got: %v", err)
		}
	})

	t.Run("sixteen_digit_clamp_refused", func(t *testing.T) {
		err := verifyRealRenderHonoured(ctx, stubRenderProber{rendered: "0.3"})
		if err == nil {
			t.Fatal("the 16-digit clamp (an engine ignoring the `!` flag) must refuse loudly")
		}
		if !strings.Contains(err.Error(), "alternate-form-2") || !strings.Contains(err.Error(), "LOSSILY") {
			t.Errorf("clamp refusal should name the `!` flag premise; got: %v", err)
		}
	})

	t.Run("probe_error_fails_closed", func(t *testing.T) {
		cause := errors.New("no such function: format")
		err := verifyRealRenderHonoured(ctx, stubRenderProber{err: cause})
		if err == nil {
			t.Fatal("a probe that cannot run must refuse (fail closed), not stream unverified")
		}
		if !errors.Is(err, cause) || !strings.Contains(err.Error(), "render fidelity") {
			t.Errorf("probe-error refusal should wrap the cause and name the probe; got: %v", err)
		}
	})
}
