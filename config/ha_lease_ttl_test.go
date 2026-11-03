package config_test

import (
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/percona/percona-clustersync-mongodb/config"
)

func TestValidateHALeaseTTL(t *testing.T) {
	t.Parallel()

	const floorErr = "--ha-lease-ttl must be at least 10s"

	for _, tc := range []struct {
		name    string
		ttl     time.Duration
		wantErr string
	}{
		{name: "zero means default", ttl: 0},
		{name: "negative", ttl: -time.Second, wantErr: floorErr},
		{name: "below floor", ttl: 9 * time.Second, wantErr: floorErr},
		{name: "at floor", ttl: 10 * time.Second},
		{name: "above floor", ttl: 60 * time.Second},
		{name: "sub-millisecond remainder", ttl: 10*time.Second + 900*time.Microsecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := config.Validate(&config.Config{
				Source: "mongodb://source", Target: "mongodb://target", HALeaseTTL: tc.ttl,
			})
			if tc.wantErr == "" {
				require.NoError(t, err)

				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

//nolint:paralleltest // Viper and environment bindings are process-global.
func TestLoadHALeaseTTL(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  string
		args []string
		want time.Duration
	}{
		{name: "default", want: 10 * time.Second},
		{name: "environment", env: "60s", want: time.Minute},
		{name: "flag overrides environment", env: "60s", args: []string{"--ha-lease-ttl=20s"}, want: 20 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			t.Setenv("PCSM_HA_LEASE_TTL", tc.env)
			cmd := &cobra.Command{}
			cmd.Flags().Duration("ha-lease-ttl", config.LeaseTTL, "")
			require.NoError(t, cmd.ParseFlags(tc.args))

			var cfg config.Config
			require.NoError(t, config.Load(cmd, &cfg))
			require.Equal(t, tc.want, cfg.HALeaseTTL)
		})
	}
}
