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

	for _, ttl := range []time.Duration{
		-time.Second, 0, 9 * time.Second, 10 * time.Second, 60 * time.Second,
		10*time.Second + 900*time.Microsecond,
	} {
		t.Run(ttl.String(), func(t *testing.T) {
			t.Parallel()

			err := config.Validate(&config.Config{
				Source: "mongodb://source", Target: "mongodb://target", HALeaseTTL: ttl,
			})
			if ttl != 0 && ttl < 10*time.Second {
				require.ErrorContains(t, err, "--ha-lease-ttl must be at least 10s")
			} else {
				require.NoError(t, err)
			}
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
