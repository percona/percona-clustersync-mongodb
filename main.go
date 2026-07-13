package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/ha"
	"github.com/percona/percona-clustersync-mongodb/log"
	"github.com/percona/percona-clustersync-mongodb/mdb"
	"github.com/percona/percona-clustersync-mongodb/metrics"
	"github.com/percona/percona-clustersync-mongodb/pcsm"
	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
	"github.com/percona/percona-clustersync-mongodb/pcsm/repl"
	"github.com/percona/percona-clustersync-mongodb/util"
)

// Constants for server configuration.
const (
	ServerReadTimeout       = 30 * time.Second
	ServerReadHeaderTimeout = 3 * time.Second
	MaxRequestSize          = humanize.MiByte
	ServerResponseTimeout   = 5 * time.Second
)

var (
	Version   = "v0.9.0" //nolint:gochecknoglobals
	Platform  = ""       //nolint:gochecknoglobals
	GitCommit = ""       //nolint:gochecknoglobals
	GitBranch = ""       //nolint:gochecknoglobals
	BuildTime = ""       //nolint:gochecknoglobals
)

func buildVersion() string {
	return Version + " " + GitCommit + " " + BuildTime
}

func main() {
	rootCmd := newRootCmd()

	err := rootCmd.Execute()
	if err != nil {
		zerolog.Ctx(context.Background()).Fatal().Err(err).Msg("")
	}
}

func newRootCmd() *cobra.Command {
	cfg := &config.Config{}

	rootCmd := &cobra.Command{
		Use:   "pcsm",
		Short: "Percona ClusterSync for MongoDB replication tool",

		SilenceUsage: true,

		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			err := config.Load(cmd, cfg)
			if err != nil {
				return errors.Wrap(err, "load config")
			}

			logLevel, err := zerolog.ParseLevel(cfg.Log.Level)
			if err != nil {
				logLevel = zerolog.InfoLevel
			}

			logOutput := os.Stdout
			// subcommands log to stderr
			if cmd.HasParent() {
				logOutput = os.Stderr
			}

			lg := log.InitGlobals(logLevel, cfg.Log.JSON, cfg.Log.NoColor, logOutput)
			ctx := lg.WithContext(context.Background())
			cmd.SetContext(ctx)

			config.WarnDeprecatedEnvVars(ctx)

			return nil
		},

		RunE: func(cmd *cobra.Command, _ []string) error {
			err := config.Validate(cfg)
			if err != nil {
				return errors.Wrap(err, "validate config")
			}

			if cfg.ResetState {
				err := resetState(cmd.Context(), cfg)
				if err != nil {
					return err
				}

				log.New("cli").Info("State has been reset")
			}

			log.Ctx(cmd.Context()).Info("Percona ClusterSync for MongoDB " + buildVersion())

			return runServer(cfg)
		},
	}

	// Persistent flags (available to all subcommands)
	rootCmd.PersistentFlags().String("log-level", "info", "Log level")
	rootCmd.PersistentFlags().Bool("log-json", false, "Output log in JSON format")
	rootCmd.PersistentFlags().Bool("log-no-color", false, "Disable log color")

	rootCmd.PersistentFlags().Bool("no-color", false, "")
	rootCmd.PersistentFlags().MarkDeprecated("no-color", "use --log-no-color instead") //nolint:errcheck

	rootCmd.PersistentFlags().Int("port", config.DefaultServerPort, "Port number")

	// MongoDB client timeout (visible: commonly needed for debugging)
	rootCmd.PersistentFlags().String("mongodb-operation-timeout", config.DefaultMongoDBOperationTimeout.String(),
		"Timeout for MongoDB operations (e.g., 30s, 5m)")

	// Root command specific flags
	rootCmd.Flags().String("source", "", "MongoDB connection string for the source")
	rootCmd.Flags().String("target", "", "MongoDB connection string for the target")
	rootCmd.Flags().String("listen-host", "localhost", "Host to bind the HTTP server")

	rootCmd.Flags().String("group", config.DefaultGroup,
		"HA group name, recorded and advertised for observability "+
			"(cross-group isolation is not yet enforced)")

	rootCmd.Flags().StringSlice("source-client-compressors", nil,
		fmt.Sprintf("Compressors for the source MongoDB client (comma-separated: zstd,zlib,snappy; default: %s)",
			strings.Join(config.DefaultClientCompressors(), ",")))
	rootCmd.Flags().StringSlice("target-client-compressors", nil,
		fmt.Sprintf("Compressors for the target MongoDB client (comma-separated: zstd,zlib,snappy; default: %s)",
			strings.Join(config.DefaultClientCompressors(), ",")))

	rootCmd.Flags().Bool("start", false, "")
	rootCmd.Flags().MarkHidden("start") //nolint:errcheck

	rootCmd.Flags().Bool("reset-state", false, "")
	rootCmd.Flags().MarkHidden("reset-state") //nolint:errcheck

	rootCmd.Flags().Bool("pause-on-initial-sync", false, "")
	rootCmd.Flags().MarkHidden("pause-on-initial-sync") //nolint:errcheck

	rootCmd.AddCommand(
		newVersionCmd(),
		newStatusCmd(cfg),
		newStartCmd(cfg),
		newFinalizeCmd(cfg),
		newPauseCmd(cfg),
		newResumeCmd(cfg),
		newResetCmd(cfg),
	)

	return rootCmd
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version",
		Run: func(cmd *cobra.Command, _ []string) {
			info := fmt.Sprintf("Version:   %s\nPlatform:  %s\nGitCommit: "+
				"%s\nGitBranch: %s\nBuildTime: %s\nGoVersion: %s",
				Version,
				Platform,
				GitCommit,
				GitBranch,
				BuildTime,
				runtime.Version(),
			)

			cmd.Println(info)
		},
	}
}

func newStatusCmd(cfg *config.Config) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Get the status of the replication process",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return NewClient(cfg.Port).Status(cmd.Context())
		},
	}
}

func newStartCmd(cfg *config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start Cluster Replication",
		RunE: func(cmd *cobra.Command, _ []string) error {
			pauseOnInitialSync, _ := cmd.Flags().GetBool("pause-on-initial-sync")
			includeNamespaces, _ := cmd.Flags().GetStringSlice("include-namespaces")
			excludeNamespaces, _ := cmd.Flags().GetStringSlice("exclude-namespaces")

			startOptions := startRequest{
				PauseOnInitialSync: pauseOnInitialSync,
				IncludeNamespaces:  includeNamespaces,
				ExcludeNamespaces:  excludeNamespaces,
			}

			if cfg.Clone.NumParallelCollections != 0 {
				v := cfg.Clone.NumParallelCollections
				startOptions.CloneNumParallelCollections = &v
			}
			if cfg.Clone.NumReadWorkers != 0 {
				v := cfg.Clone.NumReadWorkers
				startOptions.CloneNumReadWorkers = &v
			}
			if cfg.Clone.NumInsertWorkers != 0 {
				v := cfg.Clone.NumInsertWorkers
				startOptions.CloneNumInsertWorkers = &v
			}
			if cfg.Clone.SegmentSize != "" {
				_, err := config.ParseAndValidateCloneSegmentSize(cfg.Clone.SegmentSize)
				if err != nil {
					return errors.Wrap(err, "invalid clone segment size")
				}
				v := cfg.Clone.SegmentSize
				startOptions.CloneSegmentSize = &v
			}
			if cfg.Clone.ReadBatchSize != "" {
				_, err := config.ParseAndValidateCloneReadBatchSize(cfg.Clone.ReadBatchSize)
				if err != nil {
					return errors.Wrap(err, "invalid clone read batch size")
				}
				v := cfg.Clone.ReadBatchSize
				startOptions.CloneReadBatchSize = &v
			}

			if cfg.Repl.NumWorkers != 0 {
				v := cfg.Repl.NumWorkers
				startOptions.ReplNumWorkers = &v
			}
			if cfg.Repl.ChangeStreamBatchSize != 0 {
				v := cfg.Repl.ChangeStreamBatchSize
				startOptions.ReplChangeStreamBatchSize = &v
			}
			if cfg.Repl.EventQueueSize != 0 {
				v := cfg.Repl.EventQueueSize
				startOptions.ReplEventQueueSize = &v
			}
			if cfg.Repl.WorkerQueueSize != 0 {
				v := cfg.Repl.WorkerQueueSize
				startOptions.ReplWorkerQueueSize = &v
			}
			if cfg.Repl.BulkOpsSize != 0 {
				v := cfg.Repl.BulkOpsSize
				startOptions.ReplBulkOpsSize = &v
			}
			if cfg.Repl.WorkerFlushInterval != 0 {
				v := cfg.Repl.WorkerFlushInterval.String()
				startOptions.ReplWorkerFlushInterval = &v
			}
			if cfg.Repl.WorkerBulkQueueSize != 0 {
				v := cfg.Repl.WorkerBulkQueueSize
				startOptions.ReplWorkerBulkQueueSize = &v
			}

			if cfg.UseCollectionBulkWrite {
				v := cfg.UseCollectionBulkWrite
				startOptions.UseCollectionBulkWrite = &v
			}

			return NewClient(cfg.Port).Start(cmd.Context(), startOptions)
		},
	}

	cmd.Flags().Bool("pause-on-initial-sync", false, "")
	cmd.Flags().MarkHidden("pause-on-initial-sync") //nolint:errcheck

	cmd.Flags().StringSlice("include-namespaces", nil,
		"Namespaces to include in the replication (e.g. db1.collection1,db2.collection2)")
	cmd.Flags().StringSlice("exclude-namespaces", nil,
		"Namespaces to exclude from the replication (e.g. db3.collection3,db4.*)")

	cmd.Flags().Int("clone-num-parallel-collections", 0,
		"Number of collections to clone in parallel (0 = auto)")
	cmd.Flags().Int("clone-num-read-workers", 0,
		"Number of read workers during clone (0 = auto)")
	cmd.Flags().Int("clone-num-insert-workers", 0,
		"Number of insert workers during clone (0 = auto)")
	cmd.Flags().String("clone-segment-size", "",
		"Segment size for clone operations (e.g. \"500MB\", \"1GiB\"). Empty = auto.")

	cmd.Flags().String("clone-read-batch-size", "", "")
	cmd.Flags().MarkHidden("clone-read-batch-size") //nolint:errcheck

	cmd.Flags().Int("repl-num-workers", 0,
		"Number of replication workers (0 = auto)")
	cmd.Flags().Int("repl-change-stream-batch-size", 0,
		fmt.Sprintf("Change stream batch size for replication (default: %d)",
			config.ChangeStreamBatchSize))
	cmd.Flags().Int("repl-event-queue-size", 0,
		fmt.Sprintf("Event queue size between change stream reader and dispatcher (default: %d)",
			config.ReplQueueSize))
	cmd.Flags().Int("repl-worker-queue-size", 0,
		fmt.Sprintf("Per-worker routed event queue size (default: %d)",
			config.ReplQueueSize))
	cmd.Flags().Int("repl-bulk-ops-size", 0,
		fmt.Sprintf("Maximum number of operations per bulk write (default: %d)",
			config.BulkOpsSize))
	cmd.Flags().String("repl-worker-flush-interval", "0s",
		fmt.Sprintf("Maximum interval between worker bulk write flushes (e.g., 1s, 500ms) (default: %s)",
			config.WorkerFlushInterval))
	cmd.Flags().Int("repl-worker-bulk-queue-size", 0,
		fmt.Sprintf("Number of pending bulks per worker for async writes (default: %d)",
			config.WorkerBulkQueueSize))

	cmd.Flags().Bool("use-collection-bulk-write", false,
		"Use collection-level bulk write instead of client bulk write")

	return cmd
}

func newFinalizeCmd(cfg *config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "finalize",
		Short: "Finalize Cluster Replication",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return NewClient(cfg.Port).Finalize(cmd.Context())
		},
	}

	return cmd
}

func newPauseCmd(cfg *config.Config) *cobra.Command {
	return &cobra.Command{
		Use:   "pause",
		Short: "Pause Cluster Replication",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return NewClient(cfg.Port).Pause(cmd.Context())
		},
	}
}

func newResumeCmd(cfg *config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resume",
		Short: "Resume Cluster Replication",
		RunE: func(cmd *cobra.Command, _ []string) error {
			fromFailure, _ := cmd.Flags().GetBool("from-failure")

			resumeOptions := resumeRequest{
				FromFailure: fromFailure,
			}

			return NewClient(cfg.Port).Resume(cmd.Context(), resumeOptions)
		},
	}

	cmd.Flags().Bool("from-failure", false, "Resume from failure")

	return cmd
}

func newResetCmd(cfg *config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset",
		Short: "Reset PCSM state (heartbeat and recovery data)",
		// Reset command has an override for the --target flag
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			err := cmd.Root().PersistentPreRunE(cmd, args)
			if err != nil {
				return errors.Wrap(err, "root pre-run")
			}

			if cfg.Target == "" {
				return errors.New("required flag --target not set")
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			err := resetState(cmd.Context(), cfg)
			if err != nil {
				return err
			}

			log.New("cli").Info("OK: reset all")

			return nil
		},
	}

	cmd.PersistentFlags().String("target", "", "MongoDB connection string for the target")

	cmd.AddCommand(
		newResetRecoveryCmd(cfg),
		newResetMembersCmd(cfg),
		newResetLeaseCmd(cfg),
	)

	return cmd
}

func newResetLeaseCmd(cfg *config.Config) *cobra.Command {
	return &cobra.Command{
		Use:    "lease",
		Hidden: true,
		Short:  "Reset HA lease state",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			target, err := mdb.Connect(ctx, cfg.Target, cfg)
			if err != nil {
				return errors.Wrap(err, "connect")
			}

			defer func() {
				err := util.CtxWithTimeout(ctx, config.DisconnectTimeout, target.Disconnect)
				if err != nil {
					log.Ctx(ctx).Warn("Disconnect: " + err.Error())
				}
			}()

			err = ha.DeleteLease(ctx, target)
			if err != nil {
				return errors.Wrap(err, "delete lease")
			}

			log.New("cli").Info("OK: reset lease")

			return nil
		},
	}
}

func newResetRecoveryCmd(cfg *config.Config) *cobra.Command {
	return &cobra.Command{
		Use:    "recovery",
		Hidden: true,
		Short:  "Reset recovery state",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			target, err := mdb.Connect(ctx, cfg.Target, cfg)
			if err != nil {
				return errors.Wrap(err, "connect")
			}

			defer func() {
				err := util.CtxWithTimeout(ctx, config.DisconnectTimeout, target.Disconnect)
				if err != nil {
					log.Ctx(ctx).Warn("Disconnect: " + err.Error())
				}
			}()

			err = DeleteRecoveryData(ctx, target)
			if err != nil {
				return err
			}

			log.New("cli").Info("OK: reset recovery")

			return nil
		},
	}
}

func newResetMembersCmd(cfg *config.Config) *cobra.Command {
	return &cobra.Command{
		Use:    "members",
		Hidden: true,
		Short:  "Reset HA member state",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			target, err := mdb.Connect(ctx, cfg.Target, cfg)
			if err != nil {
				return errors.Wrap(err, "connect")
			}

			defer func() {
				err := util.CtxWithTimeout(ctx, config.DisconnectTimeout, target.Disconnect)
				if err != nil {
					log.Ctx(ctx).Warn("Disconnect: " + err.Error())
				}
			}()

			err = ha.DeleteMembers(ctx, target)
			if err != nil {
				return errors.Wrap(err, "delete members")
			}

			log.New("cli").Info("OK: reset members")

			return nil
		},
	}
}

func resetState(ctx context.Context, cfg *config.Config) error {
	target, err := mdb.Connect(ctx, cfg.Target, cfg)
	if err != nil {
		return errors.Wrap(err, "connect")
	}

	defer func() {
		err := util.CtxWithTimeout(ctx, config.DisconnectTimeout, target.Disconnect)
		if err != nil {
			log.Ctx(ctx).Warn("Disconnect: " + err.Error())
		}
	}()

	err = ha.DeleteLease(ctx, target)
	if err != nil {
		return errors.Wrap(err, "delete lease")
	}

	err = ha.DeleteMembers(ctx, target)
	if err != nil {
		return errors.Wrap(err, "delete members")
	}

	err = dropLegacyHeartbeat(ctx, target)
	if err != nil {
		return errors.Wrap(err, "drop legacy heartbeat")
	}

	err = DeleteRecoveryData(ctx, target)
	if err != nil {
		return errors.Wrap(err, "delete recovery data")
	}

	return nil
}

// dropLegacyHeartbeat removes the pre-HA (<= 0.9.0) singleton heartbeat
// collection. Replication state from 0.9.0 is not compatible with 0.10.0;
// operators run `pcsm reset` before starting replication with 0.10.0.
func dropLegacyHeartbeat(ctx context.Context, target *mongo.Client) error {
	err := target.Database(config.PCSMDatabase).
		Collection(config.LegacyHeartbeatCollection).
		Drop(ctx)

	return errors.Wrap(err, "drop legacy heartbeat collection")
}

// runServer starts the HTTP server with the provided configuration.
func runServer(cfg *config.Config) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	srv, err := createServer(ctx, cfg)
	if err != nil {
		return errors.Wrap(err, "new server")
	}

	// Auto-start (--start) is deferred to promotion: replication may only begin
	// while ACTIVE. A single instance becomes ACTIVE immediately, so this fires
	// as soon as the lease is acquired; a STANDBY starts once promoted.
	if cfg.Start {
		startOpts, err := resolveStartOptions(cfg, startRequest{
			PauseOnInitialSync: cfg.PauseOnInitialSync,
		})
		if err != nil {
			return err
		}

		srv.setAutoStart(startOpts)
	}

	go func() {
		<-ctx.Done()

		cleanupCtx, cancel := context.WithTimeout(context.Background(), config.DisconnectTimeout)
		defer cancel()

		err := srv.Close(cleanupCtx)
		if err != nil {
			log.New("server").Error(err, "Close server")
		}

		os.Exit(0)
	}()

	port := cfg.Port
	if port == 0 {
		port = config.DefaultServerPort
	}

	host := cfg.ListenHost
	if host == "" {
		host = "localhost"
	}

	addr := net.JoinHostPort(host, strconv.Itoa(port))
	httpServer := http.Server{
		Addr:    addr,
		Handler: srv.Handler(),

		ReadTimeout:       ServerReadTimeout,
		ReadHeaderTimeout: ServerReadHeaderTimeout,
	}

	log.Ctx(ctx).Info("Starting HTTP server at http://" + addr)

	return httpServer.ListenAndServe() //nolint:wrapcheck
}

type server struct {
	// cfg holds the configuration.
	cfg *config.Config
	// sourceCluster is the MongoDB client for the source cluster.
	sourceCluster *mongo.Client
	// targetCluster is the MongoDB client for the target cluster.
	targetCluster *mongo.Client
	// pcsm is the PCSM instance for cluster replication.
	pcsm *pcsm.PCSM
	// membership maintains this instance's member document and runs the lease
	// loop through which this instance competes to be ACTIVE.
	membership *ha.Membership

	// promRegistry is the Prometheus registry for metrics.
	promRegistry *prometheus.Registry

	// mu guards cpCancel and autoStartOpts, the server-local lifecycle state
	// driven by lease role transitions. The authoritative (role, term) lives in
	// membership; read it via membership.CurrentRole().
	mu sync.Mutex
	// checkpointCancel stops the checkpointing loop started while ACTIVE. It is nil when
	// this instance is STANDBY (no checkpointing).
	checkpointCancel context.CancelFunc
	// autoStartOpts, when non-nil, holds the StartOptions to apply on promotion
	// to ACTIVE (the deferred effect of the --start flag).
	autoStartOpts *pcsm.StartOptions
}

// createServer creates a new server with the given options.
//
// Source and target connections are established eagerly here, at process
// startup, before the HA role is known. Every instance — including one that
// stays STANDBY for its whole life — therefore holds an idle source connection.
// This is intentional: connecting up front fail-fasts an unreachable source or
// an incompatible source/target version at boot rather than on the failover
// critical path, and keeps promotion latency low (no connect + topology
// discovery when a STANDBY is promoted). A STANDBY performs no reads on the
// source; the connection only carries the driver's own monitoring traffic.
func createServer(ctx context.Context, cfg *config.Config) (*server, error) {
	lg := log.Ctx(ctx)

	source, err := mdb.Connect(ctx, cfg.Source, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "connect to source cluster")
	}

	defer func() {
		if err == nil {
			return
		}

		err1 := util.CtxWithTimeout(ctx, config.DisconnectTimeout, source.Disconnect)
		if err1 != nil {
			log.Ctx(ctx).Warn("Disconnect Source Cluster: " + err1.Error())
		}
	}()

	sourceVersion, err := mdb.Version(ctx, source)
	if err != nil {
		return nil, errors.Wrap(err, "source version")
	}

	cs, _ := connstring.Parse(cfg.Source)
	lg.Infof("Connected to source cluster [%s]: %s://%s",
		sourceVersion.FullString(), cs.Scheme, strings.Join(cs.Hosts, ","))

	target, err := mdb.Connect(ctx, cfg.Target, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "connect to target cluster")
	}

	defer func() {
		if err == nil {
			return
		}

		err1 := util.CtxWithTimeout(ctx, config.DisconnectTimeout, target.Disconnect)
		if err1 != nil {
			log.Ctx(ctx).Warn("Disconnect Target Cluster: " + err1.Error())
		}
	}()

	targetVersion, err := mdb.Version(ctx, target)
	if err != nil {
		return nil, errors.Wrap(err, "target version")
	}

	cs, _ = connstring.Parse(cfg.Target)
	lg.Infof("Connected to target cluster [%s]: %s://%s",
		targetVersion.FullString(), cs.Scheme, strings.Join(cs.Hosts, ","))

	crossVersion, err := mdb.CheckVersionCompat(sourceVersion, targetVersion)
	if err != nil {
		return nil, errors.Wrap(err, "version check")
	}

	if crossVersion {
		lg.Infof("Cross-version replication: source %s → target %s", sourceVersion, targetVersion)
	}

	group := cfg.Group
	if group == "" {
		group = config.DefaultGroup
	}

	membership, err := ha.JoinMembership(ctx, target, ha.MembershipOptions{
		Group:       group,
		Port:        cfg.Port,
		PCSMVersion: buildVersion(),
	})
	if err != nil {
		return nil, errors.Wrap(err, "join membership")
	}

	promRegistry := prometheus.NewRegistry()
	metrics.Init(promRegistry)

	pcs := pcsm.New(ctx, source, target, sourceVersion)

	s := &server{
		cfg:           cfg,
		sourceCluster: source,
		targetCluster: target,
		pcsm:          pcs,
		membership:    membership,
		promRegistry:  promRegistry,
	}

	pcs.SetOnStateChanged(func(newState pcsm.State) {
		// State-change checkpoints are fenced by the current lease term. Only an
		// ACTIVE instance can persist; a STANDBY (term unchanged, lease held by
		// another) is rejected by the fence and that is expected.
		_, term := membership.CurrentRole()

		lg := log.New("http:checkpointing")

		err := DoCheckpoint(ctx, target, pcs, term, membership.InstanceID())
		switch {
		case err == nil:
			lg.Debugf("Checkpoint saved on %q", newState)
		case errors.Is(err, errCheckpointFenced):
			// A STANDBY (or a just-deposed active) is expected to be fenced on a
			// state-change checkpoint; this is normal, not an error.
			lg.Debug("Checkpoint fenced by a newer term")
		default:
			lg.Error(err, "checkpoint")
		}
	})

	// Settle the initial role synchronously, before the HTTP server serves any
	// request. A single instance becomes ACTIVE immediately; a contender that
	// loses stays STANDBY. This closes the window in which a freshly started
	// instance would still report the STANDBY default and spuriously reject
	// writes. The transition is buffered on the (coalescing) role-change channel
	// and consumed by watchRoleChanges below.
	s.membership.FirstLeaseTick(ctx)

	s.logInitialRole(ctx)

	go s.watchRoleChanges(ctx)

	go s.membership.RunLease(ctx)

	return s, nil
}

// logInitialRole logs the role this instance settled into at startup. A STANDBY
// also reports where the ACTIVE instance is, so an operator reading standby logs
// knows which node to talk to. This complements the transition logs in
// onPromote/onDemote, which do not fire for an instance that starts (and stays)
// STANDBY.
func (s *server) logInitialRole(ctx context.Context) {
	role, term := s.membership.CurrentRole()
	lg := log.New("ha:role").With(log.Int64("term", term))

	if role == ha.RoleActive {
		lg.Info("Instance role: ACTIVE")

		return
	}

	active := activeMemberAddr(s.buildEnvelope(ctx))
	if active == "" {
		active = "unknown"
	}

	lg.Info("Instance role: STANDBY; active is " + active)
}

// Close releases the lease (so a standby can take over promptly), leaves the
// membership set, and closes the server connections.
func (s *server) Close(ctx context.Context) error {
	err0 := s.membership.Release(ctx)
	err1 := s.membership.Stop(ctx)
	err2 := s.sourceCluster.Disconnect(ctx)
	err3 := s.targetCluster.Disconnect(ctx)

	return errors.Join(err0, err1, err2, err3)
}

// watchRoleChanges consumes lease role transitions and drives the replication
// lifecycle: promotion to ACTIVE recovers state and starts checkpointing;
// demotion to STANDBY stops checkpointing and halts the pipeline so a STANDBY
// performs no writes. It returns when ctx is canceled.
func (s *server) watchRoleChanges(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case rc := <-s.membership.RoleChanges():
			switch rc.Role {
			case ha.RoleActive:
				s.onPromote(ctx, rc.Term)
			case ha.RoleStandby:
				s.onDemote(ctx, rc.Term)
			}
		}
	}
}

// onPromote handles a STANDBY->ACTIVE transition. It re-reads any persisted
// checkpoint so this instance resumes from the latest committed state, then
// starts the checkpointing loop for this ACTIVE epoch.
func (s *server) onPromote(ctx context.Context, term int64) {
	lg := log.New("ha:role").With(log.Int64("term", term))
	lg.Info("Instance role: ACTIVE (promoted)")

	// Recover persisted state before doing anything else. If this fails, this
	// instance cannot safely act as ACTIVE (it would resume from unknown state),
	// so relinquish the lease and let another instance try. It keeps competing
	// (the lease loop is not stopped), so a transient error does not bench it
	// until restart. Term fencing still protects the target in the meantime.
	err := Restore(ctx, s.targetCluster, s.pcsm)
	if err != nil {
		lg.Error(err, "restore on promotion; relinquishing lease")

		rerr := s.membership.RelinquishLease(ctx)
		if rerr != nil {
			lg.Error(rerr, "relinquish lease after failed restore")
		}

		return
	}

	s.mu.Lock()
	// Start a checkpointing loop scoped to this ACTIVE epoch, fenced by term.
	// If a write is fenced by a newer term, this instance has been deposed:
	// onFenced halts the pipeline immediately rather than waiting for the lease
	// loop to notice on its next tick.
	if s.checkpointCancel == nil {
		cpCtx, cancel := context.WithCancel(ctx)
		s.checkpointCancel = cancel
		go RunCheckpointing(cpCtx, s.targetCluster, s.pcsm, term, s.membership.InstanceID(), func() {
			s.onDemote(ctx, term)
		})
	}
	autoStartOpts := s.autoStartOpts
	s.mu.Unlock()

	// Apply a deferred --start now that this instance is ACTIVE.
	if autoStartOpts != nil && s.pcsm.Status(ctx).State == pcsm.StateIdle {
		err := s.pcsm.Start(ctx, autoStartOpts)
		if err != nil {
			lg.Error(err, "auto-start on promotion")
		}
	}
}

// setAutoStart records StartOptions to apply when this instance becomes ACTIVE.
func (s *server) setAutoStart(opts *pcsm.StartOptions) {
	s.mu.Lock()
	s.autoStartOpts = opts
	s.mu.Unlock()
}

// onDemote handles an ACTIVE->STANDBY transition for a given term. It stops the
// checkpointing loop and pauses the pipeline so the demoted instance performs no
// further writes. Pause is best-effort: term fencing on checkpoint writes is the
// hard guarantee that a demoted active cannot corrupt the target.
//
// term identifies the ACTIVE epoch being demoted. onDemote can be invoked
// out-of-band by the checkpointing fence callback (onFenced), which may race a
// subsequent re-promotion into a newer term. If this instance has already moved
// past term, the demotion is stale and ignored so it does not tear down the
// newer epoch's checkpointing/pipeline.
func (s *server) onDemote(ctx context.Context, term int64) {
	lg := log.New("ha:role").With(log.Int64("term", term))

	_, currentTerm := s.membership.CurrentRole()
	if currentTerm > term {
		lg.With(log.Int64("currentTerm", currentTerm)).
			Debug("Ignoring stale demotion for an older term")

		return
	}

	lg.Info("Instance role: STANDBY (demoted)")

	s.mu.Lock()
	if s.checkpointCancel != nil {
		s.checkpointCancel()
		s.checkpointCancel = nil
	}
	s.mu.Unlock()

	err := s.pcsm.Pause(ctx)
	if err != nil {
		// Pause fails when the pipeline is not running (e.g. idle); that is a
		// benign no-op for a demotion, so only log at debug.
		lg.Debug("pause on demotion: " + err.Error())
	}
}

// Handler returns the HTTP handler for the server.
func (s *server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/status", s.HandleStatus)
	mux.HandleFunc("/start", s.HandleStart)
	mux.HandleFunc("/finalize", s.HandleFinalize)
	mux.HandleFunc("/pause", s.HandlePause)
	mux.HandleFunc("/resume", s.HandleResume)
	mux.Handle("/metrics", s.HandleMetrics())

	// pprof endpoints for profiling and debugging
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			log.New("http").Trace(r.Method + " " + r.URL.String())
		} else {
			log.New("http").Info(r.Method + " " + r.URL.String())
		}
		mux.ServeHTTP(w, r)
	})
}

// HandleStatus handles the /status endpoint.
func (s *server) HandleStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), ServerResponseTimeout)
	defer cancel()

	if r.Method != http.MethodGet {
		http.Error(w,
			http.StatusText(http.StatusMethodNotAllowed),
			http.StatusMethodNotAllowed)

		return
	}

	if r.ContentLength > MaxRequestSize {
		http.Error(w,
			http.StatusText(http.StatusRequestEntityTooLarge),
			http.StatusRequestEntityTooLarge)

		return
	}

	status := s.pcsm.Status(ctx)

	res := statusResponse{
		responseEnvelope: s.buildEnvelope(ctx),
		Ok:               status.Error == nil,
		State:            status.State,
	}

	err := status.Error
	if err != nil {
		res.Err = err.Error()
	}

	if status.State == pcsm.StateIdle {
		writeResponse(w, res)

		return
	}

	res.EventsRead = status.Repl.EventsRead
	res.EventsApplied = status.Repl.EventsApplied
	res.LagTimeSeconds = status.TotalLagTimeSeconds

	if !status.Repl.LastReplicatedOpTime.IsZero() {
		ts := fmt.Sprintf("%d.%d",
			status.Repl.LastReplicatedOpTime.T,
			status.Repl.LastReplicatedOpTime.I)

		isoDate := time.Unix(int64(status.Repl.LastReplicatedOpTime.T),
			int64(status.Repl.LastReplicatedOpTime.I)).UTC()

		res.LastReplicatedOpTime = &lastReplicatedOpTime{
			TS:      ts,
			ISODate: isoDate.Format(time.RFC3339),
		}
	}

	res.InitialSync = &statusInitialSyncResponse{
		Completed:      status.InitialSyncCompleted,
		LagTimeSeconds: status.InitialSyncLagTimeSeconds,

		CloneCompleted:          status.Clone.IsFinished(),
		EstimatedCloneSizeBytes: status.Clone.EstimatedTotalSizeBytes,
		ClonedSizeBytes:         status.Clone.CopiedSizeBytes,
	}

	res.Finalization = makeFinalizationResponse(status.FinalizeStatus)

	switch {
	case status.State == pcsm.StateRunning && !status.Clone.IsFinished():
		res.Info = "Initial Sync: Cloning Data"
	case status.State == pcsm.StateRunning && !status.InitialSyncCompleted:
		res.Info = "Initial Sync: Replicating Changes"
	case status.State == pcsm.StateRunning:
		res.Info = "Replicating Changes"
	case status.State == pcsm.StateFinalizing:
		res.Info = "Finalizing"
	case status.State == pcsm.StateFinalized:
		res.Info = "Finalized"
	case status.State == pcsm.StateFailed:
		res.Info = "Failed"
	}

	writeResponse(w, res)
}

// buildStartOptions builds StartOptions from config, validating clone size options.
func buildStartOptions(cfg *config.Config) (*pcsm.StartOptions, error) {
	startOpts := &pcsm.StartOptions{
		PauseOnInitialSync: cfg.PauseOnInitialSync,
		Repl: repl.Options{
			UseCollectionBulkWrite: cfg.UseCollectionBulkWrite,
			NumWorkers:             cfg.Repl.NumWorkers,
			ChangeStreamBatchSize:  cfg.Repl.ChangeStreamBatchSize,
			EventQueueSize:         cfg.Repl.EventQueueSize,
			WorkerQueueSize:        cfg.Repl.WorkerQueueSize,
			BulkOpsSize:            cfg.Repl.BulkOpsSize,
			WorkerFlushInterval:    cfg.Repl.WorkerFlushInterval,
			WorkerBulkQueueSize:    cfg.Repl.WorkerBulkQueueSize,
		},
		Clone: clone.Options{
			Parallelism:   cfg.Clone.NumParallelCollections,
			ReadWorkers:   cfg.Clone.NumReadWorkers,
			InsertWorkers: cfg.Clone.NumInsertWorkers,
		},
	}

	if cfg.Clone.SegmentSize != "" {
		segmentSize, err := config.ParseAndValidateCloneSegmentSize(cfg.Clone.SegmentSize)
		if err != nil {
			return nil, errors.Wrap(err, "invalid clone segment size")
		}
		startOpts.Clone.SegmentSizeBytes = segmentSize
	}

	if cfg.Clone.ReadBatchSize != "" {
		batchSize, err := config.ParseAndValidateCloneReadBatchSize(cfg.Clone.ReadBatchSize)
		if err != nil {
			return nil, errors.Wrap(err, "invalid clone read batch size")
		}
		startOpts.Clone.ReadBatchSizeBytes = batchSize
	}

	return startOpts, nil
}

// resolveStartOptions resolves the start options from the HTTP request and config.
// Clone tuning options use config (env var) as defaults, CLI/HTTP params override.
func resolveStartOptions(cfg *config.Config, params startRequest) (*pcsm.StartOptions, error) {
	// Start with config-based options
	options, err := buildStartOptions(cfg)
	if err != nil {
		return nil, err
	}

	// HTTP params override config values
	options.PauseOnInitialSync = params.PauseOnInitialSync
	options.IncludeNamespaces = params.IncludeNamespaces
	options.ExcludeNamespaces = params.ExcludeNamespaces

	if params.CloneNumParallelCollections != nil {
		options.Clone.Parallelism = *params.CloneNumParallelCollections
	}

	if params.CloneNumReadWorkers != nil {
		options.Clone.ReadWorkers = *params.CloneNumReadWorkers
	}

	if params.CloneNumInsertWorkers != nil {
		options.Clone.InsertWorkers = *params.CloneNumInsertWorkers
	}

	// HTTP params override config for size values (need to re-validate)
	if params.CloneSegmentSize != nil {
		segmentSize, err := config.ParseAndValidateCloneSegmentSize(*params.CloneSegmentSize)
		if err != nil {
			return nil, errors.Wrap(err, "invalid clone segment size")
		}
		options.Clone.SegmentSizeBytes = segmentSize
	}

	if params.CloneReadBatchSize != nil {
		batchSize, err := config.ParseAndValidateCloneReadBatchSize(*params.CloneReadBatchSize)
		if err != nil {
			return nil, errors.Wrap(err, "invalid clone read batch size")
		}
		options.Clone.ReadBatchSizeBytes = batchSize
	}

	if params.ReplNumWorkers != nil {
		options.Repl.NumWorkers = *params.ReplNumWorkers
	}

	if params.ReplChangeStreamBatchSize != nil {
		options.Repl.ChangeStreamBatchSize = *params.ReplChangeStreamBatchSize
	}

	if params.ReplEventQueueSize != nil {
		options.Repl.EventQueueSize = *params.ReplEventQueueSize
	}

	if params.ReplWorkerQueueSize != nil {
		options.Repl.WorkerQueueSize = *params.ReplWorkerQueueSize
	}

	if params.ReplBulkOpsSize != nil {
		options.Repl.BulkOpsSize = *params.ReplBulkOpsSize
	}

	if params.ReplWorkerFlushInterval != nil {
		d, err := time.ParseDuration(*params.ReplWorkerFlushInterval)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid replWorkerFlushInterval value: %s", *params.ReplWorkerFlushInterval)
		}
		options.Repl.WorkerFlushInterval = d
	}

	if params.ReplWorkerBulkQueueSize != nil {
		options.Repl.WorkerBulkQueueSize = *params.ReplWorkerBulkQueueSize
	}

	if params.UseCollectionBulkWrite != nil {
		options.Repl.UseCollectionBulkWrite = *params.UseCollectionBulkWrite
	}

	return options, nil
}

// HandleStart handles the /start endpoint.
func (s *server) HandleStart(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), ServerResponseTimeout)
	defer cancel()

	if r.Method != http.MethodPost {
		http.Error(w,
			http.StatusText(http.StatusMethodNotAllowed),
			http.StatusMethodNotAllowed)

		return
	}

	if r.ContentLength > MaxRequestSize {
		http.Error(w,
			http.StatusText(http.StatusRequestEntityTooLarge),
			http.StatusRequestEntityTooLarge)

		return
	}

	if !s.requireActive(ctx, w) {
		return
	}

	var params startRequest

	if r.ContentLength != 0 {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w,
				http.StatusText(http.StatusInternalServerError),
				http.StatusInternalServerError)

			return
		}

		err = json.Unmarshal(data, &params)
		if err != nil {
			http.Error(w,
				http.StatusText(http.StatusBadRequest),
				http.StatusBadRequest)

			return
		}
	}

	options, err := resolveStartOptions(s.cfg, params)
	if err != nil {
		writeResponse(w, startResponse{responseEnvelope: s.buildEnvelope(ctx), Err: err.Error()})

		return
	}

	err = s.pcsm.Start(ctx, options)
	if err != nil {
		writeResponse(w, startResponse{responseEnvelope: s.buildEnvelope(ctx), Err: err.Error()})

		return
	}

	writeResponse(w, startResponse{responseEnvelope: s.buildEnvelope(ctx), Ok: true})
}

// HandleFinalize handles the /finalize endpoint.
func (s *server) HandleFinalize(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), ServerResponseTimeout)
	defer cancel()

	if r.Method != http.MethodPost {
		http.Error(w,
			http.StatusText(http.StatusMethodNotAllowed),
			http.StatusMethodNotAllowed)

		return
	}

	if r.ContentLength > MaxRequestSize {
		http.Error(w,
			http.StatusText(http.StatusRequestEntityTooLarge),
			http.StatusRequestEntityTooLarge)

		return
	}

	if !s.requireActive(ctx, w) {
		return
	}

	err := s.pcsm.Finalize(ctx)
	if err != nil {
		writeResponse(w, finalizeResponse{responseEnvelope: s.buildEnvelope(ctx), Err: err.Error()})

		return
	}

	writeResponse(w, finalizeResponse{responseEnvelope: s.buildEnvelope(ctx), Ok: true})
}

// HandlePause handles the /pause endpoint.
func (s *server) HandlePause(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), ServerResponseTimeout)
	defer cancel()

	if r.Method != http.MethodPost {
		http.Error(w,
			http.StatusText(http.StatusMethodNotAllowed),
			http.StatusMethodNotAllowed)

		return
	}

	if r.ContentLength > MaxRequestSize {
		http.Error(w,
			http.StatusText(http.StatusRequestEntityTooLarge),
			http.StatusRequestEntityTooLarge)

		return
	}

	if !s.requireActive(ctx, w) {
		return
	}

	err := s.pcsm.Pause(ctx)
	if err != nil {
		writeResponse(w, pauseResponse{responseEnvelope: s.buildEnvelope(ctx), Err: err.Error()})

		return
	}

	writeResponse(w, pauseResponse{responseEnvelope: s.buildEnvelope(ctx), Ok: true})
}

// HandleResume handles the /resume endpoint.
func (s *server) HandleResume(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), ServerResponseTimeout)
	defer cancel()

	if r.Method != http.MethodPost {
		http.Error(w,
			http.StatusText(http.StatusMethodNotAllowed),
			http.StatusMethodNotAllowed)

		return
	}

	if r.ContentLength > MaxRequestSize {
		http.Error(w,
			http.StatusText(http.StatusRequestEntityTooLarge),
			http.StatusRequestEntityTooLarge)

		return
	}

	if !s.requireActive(ctx, w) {
		return
	}

	var params resumeRequest

	if r.ContentLength != 0 {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w,
				http.StatusText(http.StatusInternalServerError),
				http.StatusInternalServerError)

			return
		}

		err = json.Unmarshal(data, &params)
		if err != nil {
			http.Error(w,
				http.StatusText(http.StatusBadRequest),
				http.StatusBadRequest)

			return
		}
	}

	options := &pcsm.ResumeOptions{
		ResumeFromFailure: params.FromFailure,
	}

	err := s.pcsm.Resume(ctx, *options)
	if err != nil {
		writeResponse(w, resumeResponse{responseEnvelope: s.buildEnvelope(ctx), Err: err.Error()})

		return
	}

	writeResponse(w, resumeResponse{responseEnvelope: s.buildEnvelope(ctx), Ok: true})
}

func (s *server) HandleMetrics() http.Handler {
	return promhttp.HandlerFor(s.promRegistry, promhttp.HandlerOpts{})
}

// writeResponse writes the response as JSON to the ResponseWriter.
func writeResponse[T any](w http.ResponseWriter, resp T) {
	err := json.NewEncoder(w).Encode(resp)
	if err != nil {
		http.Error(w,
			http.StatusText(http.StatusInternalServerError),
			http.StatusInternalServerError)
	}
}

// meInfo identifies the instance that produced a response.
type meInfo struct {
	InstanceID string `json:"instanceId"`
}

// groupMember is one instance in the HA group, as advertised in the members
// collection and surfaced in every API response.
type groupMember struct {
	InstanceID string  `json:"instanceId"`
	Host       string  `json:"host,omitempty"`
	Port       int     `json:"port,omitempty"`
	Role       ha.Role `json:"role"`
}

// groupInfo describes the HA group this instance belongs to.
type groupInfo struct {
	Name    string        `json:"name,omitempty"`
	Term    int64         `json:"term"`
	Members []groupMember `json:"members"`
}

// responseEnvelope is embedded in every API response (success and error). It advertises
// the responding instance's identity and role plus the HA group view, so an
// operator hitting any node can see who is ACTIVE and where.
type responseEnvelope struct {
	Me    meInfo    `json:"me"`
	Role  ha.Role   `json:"role"`
	Group groupInfo `json:"group"`
}

// buildEnvelope assembles the response envelope from membership state and the
// live member list. A failure reading members degrades to an empty member list
// rather than failing the request; the caller's own payload still returns.
func (s *server) buildEnvelope(ctx context.Context) responseEnvelope {
	role, term := s.membership.CurrentRole()

	env := responseEnvelope{
		Me:   meInfo{InstanceID: s.membership.InstanceID()},
		Role: role,
		Group: groupInfo{
			Name: s.membership.Group(),
			Term: term,
		},
	}

	members, err := ha.Members(ctx, s.targetCluster)
	if err != nil {
		log.New("http:envelope").Warn("list members: " + err.Error())

		return env
	}

	env.Group.Members = make([]groupMember, 0, len(members))
	for _, mem := range members {
		env.Group.Members = append(env.Group.Members, groupMember{
			InstanceID: mem.InstanceID,
			Host:       mem.Host,
			Port:       mem.Port,
			Role:       mem.Role,
		})
	}

	return env
}

// activeMemberAddr returns the "host:port" of the ACTIVE member from the member
// list, or "" if none is currently known. Used to point a rejected client at
// the instance that can serve its write.
func activeMemberAddr(env responseEnvelope) string {
	for _, mem := range env.Group.Members {
		if mem.Role == ha.RoleActive {
			return fmt.Sprintf("%s:%d", mem.Host, mem.Port)
		}
	}

	return ""
}

// notActiveResponse is the HTTP 409 body returned when a write command is sent
// to a STANDBY instance. It carries the full envelope so the caller can locate
// the ACTIVE instance.
type notActiveResponse struct {
	responseEnvelope

	Ok      bool   `json:"ok"`
	Err     string `json:"error"`
	Message string `json:"message"`
}

// requireActive rejects write commands on a non-ACTIVE instance with HTTP 409.
// It returns true when the caller may proceed (this instance is ACTIVE) and
// false when it has already written the 409 response.
func (s *server) requireActive(ctx context.Context, w http.ResponseWriter) bool {
	role, _ := s.membership.CurrentRole()
	if role == ha.RoleActive {
		return true
	}

	env := s.buildEnvelope(ctx)

	msg := "This instance is " + string(role) + "."
	if addr := activeMemberAddr(env); addr != "" {
		msg += " Active is running on " + addr + "."
	} else {
		msg += " No active instance is currently known."
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusConflict)

	err := json.NewEncoder(w).Encode(notActiveResponse{
		responseEnvelope: env,
		Ok:               false,
		Err:              "not_active",
		Message:          msg,
	})
	if err != nil {
		http.Error(w,
			http.StatusText(http.StatusInternalServerError),
			http.StatusInternalServerError)
	}

	return false
}

// startRequest represents the request body for the /start endpoint.
type startRequest struct {
	// PauseOnInitialSync indicates whether to pause after the initial sync.
	PauseOnInitialSync bool `json:"pauseOnInitialSync,omitempty"`

	// IncludeNamespaces are the namespaces to include in the replication.
	IncludeNamespaces []string `json:"includeNamespaces,omitempty"`
	// ExcludeNamespaces are the namespaces to exclude from the replication.
	ExcludeNamespaces []string `json:"excludeNamespaces,omitempty"`

	// Clone tuning options (pointer types to distinguish "not set" from zero value)
	// CloneNumParallelCollections is the number of collections to clone in parallel.
	CloneNumParallelCollections *int `json:"cloneNumParallelCollections,omitempty"`
	// CloneNumReadWorkers is the number of read workers during clone.
	CloneNumReadWorkers *int `json:"cloneNumReadWorkers,omitempty"`
	// CloneNumInsertWorkers is the number of insert workers during clone.
	CloneNumInsertWorkers *int `json:"cloneNumInsertWorkers,omitempty"`
	// CloneSegmentSize is the segment size for clone operations (e.g., "100MB", "1GiB").
	CloneSegmentSize *string `json:"cloneSegmentSize,omitempty"`
	// CloneReadBatchSize is the read batch size during clone (e.g., "16MiB").
	CloneReadBatchSize *string `json:"cloneReadBatchSize,omitempty"`

	// ReplNumWorkers is the number of replication workers.
	ReplNumWorkers *int `json:"replNumWorkers,omitempty"`
	// ReplChangeStreamBatchSize is the change stream batch size for replication.
	ReplChangeStreamBatchSize *int `json:"replChangeStreamBatchSize,omitempty"`
	// ReplEventQueueSize is the event queue size between change stream reader and dispatcher.
	ReplEventQueueSize *int `json:"replEventQueueSize,omitempty"`
	// ReplWorkerQueueSize is the per-worker routed event queue size.
	ReplWorkerQueueSize *int `json:"replWorkerQueueSize,omitempty"`
	// ReplBulkOpsSize is the maximum number of operations per bulk write.
	ReplBulkOpsSize *int `json:"replBulkOpsSize,omitempty"`
	// ReplWorkerFlushInterval is the maximum interval between worker bulk write flushes (e.g., "1s", "500ms").
	ReplWorkerFlushInterval *string `json:"replWorkerFlushInterval,omitempty"`
	// ReplWorkerBulkQueueSize is the number of pending bulks per worker for async writes.
	ReplWorkerBulkQueueSize *int `json:"replWorkerBulkQueueSize,omitempty"`

	// UseCollectionBulkWrite indicates whether to use collection-level bulk write
	// instead of client bulk write.
	UseCollectionBulkWrite *bool `json:"useCollectionBulkWrite,omitempty"`
}

// clientResponse is implemented by all API response types to allow
// doClientRequest to extract errors uniformly.
type clientResponse interface {
	IsOk() bool
	GetError() string
}

// startResponse represents the response body for the /start endpoint.
type startResponse struct {
	responseEnvelope

	// Ok indicates if the operation was successful.
	Ok bool `json:"ok"`
	// Err is the error message if the operation failed.
	Err string `json:"error,omitempty"`
}

func (r startResponse) IsOk() bool       { return r.Ok }
func (r startResponse) GetError() string { return r.Err }

// finalizeResponse represents the response body for the /finalize endpoint.
type finalizeResponse struct {
	responseEnvelope

	// Ok indicates if the operation was successful.
	Ok bool `json:"ok"`
	// Err is the error message if the operation failed.
	Err string `json:"error,omitempty"`
}

func (r finalizeResponse) IsOk() bool       { return r.Ok }
func (r finalizeResponse) GetError() string { return r.Err }

// statusResponse represents the response body for the /status endpoint.
type statusResponse struct {
	responseEnvelope

	// PauseOnInitialSync indicates if the replication is paused on initial sync.
	PauseOnInitialSync bool `json:"pauseOnInitialSync,omitempty"`

	// Ok indicates if the operation was successful.
	Ok bool `json:"ok"`
	// Err is the error message if the operation failed.
	Err string `json:"error,omitempty"`

	// State is the current state of the replication.
	State pcsm.State `json:"state"`
	// Info provides additional information about the current state.
	Info string `json:"info,omitempty"`

	// LagTimeSeconds is the current lag time in logical seconds.
	LagTimeSeconds int64 `json:"lagTimeSeconds"`
	// EventsRead is the number of events read from the source. Not counting tick events.
	EventsRead int64 `json:"eventsRead"`
	// EventsApplied is the number of events applied.
	EventsApplied int64 `json:"eventsApplied"`
	// LastReplicatedOpTime is the last replicated operation time.
	LastReplicatedOpTime *lastReplicatedOpTime `json:"lastReplicatedOpTime,omitempty"`

	// InitialSync contains the initial sync status details.
	InitialSync *statusInitialSyncResponse `json:"initialSync,omitempty"`

	// Finalization contains the finalize stage status details.
	Finalization *statusFinalizationResponse `json:"finalization,omitempty"`
}

func (r statusResponse) IsOk() bool       { return r.Ok }
func (r statusResponse) GetError() string { return r.Err }

type lastReplicatedOpTime struct {
	TS      string `json:"ts"`
	ISODate string `json:"isoDate"`
}

// statusInitialSyncResponse represents the initial sync status in the /status response.
type statusInitialSyncResponse struct {
	// LagTimeSeconds is the lag time in logical seconds until the initial sync completed.
	LagTimeSeconds int64 `json:"lagTimeSeconds,omitempty"`

	// EstimatedCloneSizeBytes is the estimated total size of the clone in bytes.
	EstimatedCloneSizeBytes uint64 `json:"estimatedCloneSizeBytes,omitempty"`
	// ClonedSizeBytes is the size of the data that has been cloned.
	ClonedSizeBytes uint64 `json:"clonedSizeBytes"`

	// Completed indicates if the initial sync is completed.
	Completed bool `json:"completed"`
	// CloneCompleted indicates if the cloning process is completed.
	CloneCompleted bool `json:"cloneCompleted"`
}

// statusFinalizationResponse represents the finalize-stage status in the /status response.
type statusFinalizationResponse struct {
	// Completed indicates whether the finalize stage has finished successfully.
	Completed bool `json:"completed"`
	// StartedAt is when the finalize stage was triggered. Omitted when the
	// finalization status was restored from a recovered checkpoint, since
	// the trigger timestamp is not persisted.
	StartedAt *time.Time `json:"startedAt,omitempty"`
	// CompletedAt is when the finalize stage finished. Omitted while in progress.
	CompletedAt *time.Time `json:"completedAt,omitempty"`

	// UnsuccessfulIndexes lists indexes that did not complete cleanly during
	// replication and were not recovered during finalize. Empty until the
	// finalize stage completes.
	UnsuccessfulIndexes []unsuccessfulIndexResponse `json:"unsuccessfulIndexes,omitempty"`
}

// unsuccessfulIndexResponse describes a single index that did not finalize cleanly.
type unsuccessfulIndexResponse struct {
	// Namespace is the database and collection (db.coll) of the index.
	Namespace string `json:"namespace"`
	// IndexName is the name of the index.
	IndexName string `json:"indexName"`
	// Type categorizes why the index is unsuccessful: "failed", "incomplete", or "inconsistent".
	Type string `json:"type"`
	// Keys is the index key spec, encoded as a JSON object preserving field order.
	Keys json.RawMessage `json:"keys,omitempty"`
	// Reason is a human-readable explanation of why the index is unsuccessful.
	Reason string `json:"reason"`
}

// makeFinalizationResponse translates a [pcsm.FinalizeStatus] into the wire format.
// Returns nil when fs is nil (no finalize has been triggered yet).
func makeFinalizationResponse(fs *pcsm.FinalizeStatus) *statusFinalizationResponse {
	if fs == nil {
		return nil
	}

	out := &statusFinalizationResponse{
		Completed: fs.Completed,
	}

	if !fs.StartedAt.IsZero() {
		t := fs.StartedAt
		out.StartedAt = &t
	}

	if !fs.CompletedAt.IsZero() {
		t := fs.CompletedAt
		out.CompletedAt = &t
	}

	if len(fs.UnsuccessfulIndexes) > 0 {
		out.UnsuccessfulIndexes = make([]unsuccessfulIndexResponse, 0, len(fs.UnsuccessfulIndexes))

		for _, idx := range fs.UnsuccessfulIndexes {
			out.UnsuccessfulIndexes = append(out.UnsuccessfulIndexes, unsuccessfulIndexResponse{
				Namespace: idx.Namespace,
				IndexName: idx.Name,
				Type:      string(idx.Type),
				Keys:      indexKeysToJSON(idx.Keys),
				Reason:    idx.Reason,
			})
		}
	}

	return out
}

// indexKeysToJSON converts a bson.Raw index keys document into a JSON object,
// preserving field order for compound indexes.
//
// On any decoding or marshaling error the function returns nil so the field is
// omitted from the response via omitempty. A malformed key spec is not worth
// failing the entire /status response over: operators still get the namespace,
// index name and type, which are enough to identify the offending index.
func indexKeysToJSON(raw bson.Raw) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}

	var doc bson.D

	err := bson.Unmarshal(raw, &doc)
	if err != nil {
		return nil
	}

	var buf bytes.Buffer

	buf.WriteByte('{')

	for i, e := range doc {
		if i > 0 {
			buf.WriteByte(',')
		}

		k, err := json.Marshal(e.Key)
		if err != nil {
			return nil
		}

		buf.Write(k)
		buf.WriteByte(':')

		v, err := json.Marshal(e.Value)
		if err != nil {
			return nil
		}

		buf.Write(v)
	}

	buf.WriteByte('}')

	return buf.Bytes()
}

// pauseResponse represents the response body for the /pause endpoint.
type pauseResponse struct {
	responseEnvelope

	// Ok indicates if the operation was successful.
	Ok bool `json:"ok"`
	// Err is the error message if the operation failed.
	Err string `json:"error,omitempty"`
}

func (r pauseResponse) IsOk() bool       { return r.Ok }
func (r pauseResponse) GetError() string { return r.Err }

// resumeRequest represents the request body for the /resume endpoint.
type resumeRequest struct {
	// FromFailure indicates whether to resume from a failed state.
	FromFailure bool `json:"fromFailure,omitempty"`
}

// resumeResponse represents the response body for the /resume
// endpoint.
type resumeResponse struct {
	responseEnvelope

	// Ok indicates if the operation was successful.
	Ok bool `json:"ok"`
	// Err is the error message if the operation failed.
	Err string `json:"error,omitempty"`
}

func (r resumeResponse) IsOk() bool       { return r.Ok }
func (r resumeResponse) GetError() string { return r.Err }

type PCSMClient struct {
	port int
}

func NewClient(port int) PCSMClient {
	return PCSMClient{port: port}
}

// Status sends a request to get the status of the cluster replication.
// It always prints the full JSON response.
func (c PCSMClient) Status(ctx context.Context) error {
	return doStatusRequest(ctx, c.port)
}

// Start sends a request to start the cluster replication.
func (c PCSMClient) Start(ctx context.Context, req startRequest) error {
	return doClientRequest[startResponse](ctx, c.port, http.MethodPost, "start", req)
}

// Finalize sends a request to finalize the cluster replication.
func (c PCSMClient) Finalize(ctx context.Context) error {
	return doClientRequest[finalizeResponse](ctx, c.port, http.MethodPost, "finalize", nil)
}

// Pause sends a request to pause the cluster replication.
func (c PCSMClient) Pause(ctx context.Context) error {
	return doClientRequest[pauseResponse](ctx, c.port, http.MethodPost, "pause", nil)
}

// Resume sends a request to resume the cluster replication.
func (c PCSMClient) Resume(ctx context.Context, req resumeRequest) error {
	return doClientRequest[resumeResponse](ctx, c.port, http.MethodPost, "resume", req)
}

func doClientRequest[T clientResponse](ctx context.Context, port int, method, path string, body any) error {
	url := fmt.Sprintf("http://localhost:%d/%s", port, path)

	bodyData := []byte("")
	if body != nil {
		var err error
		bodyData, err = json.Marshal(body)
		if err != nil {
			return errors.Wrap(err, "encode request")
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(bodyData))
	if err != nil {
		return errors.Wrap(err, "build request")
	}

	log.Ctx(ctx).Debugf("POST /%s %s", path, string(bodyData))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "request")
	}
	defer res.Body.Close()

	data, err := io.ReadAll(res.Body)
	if err != nil {
		return errors.Wrap(err, "read response")
	}

	// A STANDBY rejects write commands with 409 and a not_active body carrying a
	// human-readable message that points at the ACTIVE instance. Surface that
	// message directly instead of the generic decoded error.
	if res.StatusCode == http.StatusConflict {
		var na notActiveResponse
		if json.Unmarshal(data, &na) == nil && na.Message != "" {
			return errors.New(na.Message)
		}
	}

	var resp T

	err = json.Unmarshal(data, &resp)
	if err != nil {
		return errors.Wrap(err, "decode response")
	}

	if !resp.IsOk() {
		return errors.New(resp.GetError())
	}

	j := json.NewEncoder(os.Stdout)
	j.SetIndent("", "  ")
	err = j.Encode(resp)

	return errors.Wrap(err, "print response")
}

func doStatusRequest(ctx context.Context, port int) error {
	url := fmt.Sprintf("http://localhost:%d/status", port)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return errors.Wrap(err, "build request")
	}

	log.Ctx(ctx).Debugf("GET /status")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "request")
	}
	defer res.Body.Close()

	var resp statusResponse

	err = json.NewDecoder(res.Body).Decode(&resp)
	if err != nil {
		return errors.Wrap(err, "decode response")
	}

	j := json.NewEncoder(os.Stdout)
	j.SetIndent("", "  ")

	err = j.Encode(resp)
	if err != nil {
		return errors.Wrap(err, "print response")
	}

	if !resp.IsOk() {
		return errors.New(resp.GetError())
	}

	return nil
}
