package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
	"github.com/percona/percona-clustersync-mongodb/metrics"
	"github.com/percona/percona-clustersync-mongodb/pcsm"
	"github.com/percona/percona-clustersync-mongodb/pcsm/clone"
	"github.com/percona/percona-clustersync-mongodb/pcsm/repl"
	"github.com/percona/percona-clustersync-mongodb/topo"
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
	Version   = "v0.7.0" //nolint:gochecknoglobals
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
	cmd.Flags().String("clone-segment-size", "", "")
	cmd.Flags().MarkHidden("clone-segment-size") //nolint:errcheck

	cmd.Flags().String("clone-read-batch-size", "", "")
	cmd.Flags().MarkHidden("clone-read-batch-size") //nolint:errcheck

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
		newResetHeartbeatCmd(cfg),
	)

	return cmd
}

func newResetRecoveryCmd(cfg *config.Config) *cobra.Command {
	return &cobra.Command{
		Use:    "recovery",
		Hidden: true,
		Short:  "Reset recovery state",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			target, err := topo.Connect(ctx, cfg.Target, cfg)
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

func newResetHeartbeatCmd(cfg *config.Config) *cobra.Command {
	return &cobra.Command{
		Use:    "heartbeat",
		Hidden: true,
		Short:  "Reset heartbeat state",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			target, err := topo.Connect(ctx, cfg.Target, cfg)
			if err != nil {
				return errors.Wrap(err, "connect")
			}

			defer func() {
				err := util.CtxWithTimeout(ctx, config.DisconnectTimeout, target.Disconnect)
				if err != nil {
					log.Ctx(ctx).Warn("Disconnect: " + err.Error())
				}
			}()

			err = DeleteHeartbeat(ctx, target)
			if err != nil {
				return err
			}

			log.New("cli").Info("OK: reset heartbeat")

			return nil
		},
	}
}

func resetState(ctx context.Context, cfg *config.Config) error {
	target, err := topo.Connect(ctx, cfg.Target, cfg)
	if err != nil {
		return errors.Wrap(err, "connect")
	}

	defer func() {
		err := util.CtxWithTimeout(ctx, config.DisconnectTimeout, target.Disconnect)
		if err != nil {
			log.Ctx(ctx).Warn("Disconnect: " + err.Error())
		}
	}()

	err = DeleteHeartbeat(ctx, target)
	if err != nil {
		return errors.Wrap(err, "delete heartbeat")
	}

	err = DeleteRecoveryData(ctx, target)
	if err != nil {
		return errors.Wrap(err, "delete heartbeat")
	}

	return nil
}

// runServer starts the HTTP server with the provided configuration.
func runServer(cfg *config.Config) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	srv, err := createServer(ctx, cfg)
	if err != nil {
		return errors.Wrap(err, "new server")
	}

	if cfg.Start && srv.pcsm.Status(ctx).State == pcsm.StateIdle {
		startOpts, err := resolveStartOptions(cfg, startRequest{
			PauseOnInitialSync: cfg.PauseOnInitialSync,
		})
		if err != nil {
			return err
		}

		err = srv.pcsm.Start(ctx, startOpts)
		if err != nil {
			log.New("cli").Error(err, "Failed to start Cluster Replication")
		}
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

	addr := fmt.Sprintf("localhost:%d", port)
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
	// stopHeartbeat stops the heartbeat process in the application.
	stopHeartbeat StopHeartbeat

	// promRegistry is the Prometheus registry for metrics.
	promRegistry *prometheus.Registry
}

// createServer creates a new server with the given options.
func createServer(ctx context.Context, cfg *config.Config) (*server, error) {
	lg := log.Ctx(ctx)

	source, err := topo.Connect(ctx, cfg.Source, cfg)
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

	sourceVersion, err := topo.Version(ctx, source)
	if err != nil {
		return nil, errors.Wrap(err, "source version")
	}

	cs, _ := connstring.Parse(cfg.Source)
	lg.Infof("Connected to source cluster [%s]: %s://%s",
		sourceVersion.FullString(), cs.Scheme, strings.Join(cs.Hosts, ","))

	target, err := topo.Connect(ctx, cfg.Target, cfg)
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

	targetVersion, err := topo.Version(ctx, target)
	if err != nil {
		return nil, errors.Wrap(err, "target version")
	}

	cs, _ = connstring.Parse(cfg.Target)
	lg.Infof("Connected to target cluster [%s]: %s://%s",
		targetVersion.FullString(), cs.Scheme, strings.Join(cs.Hosts, ","))

	stopHeartbeat, err := RunHeartbeat(ctx, target)
	if err != nil {
		return nil, errors.Wrap(err, "heartbeat")
	}

	promRegistry := prometheus.NewRegistry()
	metrics.Init(promRegistry)

	pcs := pcsm.New(ctx, source, target)

	err = Restore(ctx, target, pcs)
	if err != nil {
		return nil, errors.Wrap(err, "recover PCSM")
	}

	pcs.SetOnStateChanged(func(newState pcsm.State) {
		err := DoCheckpoint(ctx, target, pcs)
		if err != nil {
			log.New("http:checkpointing").Error(err, "checkpoint")
		} else {
			log.New("http:checkpointing").Debugf("Checkpoint saved on %q", newState)
		}
	})

	go RunCheckpointing(ctx, target, pcs)

	s := &server{
		cfg:           cfg,
		sourceCluster: source,
		targetCluster: target,
		pcsm:          pcs,
		stopHeartbeat: stopHeartbeat,
		promRegistry:  promRegistry,
	}

	return s, nil
}

// Close stops heartbeat and closes the server connections.
func (s *server) Close(ctx context.Context) error {
	err0 := s.stopHeartbeat(ctx)
	err1 := s.sourceCluster.Disconnect(ctx)
	err2 := s.targetCluster.Disconnect(ctx)

	return errors.Join(err0, err1, err2)
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
		Ok:    status.Error == nil,
		State: status.State,
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
		writeResponse(w, startResponse{Err: err.Error()})

		return
	}

	err = s.pcsm.Start(ctx, options)
	if err != nil {
		writeResponse(w, startResponse{Err: err.Error()})

		return
	}

	writeResponse(w, startResponse{Ok: true})
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

	err := s.pcsm.Finalize(ctx)
	if err != nil {
		writeResponse(w, finalizeResponse{Err: err.Error()})

		return
	}

	writeResponse(w, finalizeResponse{Ok: true})
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

	err := s.pcsm.Pause(ctx)
	if err != nil {
		writeResponse(w, pauseResponse{Err: err.Error()})

		return
	}

	writeResponse(w, pauseResponse{Ok: true})
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
		writeResponse(w, resumeResponse{Err: err.Error()})

		return
	}

	writeResponse(w, resumeResponse{Ok: true})
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
	// Ok indicates if the operation was successful.
	Ok bool `json:"ok"`
	// Err is the error message if the operation failed.
	Err string `json:"error,omitempty"`
}

func (r startResponse) IsOk() bool       { return r.Ok }
func (r startResponse) GetError() string { return r.Err }

// finalizeResponse represents the response body for the /finalize endpoint.
type finalizeResponse struct {
	// Ok indicates if the operation was successful.
	Ok bool `json:"ok"`
	// Err is the error message if the operation failed.
	Err string `json:"error,omitempty"`
}

func (r finalizeResponse) IsOk() bool       { return r.Ok }
func (r finalizeResponse) GetError() string { return r.Err }

// statusResponse represents the response body for the /status endpoint.
type statusResponse struct {
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

// pauseResponse represents the response body for the /pause endpoint.
type pauseResponse struct {
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
func (c PCSMClient) Status(ctx context.Context) error {
	return doClientRequest[statusResponse](ctx, c.port, http.MethodGet, "status", nil)
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

	var resp T

	err = json.NewDecoder(res.Body).Decode(&resp)
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
