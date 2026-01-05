package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
	"github.com/percona/percona-clustersync-mongodb/metrics"
	"github.com/percona/percona-clustersync-mongodb/pcsm"
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

// contextKey is a type for context keys used in this package.
type contextKey string

// configContextKey is the context key for storing *config.Config.
const configContextKey contextKey = "config"

var (
	Version   = "v0.6.0" //nolint:gochecknoglobals
	Platform  = ""       //nolint:gochecknoglobals
	GitCommit = ""       //nolint:gochecknoglobals
	GitBranch = ""       //nolint:gochecknoglobals
	BuildTime = ""       //nolint:gochecknoglobals
)

func buildVersion() string {
	return Version + " " + GitCommit + " " + BuildTime
}

//nolint:gochecknoglobals
var rootCmd = &cobra.Command{
	Use:   "pcsm",
	Short: "Percona ClusterSync for MongoDB replication tool",

	SilenceUsage: true,

	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		// Load and validate config
		cfg, err := config.Load(cmd)
		if err != nil {
			return errors.Wrap(err, "load config")
		}

		logLevel, err := zerolog.ParseLevel(cfg.Log.Level)
		if err != nil {
			logLevel = zerolog.InfoLevel
		}

		lg := log.InitGlobals(logLevel, cfg.Log.JSON, cfg.Log.NoColor)
		ctx := lg.WithContext(context.Background())
		ctx = context.WithValue(ctx, configContextKey, cfg)
		cmd.SetContext(ctx)

		return nil
	},

	RunE: func(cmd *cobra.Command, _ []string) error {
		// Check if this is the root command being executed without a subcommand
		if cmd.CalledAs() != "pcsm" || cmd.ArgsLenAtDash() != -1 {
			return nil
		}

		cfg := cmd.Context().Value(configContextKey).(*config.Config) //nolint:forcetypeassert

		if cfg.Source == "" {
			return errors.New("required flag --source not set")
		}

		if cfg.Target == "" {
			return errors.New("required flag --target not set")
		}

		if cfg.ResetState {
			err := resetState(cmd.Context(), cfg.Target, cfg)
			if err != nil {
				return err
			}

			log.New("cli").Info("State has been reset")
		}

		log.Ctx(cmd.Context()).Info("Percona ClusterSync for MongoDB " + buildVersion())

		return runServer(cmd.Context(), cfg)
	},
}

//nolint:gochecknoglobals
var versionCmd = &cobra.Command{
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

//nolint:gochecknoglobals
var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Get the status of the replication process",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return NewClient(viper.GetInt("port")).Status(cmd.Context())
	},
}

//nolint:gochecknoglobals
var startCmd = &cobra.Command{
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

		// Read clone tuning flags (only set in request if explicitly provided)
		if cmd.Flags().Changed("clone-num-parallel-collections") {
			v, _ := cmd.Flags().GetInt("clone-num-parallel-collections")
			startOptions.CloneNumParallelCollections = &v
		}
		if cmd.Flags().Changed("clone-num-read-workers") {
			v, _ := cmd.Flags().GetInt("clone-num-read-workers")
			startOptions.CloneNumReadWorkers = &v
		}
		if cmd.Flags().Changed("clone-num-insert-workers") {
			v, _ := cmd.Flags().GetInt("clone-num-insert-workers")
			startOptions.CloneNumInsertWorkers = &v
		}
		if cmd.Flags().Changed("clone-segment-size") {
			v, _ := cmd.Flags().GetString("clone-segment-size")
			startOptions.CloneSegmentSize = &v
		}
		if cmd.Flags().Changed("clone-read-batch-size") {
			v, _ := cmd.Flags().GetString("clone-read-batch-size")
			startOptions.CloneReadBatchSize = &v
		}

		return NewClient(viper.GetInt("port")).Start(cmd.Context(), startOptions)
	},
}

//nolint:gochecknoglobals
var finalizeCmd = &cobra.Command{
	Use:   "finalize",
	Short: "Finalize Cluster Replication",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ignoreHistoryLost, _ := cmd.Flags().GetBool("ignore-history-lost")

		finalizeOptions := finalizeRequest{
			IgnoreHistoryLost: ignoreHistoryLost,
		}

		return NewClient(viper.GetInt("port")).Finalize(cmd.Context(), finalizeOptions)
	},
}

//nolint:gochecknoglobals
var pauseCmd = &cobra.Command{
	Use:   "pause",
	Short: "Pause Cluster Replication",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return NewClient(viper.GetInt("port")).Pause(cmd.Context())
	},
}

//nolint:gochecknoglobals
var resumeCmd = &cobra.Command{
	Use:   "resume",
	Short: "Resume Cluster Replication",
	RunE: func(cmd *cobra.Command, _ []string) error {
		fromFailure, _ := cmd.Flags().GetBool("from-failure")

		resumeOptions := resumeRequest{
			FromFailure: fromFailure,
		}

		return NewClient(viper.GetInt("port")).Resume(cmd.Context(), resumeOptions)
	},
}

//nolint:gochecknoglobals
var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset PCSM state (heartbeat and recovery data)",
	RunE: func(cmd *cobra.Command, _ []string) error {
		cfg := cmd.Context().Value(configContextKey).(*config.Config) //nolint:forcetypeassert

		targetURI := viper.GetString("target")
		if targetURI == "" {
			return errors.New("required flag --target not set")
		}

		err := resetState(cmd.Context(), targetURI, cfg)
		if err != nil {
			return err
		}

		log.New("cli").Info("OK: reset all")

		return nil
	},
}

//nolint:gochecknoglobals
var resetRecoveryCmd = &cobra.Command{
	Use:    "recovery",
	Hidden: true,
	Short:  "Reset recovery state",
	RunE: func(cmd *cobra.Command, _ []string) error {
		cfg := cmd.Context().Value(configContextKey).(*config.Config) //nolint:forcetypeassert

		targetURI := viper.GetString("target")
		if targetURI == "" {
			return errors.New("required flag --target not set")
		}

		ctx := cmd.Context()

		target, err := topo.Connect(ctx, targetURI, cfg)
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

//nolint:gochecknoglobals
var resetHeartbeatCmd = &cobra.Command{
	Use:    "heartbeat",
	Hidden: true,
	Short:  "Reset heartbeat state",
	RunE: func(cmd *cobra.Command, _ []string) error {
		cfg := cmd.Context().Value(configContextKey).(*config.Config) //nolint:forcetypeassert

		targetURI := viper.GetString("target")
		if targetURI == "" {
			return errors.New("required flag --target not set")
		}

		ctx := cmd.Context()

		target, err := topo.Connect(ctx, targetURI, cfg)
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

func main() {
	rootCmd.PersistentFlags().String("log-level", "info", "Log level")
	rootCmd.PersistentFlags().Bool("log-json", false, "Output log in JSON format")
	rootCmd.PersistentFlags().Bool("log-no-color", false, "Disable log color")

	rootCmd.PersistentFlags().Bool("no-color", false, "")
	rootCmd.PersistentFlags().MarkDeprecated("no-color", "use --log-no-color instead") //nolint:errcheck

	rootCmd.PersistentFlags().Int("port", config.DefaultServerPort, "Port number")
	rootCmd.Flags().String("source", "", "MongoDB connection string for the source")
	rootCmd.Flags().String("target", "", "MongoDB connection string for the target")
	rootCmd.Flags().Bool("start", false, "")
	rootCmd.Flags().MarkHidden("start") //nolint:errcheck

	rootCmd.Flags().Bool("reset-state", false, "")
	rootCmd.Flags().MarkHidden("reset-state") //nolint:errcheck

	rootCmd.Flags().Bool("pause-on-initial-sync", false, "")
	rootCmd.Flags().MarkHidden("pause-on-initial-sync") //nolint:errcheck

	// MongoDB client timeout (visible: commonly needed for debugging)
	rootCmd.PersistentFlags().String("mongodb-operation-timeout", config.DefaultMongoDBOperationTimeout.String(),
		"Timeout for MongoDB operations (e.g., 30s, 5m)")

	startCmd.Flags().Bool("pause-on-initial-sync", false, "")
	startCmd.Flags().MarkHidden("pause-on-initial-sync") //nolint:errcheck

	startCmd.Flags().StringSlice("include-namespaces", nil,
		"Namespaces to include in the replication (e.g. db1.collection1,db2.collection2)")
	startCmd.Flags().StringSlice("exclude-namespaces", nil,
		"Namespaces to exclude from the replication (e.g. db3.collection3,db4.*)")

	// Clone tuning options (per-operation, passed via CLI or HTTP request)
	startCmd.Flags().Int("clone-num-parallel-collections", 0,
		"Number of collections to clone in parallel (0 = auto)")
	startCmd.Flags().Int("clone-num-read-workers", 0,
		"Number of read workers during clone (0 = auto)")
	startCmd.Flags().Int("clone-num-insert-workers", 0,
		"Number of insert workers during clone (0 = auto)")
	startCmd.Flags().String("clone-segment-size", "", "")
	startCmd.Flags().MarkHidden("clone-segment-size") //nolint:errcheck

	startCmd.Flags().String("clone-read-batch-size", "", "")
	startCmd.Flags().MarkHidden("clone-read-batch-size") //nolint:errcheck

	resumeCmd.Flags().Bool("from-failure", false, "Reuse from failure")

	finalizeCmd.Flags().Bool("ignore-history-lost", false, "")
	finalizeCmd.Flags().MarkHidden("ignore-history-lost") //nolint:errcheck

	resetCmd.Flags().String("target", "", "MongoDB connection string for the target")

	resetCmd.AddCommand(resetRecoveryCmd, resetHeartbeatCmd)
	rootCmd.AddCommand(
		versionCmd,
		statusCmd,
		startCmd,
		finalizeCmd,
		pauseCmd,
		resumeCmd,
		resetCmd,
	)

	err := rootCmd.Execute()
	if err != nil {
		zerolog.Ctx(context.Background()).Fatal().Err(err).Msg("")
	}
}

func resetState(ctx context.Context, targetURI string, cfg *config.Config) error {
	target, err := topo.Connect(ctx, targetURI, cfg)
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
func runServer(ctx context.Context, cfg *config.Config) error {
	err := config.Validate(cfg)
	if err != nil {
		return errors.Wrap(err, "validate options")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	srv, err := createServer(ctx, cfg)
	if err != nil {
		return errors.Wrap(err, "new server")
	}

	if cfg.Start && srv.pcsm.Status(ctx).State == pcsm.StateIdle {
		err = srv.pcsm.Start(ctx, &pcsm.StartOptions{
			PauseOnInitialSync: cfg.PauseOnInitialSync,
		})
		if err != nil {
			log.New("cli").Error(err, "Failed to start Cluster Replication")
		}
	}

	go func() {
		<-ctx.Done()

		err := util.CtxWithTimeout(ctx, config.DisconnectTimeout, srv.Close)
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

// Server represents the replication Server.
type Server struct {
	// Cfg holds the configuration.
	Cfg *config.Config
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
func createServer(ctx context.Context, cfg *config.Config) (*Server, error) {
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

	pcs := pcsm.New(source, target)

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

	s := &Server{
		Cfg:           cfg,
		sourceCluster: source,
		targetCluster: target,
		pcsm:          pcs,
		stopHeartbeat: stopHeartbeat,
		promRegistry:  promRegistry,
	}

	return s, nil
}

// Close stops heartbeat and closes the server connections.
func (s *Server) Close(ctx context.Context) error {
	err0 := s.stopHeartbeat(ctx)
	err1 := s.sourceCluster.Disconnect(ctx)
	err2 := s.targetCluster.Disconnect(ctx)

	return errors.Join(err0, err1, err2)
}

// Handler returns the HTTP handler for the server.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/status", s.HandleStatus)
	mux.HandleFunc("/start", s.HandleStart)
	mux.HandleFunc("/finalize", s.HandleFinalize)
	mux.HandleFunc("/pause", s.HandlePause)
	mux.HandleFunc("/resume", s.HandleResume)
	mux.Handle("/metrics", s.HandleMetrics())

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
func (s *Server) HandleStatus(w http.ResponseWriter, r *http.Request) {
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

// resolveStartOptions resolves the start options from the HTTP request and config.
// Clone tuning options use config (env var) as defaults, CLI/HTTP params override.
func resolveStartOptions(cfg *config.Config, params startRequest) (*pcsm.StartOptions, error) {
	options := &pcsm.StartOptions{
		PauseOnInitialSync: params.PauseOnInitialSync,
		IncludeNamespaces:  params.IncludeNamespaces,
		ExcludeNamespaces:  params.ExcludeNamespaces,
		Repl: pcsm.ReplOptions{
			UseCollectionBulkWrite: cfg.UseCollectionBulkWrite,
		},
		Clone: pcsm.CloneOptions{
			Parallelism:   cfg.Clone.NumParallelCollections,
			ReadWorkers:   cfg.Clone.NumReadWorkers,
			InsertWorkers: cfg.Clone.NumInsertWorkers,
		},
	}

	if params.CloneNumParallelCollections != nil {
		options.Clone.Parallelism = *params.CloneNumParallelCollections
	}

	if params.CloneNumReadWorkers != nil {
		options.Clone.ReadWorkers = *params.CloneNumReadWorkers
	}

	if params.CloneNumInsertWorkers != nil {
		options.Clone.InsertWorkers = *params.CloneNumInsertWorkers
	}

	segmentSizeStr := cfg.Clone.SegmentSize
	if params.CloneSegmentSize != nil {
		segmentSizeStr = *params.CloneSegmentSize
	}

	if segmentSizeStr != "" {
		segmentSize, err := config.ParseAndValidateCloneSegmentSize(segmentSizeStr)
		if err != nil {
			return nil, errors.Wrap(err, "invalid clone segment size")
		}
		options.Clone.SegmentSizeBytes = segmentSize
	}

	readBatchSizeStr := cfg.Clone.ReadBatchSize
	if params.CloneReadBatchSize != nil {
		readBatchSizeStr = *params.CloneReadBatchSize
	}

	if readBatchSizeStr != "" {
		batchSize, err := config.ParseAndValidateCloneReadBatchSize(readBatchSizeStr)
		if err != nil {
			return nil, errors.Wrap(err, "invalid clone read batch size")
		}
		options.Clone.ReadBatchSizeBytes = batchSize
	}

	return options, nil
}

// HandleStart handles the /start endpoint.
func (s *Server) HandleStart(w http.ResponseWriter, r *http.Request) {
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

	options, err := resolveStartOptions(s.Cfg, params)
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
func (s *Server) HandleFinalize(w http.ResponseWriter, r *http.Request) {
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

	var params finalizeRequest

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

	options := &pcsm.FinalizeOptions{
		IgnoreHistoryLost: params.IgnoreHistoryLost,
	}

	err := s.pcsm.Finalize(ctx, *options)
	if err != nil {
		writeResponse(w, finalizeResponse{Err: err.Error()})

		return
	}

	writeResponse(w, finalizeResponse{Ok: true})
}

// HandlePause handles the /pause endpoint.
func (s *Server) HandlePause(w http.ResponseWriter, r *http.Request) {
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
func (s *Server) HandleResume(w http.ResponseWriter, r *http.Request) {
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

func (s *Server) HandleMetrics() http.Handler {
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

	// NOTE: UseCollectionBulkWrite intentionally NOT exposed via HTTP (internal only)
}

// startResponse represents the response body for the /start endpoint.
type startResponse struct {
	// Ok indicates if the operation was successful.
	Ok bool `json:"ok"`
	// Err is the error message if the operation failed.
	Err string `json:"error,omitempty"`
}

// finalizeRequest represents the request body for the /finalize endpoint.
type finalizeRequest struct {
	// IgnoreHistoryLost indicates whether the operation can ignore the ChangeStreamHistoryLost
	// error.
	IgnoreHistoryLost bool `json:"ignoreHistoryLost,omitempty"`
}

// finalizeResponse represents the response body for the /finalize endpoint.
type finalizeResponse struct {
	// Ok indicates if the operation was successful.
	Ok bool `json:"ok"`
	// Err is the error message if the operation failed.
	Err string `json:"error,omitempty"`
}

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
func (c PCSMClient) Finalize(ctx context.Context, req finalizeRequest) error {
	return doClientRequest[finalizeResponse](ctx, c.port, http.MethodPost, "finalize", req)
}

// Pause sends a request to pause the cluster replication.
func (c PCSMClient) Pause(ctx context.Context) error {
	return doClientRequest[pauseResponse](ctx, c.port, http.MethodPost, "pause", nil)
}

// Resume sends a request to resume the cluster replication.
func (c PCSMClient) Resume(ctx context.Context, req resumeRequest) error {
	return doClientRequest[resumeResponse](ctx, c.port, http.MethodPost, "resume", req)
}

func doClientRequest[T any](ctx context.Context, port int, method, path string, body any) error {
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

	j := json.NewEncoder(os.Stdout)
	j.SetIndent("", "  ")
	err = j.Encode(resp)

	return errors.Wrap(err, "print response")
}
