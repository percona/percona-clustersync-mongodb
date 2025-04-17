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
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"

	"github.com/percona-lab/percona-mongolink/config"
	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/metrics"
	"github.com/percona-lab/percona-mongolink/mongolink"
	"github.com/percona-lab/percona-mongolink/topo"
	"github.com/percona-lab/percona-mongolink/util"
)

// Constants for server configuration.
const (
	DefaultServerPort       = 2242
	ServerReadTimeout       = 30 * time.Second
	ServerReadHeaderTimeout = 3 * time.Second
	MaxRequestSize          = humanize.MiByte
	ServerResponseTimeout   = 5 * time.Second
)

var (
	Version   = "v0.1" //nolint:gochecknoglobals
	GitCommit = ""     //nolint:gochecknoglobals
	BuildID   = ""     //nolint:gochecknoglobals
)

func buildVersion() string {
	return Version + " " + GitCommit + " " + BuildID
}

//nolint:gochecknoglobals
var rootCmd = &cobra.Command{
	Use:   "mongolink",
	Short: "Percona MongoLink replication tool",

	SilenceUsage: true,

	PersistentPreRun: func(cmd *cobra.Command, _ []string) {
		logLevelFlag, _ := cmd.PersistentFlags().GetString("log-level")
		logJSON, _ := cmd.PersistentFlags().GetBool("log-json")
		logNoColor, _ := cmd.PersistentFlags().GetBool("no-color")

		logLevel, err := zerolog.ParseLevel(logLevelFlag)
		if err != nil {
			log.InitGlobals(0, logJSON, true).Fatal().Msg("Unknown log level")
		}

		lg := log.InitGlobals(logLevel, logJSON, logNoColor)
		ctx := lg.WithContext(context.Background())
		cmd.SetContext(ctx)
	},

	RunE: func(cmd *cobra.Command, _ []string) error {
		// Check if this is the root command being executed without a subcommand
		if cmd.CalledAs() != "mongolink" || cmd.ArgsLenAtDash() != -1 {
			return nil
		}

		port, err := getPort(cmd.Flags())
		if err != nil {
			return err
		}

		sourceURI, _ := cmd.Flags().GetString("source")
		if sourceURI == "" {
			sourceURI = os.Getenv("PML_SOURCE_URI")
		}
		if sourceURI == "" {
			return errors.New("required flag --source not set")
		}

		targetURI, _ := cmd.Flags().GetString("target")
		if targetURI == "" {
			targetURI = os.Getenv("PML_TARGET_URI")
		}
		if targetURI == "" {
			return errors.New("required flag --target not set")
		}

		if ok, _ := cmd.Flags().GetBool("reset-state"); ok {
			err := resetState(cmd.Context(), targetURI)
			if err != nil {
				return err
			}

			log.New("cli").Info("State has been reset")
		}

		start, _ := cmd.Flags().GetBool("start")
		pause, _ := cmd.Flags().GetBool("pause-on-initial-sync")

		log.Ctx(cmd.Context()).Info("Percona MongoLink " + buildVersion())

		return runServer(cmd.Context(), serverOptions{
			port:      port,
			sourceURI: sourceURI,
			targetURI: targetURI,
			start:     start,
			pause:     pause,
		})
	},
}

//nolint:gochecknoglobals
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version",
	Run: func(cmd *cobra.Command, _ []string) {
		cmd.Println(buildVersion())
	},
}

//nolint:gochecknoglobals
var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Get the status of the replication process",
	RunE: func(cmd *cobra.Command, _ []string) error {
		port, err := getPort(cmd.Flags())
		if err != nil {
			return err
		}

		return NewClient(port).Status(cmd.Context())
	},
}

//nolint:gochecknoglobals
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start Cluster Replication",
	RunE: func(cmd *cobra.Command, _ []string) error {
		port, err := getPort(cmd.Flags())
		if err != nil {
			return err
		}

		pauseOnInitialSync, _ := cmd.Flags().GetBool("pause-on-initial-sync")

		startOptions := startRequest{
			PauseOnInitialSync: pauseOnInitialSync,
		}

		return NewClient(port).Start(cmd.Context(), startOptions)
	},
}

//nolint:gochecknoglobals
var finalizeCmd = &cobra.Command{
	Use:   "finalize",
	Short: "Finalize Cluster Replication",
	RunE: func(cmd *cobra.Command, _ []string) error {
		port, err := getPort(cmd.Flags())
		if err != nil {
			return err
		}

		ignoreHistoryLost, _ := cmd.Flags().GetBool("ignore-history-lost")

		finalizeOptions := finalizeRequest{
			IgnoreHistoryLost: ignoreHistoryLost,
		}

		return NewClient(port).Finalize(cmd.Context(), finalizeOptions)
	},
}

//nolint:gochecknoglobals
var pauseCmd = &cobra.Command{
	Use:   "pause",
	Short: "Pause Cluster Replication",
	RunE: func(cmd *cobra.Command, _ []string) error {
		port, err := getPort(cmd.Flags())
		if err != nil {
			return err
		}

		return NewClient(port).Pause(cmd.Context())
	},
}

//nolint:gochecknoglobals
var resumeCmd = &cobra.Command{
	Use:   "resume",
	Short: "Resume Cluster Replication",
	RunE: func(cmd *cobra.Command, _ []string) error {
		port, err := getPort(cmd.Flags())
		if err != nil {
			return err
		}

		return NewClient(port).Resume(cmd.Context())
	},
}

//nolint:gochecknoglobals
var resetCmd = &cobra.Command{
	Use:    "reset",
	Short:  "Reset MongoLink state",
	Hidden: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		targetURI, _ := cmd.Flags().GetString("target")
		if targetURI == "" {
			targetURI = os.Getenv("PML_TARGET_URI")
		}
		if targetURI == "" {
			return errors.New("required flag --target not set")
		}

		err := resetState(cmd.Context(), targetURI)
		if err != nil {
			return err
		}

		log.New("cli").Info("OK: reset all")

		return nil
	},
}

//nolint:gochecknoglobals
var resetRecoveryCmd = &cobra.Command{
	Use:   "recovery",
	Short: "Reset recovery state",
	RunE: func(cmd *cobra.Command, _ []string) error {
		targetURI, _ := cmd.InheritedFlags().GetString("target")
		if targetURI == "" {
			targetURI = os.Getenv("PML_TARGET_URI")
		}
		if targetURI == "" {
			return errors.New("required flag --target not set")
		}

		ctx := cmd.Context()

		target, err := topo.Connect(ctx, targetURI)
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
	Use:   "heartbeat",
	Short: "Reset heartbeat state",
	RunE: func(cmd *cobra.Command, _ []string) error {
		targetURI, _ := cmd.InheritedFlags().GetString("target")
		if targetURI == "" {
			targetURI = os.Getenv("PML_TARGET_URI")
		}
		if targetURI == "" {
			return errors.New("required flag --target not set")
		}

		ctx := cmd.Context()

		target, err := topo.Connect(ctx, targetURI)
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

func getPort(flags *pflag.FlagSet) (int, error) {
	port, _ := flags.GetInt("port")
	if flags.Changed("port") {
		return port, nil
	}

	portVar := os.Getenv("PML_PORT")
	if portVar == "" {
		return port, nil
	}

	parsedPort, err := strconv.ParseInt(portVar, 10, 32)
	if err != nil {
		return 0, errors.Errorf("invalid environment variable PML_PORT='%s'", portVar)
	}

	return int(parsedPort), nil
}

func main() {
	rootCmd.PersistentFlags().String("log-level", "info", "Log level")
	rootCmd.PersistentFlags().Bool("log-json", false, "Output log in JSON format")
	rootCmd.PersistentFlags().Bool("no-color", false, "Disable log color")

	rootCmd.Flags().Int("port", DefaultServerPort, "Port number")
	rootCmd.Flags().String("source", "", "MongoDB connection string for the source")
	rootCmd.Flags().String("target", "", "MongoDB connection string for the target")
	rootCmd.Flags().Bool("start", false, "Start Cluster Replication immediately")
	rootCmd.Flags().Bool("reset-state", false, "Reset stored MongoLink state")
	rootCmd.Flags().Bool("pause-on-initial-sync", false, "Pause on Initial Sync")
	rootCmd.Flags().MarkHidden("start")                 //nolint:errcheck
	rootCmd.Flags().MarkHidden("reset-state")           //nolint:errcheck
	rootCmd.Flags().MarkHidden("pause-on-initial-sync") //nolint:errcheck

	statusCmd.Flags().Int("port", DefaultServerPort, "Port number")

	startCmd.Flags().Int("port", DefaultServerPort, "Port number")
	startCmd.Flags().Bool("pause-on-initial-sync", false, "Pause on Initial Sync")
	startCmd.Flags().MarkHidden("pause-on-initial-sync") //nolint:errcheck

	pauseCmd.Flags().Int("port", DefaultServerPort, "Port number")
	resumeCmd.Flags().Int("port", DefaultServerPort, "Port number")

	finalizeCmd.Flags().Int("port", DefaultServerPort, "Port number")
	finalizeCmd.Flags().Bool("ignore-history-lost", false, "Ignore history lost error")
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

func resetState(ctx context.Context, targetURI string) error {
	target, err := topo.Connect(ctx, targetURI)
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

type serverOptions struct {
	port      int
	sourceURI string
	targetURI string
	start     bool
	pause     bool
}

func (s serverOptions) validate() error {
	if s.port <= 1024 || s.port > 65535 {
		return errors.New("port value is outside the supported range [1024 - 65535]")
	}

	switch {
	case s.sourceURI == "" && s.targetURI == "":
		return errors.New("source URI and target URI are empty")
	case s.sourceURI == "":
		return errors.New("source URI is empty")
	case s.targetURI == "":
		return errors.New("target URI is empty")
	case s.sourceURI == s.targetURI:
		return errors.New("source URI and target URI are identical")
	}

	return nil
}

// runServer starts the HTTP server with the provided configuration.
func runServer(ctx context.Context, options serverOptions) error {
	err := options.validate()
	if err != nil {
		return errors.Wrap(err, "validate options")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	srv, err := createServer(ctx, options.sourceURI, options.targetURI)
	if err != nil {
		return errors.Wrap(err, "new server")
	}

	if options.start && srv.mlink.Status(ctx).State == mongolink.StateIdle {
		err = srv.mlink.Start(ctx, &mongolink.StartOptions{
			PauseOnInitialSync: options.pause,
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

	addr := fmt.Sprintf("localhost:%d", options.port)
	httpServer := http.Server{
		Addr:    addr,
		Handler: srv.Handler(),

		ReadTimeout:       ServerReadTimeout,
		ReadHeaderTimeout: ServerReadHeaderTimeout,
	}

	log.Ctx(ctx).Info("Starting HTTP server at http://" + addr)

	return httpServer.ListenAndServe() //nolint:wrapcheck
}

// server represents the replication server.
type server struct {
	// sourceCluster is the MongoDB client for the source cluster.
	sourceCluster *mongo.Client
	// targetCluster is the MongoDB client for the target cluster.
	targetCluster *mongo.Client
	// mlink is the MongoLink instance for cluster replication.
	mlink *mongolink.MongoLink
	// stopHeartbeat stops the heartbeat process in the application.
	stopHeartbeat StopHeartbeat

	// promRegistry is the Prometheus registry for metrics.
	promRegistry *prometheus.Registry
}

// createServer creates a new server with the given options.
func createServer(ctx context.Context, sourceURI, targetURI string) (*server, error) {
	lg := log.Ctx(ctx)

	source, err := topo.Connect(ctx, sourceURI)
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

	cs, _ := connstring.Parse(sourceURI)
	lg.Infof("Connected to source cluster [%s]: %s://%s",
		sourceVersion.FullString(), cs.Scheme, strings.Join(cs.Hosts, ","))

	target, err := topo.ConnectWithOptions(ctx, targetURI, &topo.ConnectOptions{
		Compressors: config.UseTargetClientCompressors(),
	})
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

	cs, _ = connstring.Parse(targetURI)
	lg.Infof("Connected to target cluster [%s]: %s://%s",
		targetVersion.FullString(), cs.Scheme, strings.Join(cs.Hosts, ","))

	stopHeartbeat, err := RunHeartbeat(ctx, target)
	if err != nil {
		return nil, errors.Wrap(err, "heartbeat")
	}

	promRegistry := prometheus.NewRegistry()
	metrics.Init(promRegistry)

	mlink := mongolink.New(source, target)

	err = Restore(ctx, target, mlink)
	if err != nil {
		return nil, errors.Wrap(err, "recover MongoLink")
	}

	go RunCheckpointing(ctx, target, mlink)

	s := &server{
		sourceCluster: source,
		targetCluster: target,
		mlink:         mlink,
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

	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/start", s.handleStart)
	mux.HandleFunc("/finalize", s.handleFinalize)
	mux.HandleFunc("/pause", s.handlePause)
	mux.HandleFunc("/resume", s.handleResume)
	mux.Handle("/metrics", s.handleMetrics())

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			log.New("http").Trace(r.Method + " " + r.URL.String())
		} else {
			log.New("http").Info(r.Method + " " + r.URL.String())
		}
		mux.ServeHTTP(w, r)
	})
}

// handleStatus handles the /status endpoint.
func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
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

	status := s.mlink.Status(ctx)

	res := statusResponse{
		Ok:    status.Error == nil,
		State: status.State,
	}

	if err := status.Error; err != nil {
		res.Err = err.Error()
	}

	if status.State == mongolink.StateIdle {
		writeResponse(w, res)

		return
	}

	res.EventsProcessed = &status.Repl.EventsProcessed
	res.LagTime = status.TotalLagTime

	if !status.Repl.LastReplicatedOpTime.IsZero() {
		res.LastReplicatedOpTime = fmt.Sprintf("%d.%d",
			status.Repl.LastReplicatedOpTime.T,
			status.Repl.LastReplicatedOpTime.I)
	}

	res.InitialSync = &statusInitialSyncResponse{
		Completed: status.InitialSyncCompleted,
		LagTime:   status.InitialSyncLagTime,

		CloneCompleted:     status.Clone.IsFinished(),
		EstimatedCloneSize: &status.Clone.EstimatedTotalSize,
		ClonedSize:         status.Clone.CopiedSize,
	}

	switch {
	case status.State == mongolink.StateRunning && !status.Clone.IsFinished():
		res.Info = "Initial Sync: Cloning Data"
	case status.State == mongolink.StateRunning && !status.InitialSyncCompleted:
		res.Info = "Initial Sync: Replicating Changes"
	case status.State == mongolink.StateRunning:
		res.Info = "Replicating Changes"
	case status.State == mongolink.StateFinalizing:
		res.Info = "Finalizing"
	case status.State == mongolink.StateFinalized:
		res.Info = "Finalized"
	case status.State == mongolink.StateFailed:
		res.Info = "Failed"
	}

	writeResponse(w, res)
}

// handleStart handles the /start endpoint.
func (s *server) handleStart(w http.ResponseWriter, r *http.Request) {
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

	options := &mongolink.StartOptions{
		PauseOnInitialSync: params.PauseOnInitialSync,
		IncludeNamespaces:  params.IncludeNamespaces,
		ExcludeNamespaces:  params.ExcludeNamespaces,
	}

	err := s.mlink.Start(ctx, options)
	if err != nil {
		writeResponse(w, startResponse{Err: err.Error()})

		return
	}

	writeResponse(w, startResponse{Ok: true})
}

// handleFinalize handles the /finalize endpoint.
func (s *server) handleFinalize(w http.ResponseWriter, r *http.Request) {
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

	options := &mongolink.FinalizeOptions{
		IgnoreHistoryLost: params.IgnoreHistoryLost,
	}

	err := s.mlink.Finalize(ctx, *options)
	if err != nil {
		writeResponse(w, finalizeResponse{Err: err.Error()})

		return
	}

	writeResponse(w, finalizeResponse{Ok: true})
}

// handlePause handles the /pause endpoint.
func (s *server) handlePause(w http.ResponseWriter, r *http.Request) {
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

	err := s.mlink.Pause(ctx)
	if err != nil {
		writeResponse(w, pauseResponse{Err: err.Error()})

		return
	}

	writeResponse(w, pauseResponse{Ok: true})
}

// handleResume handles the /resume endpoint.
func (s *server) handleResume(w http.ResponseWriter, r *http.Request) {
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

	err := s.mlink.Resume(ctx)
	if err != nil {
		writeResponse(w, resumeResponse{Err: err.Error()})

		return
	}

	writeResponse(w, resumeResponse{Ok: true})
}

func (s *server) handleMetrics() http.Handler {
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
	State mongolink.State `json:"state"`
	// Info provides additional information about the current state.
	Info string `json:"info,omitempty"`

	// LagTime is the current lag time in logical seconds.
	LagTime *int64 `json:"lagTime,omitempty"`
	// EventsProcessed is the number of events processed.
	EventsProcessed *int64 `json:"eventsProcessed,omitempty"`
	// LastReplicatedOpTime is the last replicated operation time.
	LastReplicatedOpTime string `json:"lastReplicatedOpTime,omitempty"`

	// InitialSync contains the initial sync status details.
	InitialSync *statusInitialSyncResponse `json:"initialSync,omitempty"`
}

// statusInitialSyncResponse represents the initial sync status in the /status response.
type statusInitialSyncResponse struct {
	// LagTime is the lag time in logical seconds until the initial sync completed.
	LagTime *int64 `json:"lagTime,omitempty"`

	// EstimatedCloneSize is the estimated total size of the clone.
	EstimatedCloneSize *uint64 `json:"estimatedCloneSize,omitempty"`
	// ClonedSize is the size of the data that has been cloned.
	ClonedSize uint64 `json:"clonedSize"`

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

// resumeResponse represents the response body for the /resume
// endpoint.
type resumeResponse struct {
	// Ok indicates if the operation was successful.
	Ok bool `json:"ok"`
	// Err is the error message if the operation failed.
	Err string `json:"error,omitempty"`
}

type MongoLinkClient struct {
	port int
}

func NewClient(port int) MongoLinkClient {
	return MongoLinkClient{port: port}
}

// Status sends a request to get the status of the cluster replication.
func (c MongoLinkClient) Status(ctx context.Context) error {
	return doClientRequest[statusResponse](ctx, c.port, http.MethodGet, "status", nil)
}

// Start sends a request to start the cluster replication.
func (c MongoLinkClient) Start(ctx context.Context, req startRequest) error {
	return doClientRequest[startResponse](ctx, c.port, http.MethodPost, "start", req)
}

// Finalize sends a request to finalize the cluster replication.
func (c MongoLinkClient) Finalize(ctx context.Context, req finalizeRequest) error {
	return doClientRequest[finalizeResponse](ctx, c.port, http.MethodPost, "finalize", req)
}

// Pause sends a request to pause the cluster replication.
func (c MongoLinkClient) Pause(ctx context.Context) error {
	return doClientRequest[pauseResponse](ctx, c.port, http.MethodPost, "pause", nil)
}

// Resume sends a request to resume the cluster replication.
func (c MongoLinkClient) Resume(ctx context.Context) error {
	return doClientRequest[resumeResponse](ctx, c.port, http.MethodPost, "resume", nil)
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
