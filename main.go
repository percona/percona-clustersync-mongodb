package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/rs/zerolog"

	"github.com/percona-lab/percona-mongolink/config"
	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/mongolink"
	"github.com/percona-lab/percona-mongolink/topo"
)

// Constants for server configuration.
const (
	ServerReadTimeout       = 30 * time.Second
	ServerReadHeaderTimeout = 3 * time.Second
	MaxRequestSize          = config.MiB
	ServerResponseTimeout   = 5 * time.Second
)

func main() {
	var (
		logLevelFlag string
		logJSON      bool
		logNoColor   bool

		port string
	)

	rootCmd := &cobra.Command{
		Use:   "mongolink",
		Short: "Percona MongoLink replication tool",
		PersistentPreRun: func(cmd *cobra.Command, _ []string) {
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

			port, err := cmd.Flags().GetString("port")
			if err != nil {
				return err //nolint:wrapcheck
			}

			sourceURI, _ := cmd.Flags().GetString("source")
			if sourceURI == "" {
				return errors.New("required flag --source not set")
			}

			targetURI, _ := cmd.Flags().GetString("target")
			if targetURI == "" {
				return errors.New("required flag --target not set")
			}

			return runServer(cmd.Context(), port, sourceURI, targetURI)
		},
	}

	rootCmd.SilenceErrors = true
	rootCmd.SilenceUsage = true

	rootCmd.PersistentFlags().StringVar(&logLevelFlag, "log-level", "info", "Log level")
	rootCmd.PersistentFlags().BoolVar(&logJSON, "log-json", false, "Output log in JSON format")
	rootCmd.PersistentFlags().BoolVar(&logNoColor, "no-color", false, "Disable log color")

	rootCmd.Flags().StringVar(&port, "port", "2242", "Port number")
	rootCmd.Flags().String("source", "", "MongoDB connection string for the source")
	rootCmd.Flags().String("target", "", "MongoDB connection string for the target")

	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Get the status of the replication process",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return NewClient(port).Status(cmd.Context())
		},
	}

	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start Cluster Replication",
		RunE: func(cmd *cobra.Command, _ []string) error {
			pauseOnInitialSync, err := cmd.Flags().GetBool("pause-on-initial-sync")
			if err != nil {
				return err //nolint:wrapcheck
			}

			startOptions := startRequest{
				PauseOnInitialSync: pauseOnInitialSync,
			}

			return NewClient(port).Start(cmd.Context(), startOptions)
		},
	}

	startCmd.Flags().Bool("pause-on-initial-sync", false, "Pause on Initial Sync")
	_ = startCmd.Flags().MarkHidden("pause-on-initial-sync")

	finalizeCmd := &cobra.Command{
		Use:   "finalize",
		Short: "Finalize Cluster Replication",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return NewClient(port).Finalize(cmd.Context())
		},
	}

	pauseCmd := &cobra.Command{
		Use:   "pause",
		Short: "Pause Cluster Replication",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return NewClient(port).Pause(cmd.Context())
		},
	}

	resumeCmd := &cobra.Command{
		Use:   "resume",
		Short: "Resume Cluster Replication",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return NewClient(port).Resume(cmd.Context())
		},
	}

	rootCmd.AddCommand(statusCmd, startCmd, finalizeCmd, pauseCmd, resumeCmd)

	err := rootCmd.Execute()
	if err != nil {
		zerolog.Ctx(context.Background()).Fatal().Err(err).Msg("")
	}
}

// runServer starts the HTTP server with the provided configuration.
func runServer(ctx context.Context, port, sourceURI, targetURI string) error {
	switch {
	case sourceURI == "" && targetURI == "":
		return errors.New("source URI and target URI are empty")
	case sourceURI == "":
		return errors.New("source URI is empty")
	case targetURI == "":
		return errors.New("target URI is empty")
	case sourceURI == targetURI:
		return errors.New("source URI and target URI are identical")
	}

	addr, err := buildServerAddr(port)
	if err != nil {
		return errors.Wrap(err, "build server address")
	}

	srv, err := createServer(ctx, sourceURI, targetURI)
	if err != nil {
		return errors.Wrap(err, "new server")
	}

	httpServer := http.Server{
		Addr:    addr,
		Handler: srv.Handler(),

		ReadTimeout:       ServerReadTimeout,
		ReadHeaderTimeout: ServerReadHeaderTimeout,
	}

	log.Ctx(ctx).Info("Starting server at http://" + addr)

	err = httpServer.ListenAndServe()
	if err != nil {
		return errors.Wrap(err, "listen")
	}

	if err := srv.Close(ctx); err != nil {
		return errors.Wrap(err, "close server")
	}

	return nil
}

var errUnsupportedPortRange = errors.New("port value is outside the supported range [1024 - 65535]")

// buildServerAddr constructs the server address from the port.
func buildServerAddr(port string) (string, error) {
	i, err := strconv.ParseInt(port, 10, 32)
	if err != nil {
		return "", errors.Wrap(err, "invalid port value format")
	}

	if i < 1024 || i > 65535 {
		return "", errUnsupportedPortRange
	}

	return "localhost:" + port, nil
}

// server represents the replication server.
type server struct {
	// sourceCluster is the MongoDB client for the source cluster.
	sourceCluster *mongo.Client
	// targetCluster is the MongoDB client for the target cluster.
	targetCluster *mongo.Client
	// mlink is the MongoLink instance for cluster replication.
	mlink *mongolink.MongoLink
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

		err1 := source.Disconnect(ctx)
		if err1 != nil {
			log.Ctx(ctx).Warn("Disconnect Source Cluster: " + err1.Error())
		}
	}()

	lg.Debug("Connected to source cluster")

	target, err := topo.Connect(ctx, targetURI)
	if err != nil {
		return nil, errors.Wrap(err, "connect to target cluster")
	}

	defer func() {
		if err == nil {
			return
		}

		err1 := target.Disconnect(ctx)
		if err1 != nil {
			log.Ctx(ctx).Warn("Disconnect Target Cluster: " + err1.Error())
		}
	}()

	lg.Debug("Connected to target cluster")

	s := &server{
		sourceCluster: source,
		targetCluster: target,
		mlink:         mongolink.New(source, target),
	}

	return s, nil
}

// Close closes the server connections.
func (s *server) Close(ctx context.Context) error {
	err0 := s.sourceCluster.Disconnect(ctx)
	err1 := s.targetCluster.Disconnect(ctx)

	return errors.Join(err0, err1)
}

// Handler returns the HTTP handler for the server.
func (s *server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/start", s.handleStart)
	mux.HandleFunc("/finalize", s.handleFinalize)
	mux.HandleFunc("/pause", s.handlePause)
	mux.HandleFunc("/resume", s.handleResume)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.New("http").Info(r.Method + " " + r.URL.String())
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

	err := s.mlink.Finalize(ctx)
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
	EstimatedCloneSize *int64 `json:"estimatedCloneSize,omitempty"`
	// ClonedSize is the size of the data that has been cloned.
	ClonedSize int64 `json:"clonedSize"`

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
	port string
}

func NewClient(port string) MongoLinkClient {
	return MongoLinkClient{port: port}
}

// Status sends a request to get the status of the cluster replication.
func (c MongoLinkClient) Status(ctx context.Context) error {
	return doClientRequest[statusResponse](ctx, c.port, http.MethodGet, "status", nil)
}

// Start sends a request to start the cluster replication.
func (c MongoLinkClient) Start(ctx context.Context, startOptions startRequest) error {
	return doClientRequest[startResponse](ctx, c.port, http.MethodPost, "start", startOptions)
}

// Finalize sends a request to finalize the cluster replication.
func (c MongoLinkClient) Finalize(ctx context.Context) error {
	return doClientRequest[finalizeResponse](ctx, c.port, http.MethodPost, "finalize", nil)
}

// Pause sends a request to pause the cluster replication.
func (c MongoLinkClient) Pause(ctx context.Context) error {
	return doClientRequest[pauseResponse](ctx, c.port, http.MethodPost, "pause", nil)
}

// Resume sends a request to resume the cluster replication.
func (c MongoLinkClient) Resume(ctx context.Context) error {
	return doClientRequest[resumeResponse](ctx, c.port, http.MethodPost, "resume", nil)
}

func doClientRequest[T any](ctx context.Context, port, method, path string, options any) error {
	url := fmt.Sprintf("http://localhost:%s/%s", port, path)

	data := []byte("")
	if options != nil {
		var err error
		data, err = json.Marshal(options)
		if err != nil {
			return errors.Wrap(err, "encode request")
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(data))
	if err != nil {
		return errors.Wrap(err, "build request")
	}

	log.Ctx(ctx).Debugf("POST /%s %s", path, string(data))

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
