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
	"strings"
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
	MaxRequestSize          = config.KiB
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

	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start the replication process",
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

	finalizeCmd := &cobra.Command{
		Use:   "finalize",
		Short: "Finalize the replication process",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return NewClient(port).Finalize(cmd.Context())
		},
	}
	finalizeCmd.Flags().Bool("pause-on-initial-sync", false, "Pause on Initial Sync")

	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Get the status of the replication process",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return NewClient(port).Status(cmd.Context())
		},
	}

	rootCmd.AddCommand(startCmd, finalizeCmd, statusCmd)

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

	srv, err := newServer(ctx, sourceURI, targetURI)
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
	// mlink is the MongoLink instance for replication.
	mlink *mongolink.MongoLink
}

// newServer creates a new server with the given options.
func newServer(ctx context.Context, sourceURI, targetURI string) (*server, error) {
	lg := log.Ctx(ctx)

	source, err := topo.Connect(ctx, sourceURI)
	if err != nil {
		return nil, errors.Wrap(err, "connect to source cluster")
	}

	lg.Debug("Connected to source cluster")

	target, err := topo.Connect(ctx, targetURI)
	if err != nil {
		return nil, errors.Wrap(err, "connect to target cluster")
	}

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

	mux.HandleFunc("/start", s.handleStart)
	mux.HandleFunc("/finalize", s.handleFinalize)
	mux.HandleFunc("/status", s.handleStatus)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.New("http").Info(r.Method + " " + r.URL.String())
		mux.ServeHTTP(w, r)
	})
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

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w,
			http.StatusText(http.StatusInternalServerError),
			http.StatusInternalServerError)

		return
	}

	var params startRequest

	err = json.Unmarshal(data, &params)
	if err != nil {
		http.Error(w,
			http.StatusText(http.StatusBadRequest),
			http.StatusBadRequest)

		return
	}

	options := &mongolink.StartOptions{
		PauseOnInitialSync: params.PauseOnInitialSync,
		IncludeNamespaces:  params.IncludeNamespaces,
		ExcludeNamespaces:  params.ExcludeNamespaces,
	}

	err = s.mlink.Start(ctx, options)
	if err != nil {
		writeResponse(w, startResponse{Error: err.Error()})

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
		writeResponse(w, finalizeResponse{Error: err.Error()})

		return
	}

	writeResponse(w, finalizeResponse{Ok: true})
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

	status := s.mlink.Status(ctx)

	res := statusResponse{
		Ok:    status.Error == nil,
		State: status.State,
	}

	if err := status.Error; err != nil {
		res.Error = err.Error()
	}

	if status.State != mongolink.StateIdle {
		res.EventsProcessed = &status.Repl.EventsProcessed
		res.LagTime = status.TotalLagTime

		if !status.Repl.LastReplicatedOpTime.IsZero() {
			res.LastReplicatedOpTime = fmt.Sprintf("%d.%d",
				status.Repl.LastReplicatedOpTime.T,
				status.Repl.LastReplicatedOpTime.I)
		}

		res.InitialSync = &statusInitialSyncResponse{
			PauseOnInitialSync: status.PauseOnInitialSync,
			Completed:          status.InitialSyncCompleted,
			LagTime:            status.InitialSyncLagTime,
		}

		res.InitialSync.CloneCompleted = status.Clone.Completed
		res.InitialSync.EstimatedCloneSize = &status.Clone.EstimatedTotalSize
		res.InitialSync.ClonedSize = status.Clone.CopiedSize
	}

	switch {
	case status.State == mongolink.StateRunning && !status.Clone.Completed:
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

// writeResponse writes the response as JSON to the ResponseWriter.
func writeResponse[T any](w http.ResponseWriter, resp T) {
	err := json.NewEncoder(w).Encode(resp)
	if err != nil {
		http.Error(w,
			http.StatusText(http.StatusInternalServerError),
			http.StatusInternalServerError)

		return
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
	// Error is the error message if the operation failed.
	Error string `json:"error,omitempty"`
}

// finalizeResponse represents the response body for the /finalize endpoint.
type finalizeResponse struct {
	// Ok indicates if the operation was successful.
	Ok bool `json:"ok"`
	// Error is the error message if the operation failed.
	Error string `json:"error,omitempty"`
}

// statusResponse represents the response body for the /status endpoint.
type statusResponse struct {
	// Ok indicates if the operation was successful.
	Ok bool `json:"ok"`
	// State is the current state of the replication.
	State mongolink.State `json:"state"`
	// Info provides additional information about the current state.
	Info string `json:"info,omitempty"`
	// Error is the error message if the operation failed.
	Error string `json:"error,omitempty"`

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
	// PauseOnInitialSync indicates if the replication is paused on initial sync.
	PauseOnInitialSync bool `json:"pauseOnInitialSync,omitempty"`

	// Completed indicates if the initial sync is completed.
	Completed bool `json:"completed"`
	// LagTime is the lag time in logical seconds until the initial sync completed.
	LagTime *int64 `json:"lagTime,omitempty"`

	// CloneCompleted indicates if the cloning process is completed.
	CloneCompleted bool `json:"cloneCompleted"`
	// EstimatedCloneSize is the estimated total size of the clone.
	EstimatedCloneSize *int64 `json:"estimatedCloneSize,omitempty"`
	// ClonedSize is the size of the data that has been cloned.
	ClonedSize int64 `json:"clonedSize"`
}

type Client struct {
	port string
}

func NewClient(port string) Client {
	return Client{port: port}
}

// Start sends a request to start the replication process.
func (c Client) Start(ctx context.Context, startOptions startRequest) error {
	url := fmt.Sprintf("http://localhost:%s/start", c.port)

	payload, err := json.MarshalIndent(startOptions, "", "  ")
	if err != nil {
		return errors.Wrap(err, "marshal request")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return errors.Wrap(err, "build request")
	}

	log.Ctx(ctx).Debug("POST /start\n" + string(payload))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "request")
	}
	defer res.Body.Close()

	var resp startResponse

	err = json.NewDecoder(res.Body).Decode(&resp)
	if err != nil {
		return errors.Wrap(err, "decode response")
	}

	if !resp.Ok {
		return errors.New(resp.Error)
	}

	j := json.NewEncoder(os.Stdout)
	j.SetIndent("", "  ")
	err = j.Encode(resp)

	return errors.Wrap(err, "print response")
}

// Finalize sends a request to finalize the replication process.
func (c Client) Finalize(ctx context.Context) error {
	url := fmt.Sprintf("http://localhost:%s/finalize", c.port)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader("{}"))
	if err != nil {
		return errors.Wrap(err, "build request")
	}

	log.Ctx(ctx).Debug("POST /finalize {}")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "request")
	}
	defer res.Body.Close()

	var resp finalizeResponse

	err = json.NewDecoder(res.Body).Decode(&resp)
	if err != nil {
		return errors.Wrap(err, "decode response")
	}

	if !resp.Ok {
		return errors.New(resp.Error)
	}

	j := json.NewEncoder(os.Stdout)
	j.SetIndent("", "  ")
	err = j.Encode(resp)

	return errors.Wrap(err, "print response")
}

// Status sends a request to get the status of the replication process.
func (c Client) Status(ctx context.Context) error {
	url := fmt.Sprintf("http://localhost:%s/status", c.port)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return errors.Wrap(err, "build request")
	}

	log.Ctx(ctx).Debug("GET /status")

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

	if !resp.Ok {
		return errors.New(resp.Error)
	}

	j := json.NewEncoder(os.Stdout)
	j.SetIndent("", "  ")
	err = j.Encode(resp)

	return errors.Wrap(err, "print response")
}
