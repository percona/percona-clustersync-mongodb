package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/hlog"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/mongolink"
	"github.com/percona-lab/percona-mongolink/topo"
)

func main() {
	var port, sourceURI, targetURI string
	var logLevelFlag string
	var logNoColor bool

	flag.StringVar(&port, "port", "2242", "port")
	flag.StringVar(&sourceURI, "source", "", "MongoDB connection string")
	flag.StringVar(&targetURI, "target", "", "MongoDB connection string")
	flag.StringVar(&logLevelFlag, "log-level", "info", "log level")
	flag.BoolVar(&logNoColor, "no-color", false, "disable log color")
	flag.Parse()

	logLevel, err := zerolog.ParseLevel(logLevelFlag)
	if err != nil {
		log.New(0, true).Fatal().Timestamp().Err(err).Msg("parse log level")
	}

	l := log.New(logLevel, logNoColor)
	log.SetFallbackLogger(l)
	ctx := l.WithContext(context.Background())

	command := flag.Arg(0)
	switch command {
	case "start":
		err = requestStart(ctx, port)
	case "finalize":
		err = requestFinalize(ctx, port)
	case "status":
		err = requestStatus(ctx, port)
	case "":
		err = runServer(ctx, port, sourceURI, targetURI)
	default:
		err = errors.New("unknown command")
	}

	if err != nil {
		l.Fatal().Timestamp().Err(err).Msg("")
	}
}

func runServer(ctx context.Context, port, sourceURI, targetURI string) error {
	switch {
	case sourceURI == "" && targetURI == "":
		return errors.New("source uri and target uri are empty")
	case sourceURI == "":
		return errors.New("source uri is empty")
	case targetURI == "":
		return errors.New("target uri is empty")
	case sourceURI == targetURI:
		return errors.New("source uri and target uri are identical")
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

		ReadTimeout:       30 * time.Second,
		ReadHeaderTimeout: 3 * time.Second,
	}

	log.Info(ctx, "starting server at http://"+addr)
	err = httpServer.ListenAndServe()
	if err != nil {
		return errors.Wrap(err, "listen")
	}

	if err := srv.Close(ctx); err != nil {
		return errors.Wrap(err, "close server")
	}
	return nil
}

var errUnsupportedPortRange = errors.New("port value is outside supported range [1024 - 65535]")

// buildServerAddr builds the server address from the port.
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
	sourceCluster *mongo.Client
	targetCluster *mongo.Client

	repl *mongolink.MongoLink
}

// newServer creates a new server with the given options.
func newServer(ctx context.Context, sourceURI, targetURI string) (*server, error) {
	source, err := topo.Connect(ctx, sourceURI)
	if err != nil {
		return nil, errors.Wrap(err, "connect to source cluster")
	}
	log.Debug(ctx, "connected to source cluster")

	target, err := topo.Connect(ctx, targetURI)
	if err != nil {
		return nil, errors.Wrap(err, "connect to target cluster")
	}
	log.Debug(ctx, "connected to target cluster")

	s := &server{
		sourceCluster: source,
		targetCluster: target,
		repl:          mongolink.New(source, target),
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

	logAccess := hlog.AccessHandler(
		func(r *http.Request, status, size int, duration time.Duration) {
			hlog.FromRequest(r).Info().
				Timestamp().
				Str("method", r.Method).
				Stringer("url", r.URL).
				Int("status", status).
				Msg("")
		})

	return logAccess(mux)
}

// handleStart handles the /start endpoint.
func (s *server) handleStart(w http.ResponseWriter, r *http.Request) {
	ctx := log.WithAttrs(r.Context(), log.Scope("/start"))

	var params startRequest
	err := json.NewDecoder(r.Body).Decode(&params)
	if err != nil {
		log.Error(ctx, err, "decode request body")
		err := json.NewEncoder(w).Encode(startReponse{Error: err.Error()})
		if err != nil {
			log.Error(ctx, err, "write status")
			internalServerError(w)
		}
		return
	}

	options := &mongolink.StartOptions{
		DropBeforeCreate: true,
		// TODO: uncomment when tests will be added
		// DropBeforeCreate: params.DropBeforeCreate,
		IncludeNamespaces: params.IncludeNamespaces,
		ExcludeNamespaces: params.ExcludeNamespaces,
	}
	err = s.repl.Start(ctx, options)
	if err != nil {
		log.Error(ctx, err, "start replication")
		err := json.NewEncoder(w).Encode(startReponse{Error: err.Error()})
		if err != nil {
			log.Error(ctx, err, "write status")
			internalServerError(w)
		}
		return
	}

	err = json.NewEncoder(w).Encode(startReponse{Ok: true})
	if err != nil {
		log.Error(ctx, err, "write status")
		internalServerError(w)
	}
}

// handleFinalize handles the /finalize endpoint.
func (s *server) handleFinalize(w http.ResponseWriter, r *http.Request) {
	ctx := log.WithAttrs(r.Context(), log.Scope("/finalize"))

	err := s.repl.Finalize(ctx)
	if err != nil {
		log.Error(ctx, err, "finalize replication")
		err := json.NewEncoder(w).Encode(finalizeReponse{Error: err.Error()})
		if err != nil {
			log.Error(ctx, err, "write status")
			internalServerError(w)
		}
		return
	}

	err = json.NewEncoder(w).Encode(finalizeReponse{Ok: true})
	if err != nil {
		log.Error(ctx, err, "write status")
		internalServerError(w)
	}
}

// handleStatus handles the /status endpoint.
func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx := log.WithAttrs(r.Context(), log.Scope("/status"))

	replStatus, err := s.repl.Status(ctx)
	if err != nil {
		log.Error(ctx, err, "replication status")
		err := json.NewEncoder(w).Encode(statusResponse{Error: err.Error()})
		if err != nil {
			log.Error(ctx, err, "write status")
			internalServerError(w)
		}
		return
	}

	res := statusResponse{
		Ok:              true,
		State:           replStatus.State,
		Finalizable:     replStatus.Finalizable,
		Info:            replStatus.Info,
		EventsProcessed: replStatus.EventsProcessed,
	}

	if replStatus.State != mongolink.IdleState {
		res.Clone = &CloneStatus{
			Finished:             replStatus.Clone.Finished,
			EstimatedTotalBytes:  replStatus.Clone.EstimatedTotalBytes,
			EstimatedClonedBytes: replStatus.Clone.EstimatedClonedBytes,
		}

		if replStatus.Clone.Finished && !replStatus.LastAppliedOpTime.IsZero() {
			res.LastAppliedOpTime = fmt.Sprintf("%d.%d",
				replStatus.LastAppliedOpTime.T,
				replStatus.LastAppliedOpTime.I)
		}
	}

	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		log.Error(ctx, err, "write status")
		internalServerError(w)
	}
}

// startRequest represents the request body for the /start endpoint.
type startRequest struct {
	// TODO: uncomment when tests will be added
	// DropBeforeCreate bool `json:"dropBeforeCreateCollection"`
	IncludeNamespaces []string `json:"includeNamespaces,omitempty"`
	ExcludeNamespaces []string `json:"excludeNamespaces,omitempty"`
}

// startReponse represents the response body for the /start endpoint.
type startReponse struct {
	Ok    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// finalizeReponse represents the response body for the /finalize endpoint.
type finalizeReponse struct {
	Ok    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// CloneStatus represents the status of the cloning process.
type CloneStatus struct {
	Finished             bool  `json:"finished,omitempty"`
	EstimatedTotalBytes  int64 `json:"estimatedTotalBytes,omitempty"`
	EstimatedClonedBytes int64 `json:"estimatedClonedBytes,omitempty"`
}

// statusResponse represents the response body for the /status endpoint.
type statusResponse struct {
	Ok    bool   `json:"ok"`
	Error string `json:"error,omitempty"`

	State             mongolink.State `json:"state"`
	Finalizable       bool            `json:"finalizable,omitempty"`
	LastAppliedOpTime string          `json:"lastAppliedOpTime,omitempty"`
	Info              string          `json:"info,omitempty"`
	EventsProcessed   int64           `json:"eventsProcessed,omitempty"`

	Clone *CloneStatus `json:"clone,omitempty"`
}

// internalServerError sends an internal server error response.
func internalServerError(w http.ResponseWriter) {
	http.Error(w,
		http.StatusText(http.StatusInternalServerError),
		http.StatusInternalServerError)
}

func requestStart(ctx context.Context, port string) error {
	url := fmt.Sprintf("http://localhost:%s/start", port)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader("{}"))
	if err != nil {
		return errors.Wrap(err, "build request")
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "request")
	}
	defer res.Body.Close()

	var resp startReponse
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

func requestFinalize(ctx context.Context, port string) error {
	url := fmt.Sprintf("http://localhost:%s/finalize", port)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader("{}"))
	if err != nil {
		return errors.Wrap(err, "build request")
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "request")
	}
	defer res.Body.Close()

	var resp finalizeReponse
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

func requestStatus(ctx context.Context, port string) error {
	url := fmt.Sprintf("http://localhost:%s/status", port)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return errors.Wrap(err, "build request")
	}

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
