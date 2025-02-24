package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/rs/zerolog"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/mongolink"
	"github.com/percona-lab/percona-mongolink/topo"
)

const (
	ServerReadTimeout       = 30 * time.Second
	ServerReadHeaderTimeout = 3 * time.Second
	MaxRequestSize          = 1024 // 1KB
	ServerResponseTimeout   = 5 * time.Second
)

func main() {
	var (
		port         string
		sourceURI    string
		targetURI    string
		logLevelFlag string
		logJSON      bool
		logNoColor   bool
	)

	flag.StringVar(&port, "port", "2242", "port")
	flag.StringVar(&sourceURI, "source", "", "MongoDB connection string")
	flag.StringVar(&targetURI, "target", "", "MongoDB connection string")
	flag.StringVar(&logLevelFlag, "log-level", "info", "log level")
	flag.BoolVar(&logJSON, "log-json", false, "output log in JSON")
	flag.BoolVar(&logNoColor, "no-color", false, "disable log color")
	flag.Parse()

	logLevel, err := zerolog.ParseLevel(logLevelFlag)
	if err != nil {
		log.InitGlobals(0, logJSON, true).Fatal().Msg("unknown log level")
	}

	lg := log.InitGlobals(logLevel, logJSON, logNoColor)
	ctx := lg.WithContext(context.Background())

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
		lg.Fatal().Err(err).Msg("")
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

		ReadTimeout:       ServerReadTimeout,
		ReadHeaderTimeout: ServerReadHeaderTimeout,
	}

	log.Ctx(ctx).Info("starting server at http://" + addr)

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

	mlink *mongolink.MongoLink
}

// newServer creates a new server with the given options.
func newServer(ctx context.Context, sourceURI, targetURI string) (*server, error) {
	lg := log.Ctx(ctx)

	source, err := topo.Connect(ctx, sourceURI)
	if err != nil {
		return nil, errors.Wrap(err, "connect to source cluster")
	}

	lg.Debug("connected to source cluster")

	target, err := topo.Connect(ctx, targetURI)
	if err != nil {
		return nil, errors.Wrap(err, "connect to target cluster")
	}

	lg.Debug("connected to target cluster")

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
		IncludeNamespaces: params.IncludeNamespaces,
		ExcludeNamespaces: params.ExcludeNamespaces,
	}

	err = s.mlink.Start(ctx, options)
	if err != nil {
		writeResponse(w, startReponse{Error: err.Error()})

		return
	}

	writeResponse(w, startReponse{Ok: true})
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
		writeResponse(w, startReponse{Error: err.Error()})

		return
	}

	writeResponse(w, finalizeReponse{Ok: true})
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

	replStatus, err := s.mlink.Status(ctx)
	if err != nil {
		writeResponse(w, statusResponse{Error: err.Error()})

		return
	}

	errorMessage := ""
	if replStatus.Clone.Error != nil {
		errorMessage = replStatus.Clone.Error.Error()
	} else if replStatus.Error != nil {
		errorMessage = replStatus.Error.Error()
	}

	res := statusResponse{
		Ok:              replStatus.Error == nil,
		Error:           errorMessage,
		State:           replStatus.State,
		Finalizable:     replStatus.Finalizable,
		Info:            replStatus.Info,
		EventsProcessed: replStatus.EventsProcessed,
	}

	if replStatus.State != mongolink.StateIdle {
		res.Clone = &CloneStatus{
			EstimatedTotalBytes:  replStatus.Clone.EstimatedTotalBytes,
			EstimatedClonedBytes: replStatus.Clone.EstimatedClonedBytes,
			Finished:             replStatus.Clone.Finished,
		}

		if replStatus.Clone.Finished && !replStatus.LastAppliedOpTime.IsZero() {
			res.LastAppliedOpTime = fmt.Sprintf("%d.%d",
				replStatus.LastAppliedOpTime.T,
				replStatus.LastAppliedOpTime.I)
		}
	}

	log.New("http:status").Unwrap().
		Trace().
		Bool("ok", res.Ok).
		Err(replStatus.Error).
		Str("state", string(res.State)).
		Send()
	writeResponse(w, res)
}

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

// requestStart sends a request to start the replication process.
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

// requestFinalize sends a request to finalize the replication process.
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

// requestStatus sends a request to get the status of the replication process.
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
