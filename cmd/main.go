package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/hlog"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
	"github.com/percona-lab/percona-mongolink/repl"
	"github.com/percona-lab/percona-mongolink/topo"
)

func main() {
	srvOpts := serverOptions{}
	var logLevelFlag string
	var logNoColor bool
	flag.StringVar(&srvOpts.Port, "port", "2242", "port")
	flag.StringVar(&srvOpts.SourceURI, "source", "", "MongoDB connection string")
	flag.StringVar(&srvOpts.TargetURI, "target", "", "MongoDB connection string")
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

	err = srvOpts.verify()
	if err != nil {
		l.Fatal().Timestamp().Err(err).Msg("")
	}

	addr, err := buildServerAddr(srvOpts.Port)
	if err != nil {
		l.Fatal().Timestamp().Err(err).Msg("build server address")
	}

	srv, err := newServer(ctx, srvOpts)
	if err != nil {
		l.Fatal().Timestamp().Err(err).Msg("new server")
	}

	httpServer := http.Server{
		Addr:    addr,
		Handler: srv.Handler(),

		ReadTimeout:       30 * time.Second,
		ReadHeaderTimeout: 3 * time.Second,
	}

	l.Info().Timestamp().Msg("starting server at http://" + addr)
	err = httpServer.ListenAndServe()
	if err != nil {
		l.Fatal().Timestamp().Err(err).Msg("listen")
	}

	if err := srv.Close(ctx); err != nil {
		l.Fatal().Timestamp().Err(err).Msg("close server")
	}
}

type serverOptions struct {
	Port      string
	SourceURI string
	TargetURI string
}

func (o serverOptions) verify() error {
	switch {
	case o.SourceURI == "" && o.TargetURI == "":
		return errors.New("source uri and target uri are empty")
	case o.SourceURI == "":
		return errors.New("source uri is empty")
	case o.TargetURI == "":
		return errors.New("target uri is empty")
	case o.SourceURI == o.TargetURI:
		return errors.New("source uri and target uri are identical")
	}
	return nil
}

var errUnsupportedPortRange = errors.New("port value is outside supported range [1024 - 65535]")

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

type server struct {
	sourceCluster *mongo.Client
	targetCluster *mongo.Client

	repl *repl.Coordinator
}

func newServer(ctx context.Context, options serverOptions) (*server, error) {
	source, err := topo.Connect(ctx, options.SourceURI)
	if err != nil {
		return nil, errors.Wrap(err, "connect to source cluster")
	}
	log.Debug(ctx, "connected to source cluster")

	target, err := topo.Connect(ctx, options.TargetURI)
	if err != nil {
		return nil, errors.Wrap(err, "connect to target cluster")
	}
	log.Debug(ctx, "connected to target cluster")

	s := &server{
		sourceCluster: source,
		targetCluster: target,
		repl:          repl.New(source, target),
	}
	return s, nil
}

func (s *server) Close(ctx context.Context) error {
	err0 := s.sourceCluster.Disconnect(ctx)
	err1 := s.targetCluster.Disconnect(ctx)
	return errors.Join(err0, err1)
}

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

	options := &repl.StartOptions{
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
		Clone: CloneStatus{
			Finished:             replStatus.Clone.Finished,
			EstimatedTotalBytes:  replStatus.Clone.EstimatedTotalBytes,
			EstimatedClonedBytes: replStatus.Clone.EstimatedClonedBytes,
		},
	}

	if replStatus.Clone.Finished && !replStatus.LastAppliedOpTime.IsZero() {
		res.LastAppliedOpTime = fmt.Sprintf("%d.%d",
			replStatus.LastAppliedOpTime.T,
			replStatus.LastAppliedOpTime.I)
	}

	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		log.Error(ctx, err, "write status")
		internalServerError(w)
	}
}

type startRequest struct {
	// TODO: uncomment when tests will be added
	// DropBeforeCreate bool `json:"dropBeforeCreateCollection"`
	IncludeNamespaces []string `json:"includeNamespaces,omitempty"`
	ExcludeNamespaces []string `json:"excludeNamespaces,omitempty"`
}

type startReponse struct {
	Ok    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type finalizeReponse struct {
	Ok    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type CloneStatus struct {
	Finished             bool  `json:"finished"`
	EstimatedTotalBytes  int64 `json:"estimatedTotalBytes"`
	EstimatedClonedBytes int64 `json:"estimatedClonedBytes"`
}

type statusResponse struct {
	Ok    bool   `json:"ok"`
	Error string `json:"error,omitempty"`

	State             repl.State  `json:"state"`
	Finalizable       bool        `json:"finalizable,omitempty"`
	LastAppliedOpTime string      `json:"lastAppliedOpTime,omitempty"`
	Info              string      `json:"info"`
	EventsProcessed   int64       `json:"eventsProcessed"`
	Clone             CloneStatus `json:"clone"`
}

func internalServerError(w http.ResponseWriter) {
	http.Error(w,
		http.StatusText(http.StatusInternalServerError),
		http.StatusInternalServerError)
}
