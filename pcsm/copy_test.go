package pcsm_test

import (
	"container/list"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/dustin/go-humanize"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-clustersync-mongodb/config"
	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/pcsm"
	"github.com/percona/percona-clustersync-mongodb/topo"
)

func getNamespace() pcsm.Namespace {
	s := os.Getenv("PCSM_TEST_NAMESPACE")
	if s == "" {
		panic("PCSM_TEST_NAMESPACE is empty")
	}

	db, coll, _ := strings.Cut(s, ".")
	if db == "" || coll == "" {
		panic("PCSM_TEST_NAMESPACE contains invalid namespace")
	}

	return pcsm.Namespace{Database: db, Collection: coll}
}

func getSourceURI() string {
	s := os.Getenv("PCSM_SOURCE_URI")
	if s == "" {
		panic("PCSM_SOURCE_URI is empty")
	}

	return s
}

func getTargetURI() string {
	s := os.Getenv("PCSM_TARGET_URI")
	if s == "" {
		panic("PCSM_TARGET_URI is empty")
	}

	return s
}

func getNumInsertWorker() int {
	num, err := strconv.Atoi(os.Getenv("PCSM_TEST_NUM_INSERT_WORKER"))
	if err != nil {
		panic(fmt.Sprintf("PCSM_TEST_NUM_INSERT_WORKER: %v", err))
	}

	if num < 1 {
		num = runtime.NumCPU()
	}

	return num
}

func getReadBatchSize() int {
	size, err := humanize.ParseBytes(os.Getenv("PCSM_TEST_READ_BACTH_SIZE"))
	if err != nil {
		panic(fmt.Sprintf("PCSM_TEST_READ_BACTH_SIZE: %v", err))
	}

	size = max(size, 1)

	return int(size) //nolint:gosec
}

func getInsertBatchSize() int {
	size, err := humanize.ParseBytes(os.Getenv("PCSM_TEST_INSERT_BACTH_SIZE"))
	if err != nil {
		panic(fmt.Sprintf("PCSM_TEST_INSERT_BACTH_SIZE: %v", err))
	}

	size = max(size, 1)

	return int(size) //nolint:gosec
}

func BenchmarkRead(b *testing.B) {
	ctx := b.Context()

	ns := getNamespace()
	mc, err := topo.Connect(ctx, getSourceURI(), &config.Config{})
	if err != nil {
		b.Fatal(err)
	}
	defer mc.Disconnect(ctx) //nolint:errcheck

	stats, _ := topo.GetCollStats(ctx, mc, ns.Database, ns.Collection)
	b.Logf("read size %s\n", humanize.Bytes(uint64(stats.Size))) //nolint:gosec

	if stats.AvgObjSize == 0 {
		b.Fatal("zero AvgObjSize")
	}

	file, err := os.Create(ns.String() + ".bson")
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()

	mcoll := mc.Database(ns.Database).Collection(ns.Collection)
	//nolint:gosec
	cur, err := mcoll.Find(ctx, bson.D{}, options.Find().SetBatchSize(int32(getReadBatchSize())))
	if err != nil {
		b.Fatal(err)
	}
	defer cur.Close(ctx)

	b.ResetTimer()

	for cur.Next(ctx) {
		n, err := file.Write(cur.Current)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(cur.Current) {
			b.Fatal(io.ErrShortWrite)
		}
	}

	err = cur.Err()
	if err != nil {
		b.Fatal(err)
	}

	err = file.Close()
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkInsert(b *testing.B) {
	ctx := b.Context()

	ns := getNamespace()
	mc, err := topo.Connect(ctx, getTargetURI(), &config.Config{})
	if err != nil {
		b.Fatal(err)
	}
	defer mc.Disconnect(ctx) //nolint:errcheck

	file, err := os.Open(ns.String() + ".bson")
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()

	stats, _ := file.Stat()
	b.Logf("insert size %s\n", humanize.Bytes(uint64(stats.Size()))) //nolint:gosec

	mcoll := mc.Database(ns.Database).Collection(ns.Collection)
	mcoll.Drop(ctx) //nolint:errcheck

	insertOptions := options.InsertMany().SetBypassDocumentValidation(true).SetOrdered(false)

	type task struct {
		docs []any
		size int
	}

	batchSize := getInsertBatchSize()

	all := list.New()
	totalSize := 0
	size := 0
	docs := make([]any, 0, batchSize)

	for {
		raw, err := bson.ReadDocument(file)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			b.Fatal(err)
		}

		if size+len(raw) > config.MaxWriteBatchSizeBytes || len(docs) >= batchSize {
			all.PushBack(task{docs, size})
			totalSize += size

			docs = make([]any, 0, batchSize)
			size = 0
		}

		docs = append(docs, raw)
		size += len(raw)
	}

	file.Close()

	concurrency := getNumInsertWorker()
	docCh := make(chan task, concurrency)
	grp, grpCtx := errgroup.WithContext(ctx)

	for range concurrency {
		grp.Go(func() error {
			mcoll := mc.Database(ns.Database).Collection(ns.Collection)

			for t := range docCh {
				_, err = mcoll.InsertMany(grpCtx, t.docs, insertOptions)
				if err != nil {
					if mongo.IsDuplicateKeyError(err) {
						continue
					}

					return err //nolint:wrapcheck
				}
			}

			return nil
		})
	}

	b.ResetTimer()

	var next *list.Element
	for el := all.Front(); el != nil; el = next {
		docCh <- el.Value.(task) //nolint:forcetypeassert
		next = el.Next()
		all.Remove(el)
	}
	close(docCh)

	err = grp.Wait()
	if err != nil {
		b.Fatal(err)
	}
}
