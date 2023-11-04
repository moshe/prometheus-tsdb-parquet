package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"

	gokitlog "github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

func main() {
	blockPath := flag.String("block", "", "Path to block directory")
	outputPath := flag.String("output", "", "Path to output parquet directory")
	shardSize := flag.Int("shard-size", 3_000_000, "Maximum number of series in a single parquet file")
	flag.Parse()

	if *blockPath == "" {
		log.Fatal("-block argument is required")
	}

	if *outputPath == "" {
		log.Fatal("-output argument is required")
	}

	if err := run(*blockPath, *outputPath, *shardSize); err != nil {
		log.Fatalf("error: %s", err)
	}
}

func run(blockPath string, outputPath string, shardSize int) error {

	wr, err := NewParquetWriter(outputPath, shardSize)
	defer wr.Close()

	logger := gokitlog.NewLogfmtLogger(os.Stderr)

	block, err := tsdb.OpenBlock(logger, blockPath, chunkenc.NewPool())
	if err != nil {
		return errors.Wrap(err, "tsdb.OpenBlock")
	}

	indexr, err := block.Index()
	if err != nil {
		return errors.Wrap(err, "block.Index")
	}
	defer indexr.Close()

	chunkr, err := block.Chunks()
	if err != nil {
		return errors.Wrap(err, "block.Chunks")
	}
	defer chunkr.Close()

	postings, err := indexr.Postings("", "")
	if err != nil {
		return errors.Wrap(err, "indexr.Postings")
	}

	var it chunkenc.Iterator
	for postings.Next() {
		ref := postings.At()
		lset := labels.Labels{}
		chks := []chunks.Meta{}
		if err := indexr.Series(ref, &lset, &chks); err != nil {
			return errors.Wrap(err, "indexr.Series")
		}

		for _, meta := range chks {
			chunk, err := chunkr.Chunk(meta.Ref)
			if err != nil {
				return errors.Wrap(err, "chunkr.Chunk")
			}

			var numberOfSamples = 0
			var maxTimestamp int64 = 0
			var minTimestamp int64 = 0
			var minValue float64 = 0
			var maxValue float64 = 0

			metric := map[string]string{}
			for _, l := range lset {
				metric[l.Name] = l.Value
			}

			it := chunk.Iterator(it)
			for it.Next() {
				t, v := it.At()
				if math.IsNaN(v) {
					continue
				}
				if math.IsInf(v, -1) || math.IsInf(v, 1) {
					continue
				}
				maxValue = math.Max(v, maxValue)
				minValue = math.Min(v, minValue)

				if t > maxTimestamp {
					maxTimestamp = t
				}
				if t < minTimestamp {
					minTimestamp = t
				}
				numberOfSamples += 1
			}
			if it.Err() != nil {
				return errors.Wrap(err, "iterator.Err")
			}

			if err := wr.Write(&Line{
				Labels:          metric,
				NumberOfSamples: numberOfSamples,
				MetricName:      metric["__name__"],
				MinTimestamp:    minTimestamp,
				MaxTimestamp:    maxTimestamp,
				MinValue:        minValue,
				MaxValue:        maxValue,
			}); err != nil {
				return errors.Wrap(err, fmt.Sprintf("Writer.Write(%v)", metric))
			}
		}
	}

	if postings.Err() != nil {
		return errors.Wrap(err, "postings.Err")
	}

	return nil
}
