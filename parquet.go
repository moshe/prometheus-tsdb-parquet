package main

import (
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"log"
	"strconv"
	"time"
)

type ParquetWriter struct {
	Writer     *writer.ParquetWriter
	counter    int
	startTime  int64
	shard      int
	outputPath string
	shardSize  int
}

func (w *ParquetWriter) Close() {
	err := w.Writer.WriteStop()
	if err != nil {
		return
	}
	err = w.Writer.PFile.Close()
	if err != nil {
		return
	}

}

func getFilename(outputPath string, shard int) string {
	return outputPath + "part" + strconv.Itoa(shard) + ".parquet"
}

func (w *ParquetWriter) openWriter() error {
	shard := w.counter / w.shardSize
	if w.shard != shard {
		if w.shard != -1 {
			w.Close()
		}
		w.shard = shard

		file := getFilename(w.outputPath, shard)
		log.Println("New filename!", file)
		fw, err := local.NewLocalFileWriter(file)
		if err != nil {
			return err
		}
		wrt, err := writer.NewParquetWriter(fw, new(Line), 4)
		wrt.RowGroupSize = 256 * 1024 * 1024
		wrt.PageSize = 8 * 1024
		wrt.CompressionType = parquet.CompressionCodec_SNAPPY
		w.Writer = wrt
	}
	return nil
}

func NewParquetWriter(outputPath string, shardSize int) (*ParquetWriter, error) {
	return &ParquetWriter{
			counter:    0,
			startTime:  time.Now().Unix(),
			shard:      -1,
			outputPath: outputPath,
			shardSize:  shardSize,
		},
		nil
}

type Line struct {
	Labels          map[string]string `parquet:"name=Labels, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY"`
	NumberOfSamples int               `parquet:"name=NumberOfSamples, type=INT64"`
	Hash            string            `parquet:"name=Hash, type=BYTE_ARRAY"`
	MetricName      string            `parquet:"name=MetricName, type=BYTE_ARRAY"`
	MinTimestamp    int64             `parquet:"name=MinTimestamp, type=INT64"`
	MaxTimestamp    int64             `parquet:"name=MaxTimestamp, type=INT64"`
	MinValue        float64           `parquet:"name=MinValue, type=FLOAT"`
	MaxValue        float64           `parquet:"name=MaxValue, type=FLOAT"`
	Size            int               `parquet:"name=Size, type=INT64"`
}

func (w *ParquetWriter) Write(line *Line) error {
	w.counter += 1
	err := w.openWriter()
	if err != nil {
		return err
	}
	if w.counter%100_000 == 0 {
		now := time.Now().Unix()
		log.Print(w.counter, " ->", float64(w.counter)/float64(now-w.startTime), " rps")
	}

	err = w.Writer.Write(line)
	if err != nil {
		return err
	}

	return nil
}
