# prometheus-tsdb-parquet
Export Prometheus TSDB files into parquet files

## Build
run `make` / `go build -o bin/prometheus-tsdb-dump .`

## Usage
Grab prometheus chunk from prometheus data directory.  
example directory structure:
```
./chunks/000001
./chunks/000002
./chunks/000003
./index
./meta.json
./tombstones
```
Once you copied a prometheus chunk, run the following:
```
bin/prometheus-tsdb-dump \
    --block <path_to_chunk_dir> 
    --output <destination_directory>
    --shard-size <optional: Max number of series in a signle parquet file (default 3_000_000)
```

Parquet Schema
```go
type Line struct {
	Labels          map[string]string
	NumberOfSamples int              
	Hash            string           
	MetricName      string           
	MinTimestamp    int64            
	MaxTimestamp    int64            
	MinValue        float64          
	MaxValue        float64          
	Size            int              
}
```

## Credits
Inspired by https://github.com/ryotarai/prometheus-tsdb-dump
