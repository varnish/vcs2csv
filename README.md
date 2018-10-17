# vcs2csv

``vcs2csv`` is a small program that writes selected VCS buckets to gzipped CSV files for persistent storage.

## Get

```bash
go get github.com/varnish/vcs2csv
```

## Usage

The process is started with some arguments. The arguments are used to specify host, port, VCS keys to include (whitespace separated list of strings) and the output directory where the CSV files will be stored. In the following example, defaults are used:

```bash
vcs2csv --listen-host 127.0.0.1 --listen-port 6556 --keys "ALL" --directory /var/lib/vcs2csv/
```

VCS must then be configured to connect and stream JSON buckets to the same port:

```bash
vcs -O 127.0.0.1:6556
```

Buckets will be written to ``/var/lib/vcs2csv/YYYY-MM-DD-key.csv.gz``. The *key* will be url encoded.
