# vcs2csv

``vcs2csv`` is a small program that writes selected VCS buckets to gzipped CSV files for persistent storage.

## Get

```bash
go get github.com/varnish/vcs2csv
```

The binary will be installed to ``$GOPATH/bin/vcs2csv``.

### Cross compile to Linux

```bash
cd $GOPATH/src/github/varnish/vcs2csv
GOOS=linux GOARCH=amd64 go build
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

The following is an example with regular expression matching on the keys:

```bash
vcs2csv --listen-host 127.0.0.1 --listen-port 6556 --key-patterns "^COUNTRY/.* ^METRICS" --directory /var/lib/vcs2csv/
```

## CSV format

* Column  0: ``timestamp`` (int, epoch),
* Column  1: ``n_requests`` (int),
* Column  2: ``n_req_uniq`` (int),
* Column  3: ``n_misses`` (int),
* Column  4: ``n_restarts`` (int),
* Column  5: ``ttfb_miss`` (float),
* Column  6: ``ttfb_hit`` (float),
* Column  7: ``n_bodybytes`` (int),
* Column  8: ``respbytes`` (int),
* Column  9: ``reqbytes`` (int),
* Column 10: ``bereqbytes`` (int),
* Column 11: ``berespbytes`` (int),
* Column 12: ``resp_code_1xx`` (int),
* Column 13: ``resp_code_2xx`` (int),
* Column 14: ``resp_code_3xx`` (int),
* Column 15: ``resp_code_4xx`` (int),
* Column 16: ``resp_code_5xx`` (int)

