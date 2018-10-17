# vcs2csv

``vcs2csv`` is a small program that writes selected VCS buckets to gzipped CSV files for persistent storage.

## Usage

The process is started with some arguments to specify host, port, keys to include (whitespace separated) and the output directory for the CSV files:

```bash
vcs2csv --listen-host 127.0.0.1 --listen-port 6556 --keys "ALL HOST/example.com" --directory /var/lib/vcs2csv/
```

VCS must then be configured to connect and stream JSON buckets to the same port:

```bash
vcs -O 127.0.0.1:6556
```

Buckets will be written to ``/var/lib/vcs2csv/YYYY-MM-DD-Key.csv.gz``. The *Key* will be url encoded.
