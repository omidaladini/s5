#S5

> "Because your data is probably small enough!" -- <cite>Author</cite>

S5 to directly export data from SQL-like databases to Amazon S3. Although
the same could be achieved by gluing existing tools, S5 is built to:

* Be standalone, have no runtime dependencies and be easy to run and deploy.
* Have predictable operational behavior.
* Leave no trace, and be friendly to the health of your systems.

The name _S5_ borrows its name from its previous name _SqlS3Streamer_ which has
five Ss in total.

## Use cases

This tool is made to export _small_ data stored in SQL-like databases to
a compressed, field and record-delimited format, such as TSV or CSV, to Amazon
S3. This is primarily made with Amazon Redshift in mind that imports such formats
natively.

This tool is only useful if your data is _small_ enough that given your database
and network resources, could be exported in _reasonable_ amount of time.

You can export select number of columns from your tables and even apply simple
transformations, as far as your SQL database allows.

## How it works

S5 runs an arbitrary SQL query against a database, streams the result in chunks
and uses Amazon S3's multipart upload API to store the data on S3.

The result-set could be arbitrarily large, such as 10s of GB of data and the
memory consumption will remain constant.

###Output Format

Currently S5 only knows how to serialize records into field/record-delimited
formats such as TSV and CSV.

###Compression

S5 can compress the exported data in a way that the resulting file, stitched together
by Amazon S3 multipart API, forms a valid gzip file.

### Pipelining

S5 works by reading chunks of data and uploading them until all the data is
consumed. In order to speed up the process, reading and uploading happens in
stages; meaning that while a part is being uploaded, the next is being read,
serialized and compressed.

##Running

You can run S5 by supplying the arguments:

    s5  --sql.user=user                               \
        --sql.password=pass                           \
        --sql.database=mydb                           \
        --s3.region us-east-1                         \
        --s3.accesskey S3ACCESSKEY                    \
        --s3.secretkey S3SECRETKEY                    \
        --s3.bucket mydbs                             \
        --s3.path '2015/07/27/foos.gz'                \
        --sql.query="select * from foos"              \
        --compress                                    \
        --chunksizemb 100

For obvious explanation of command line arguments try:

    s5 --help

When a multipart S3 upload fails, the chunks are not automatically removed by
S3. They won't even show in any file hierarchy on S3 but you'll be charged for
them. In order to clean those up, I wrote this little tool which you can schedule
to run at an appropriate time, not interfering with uploads:

    s3multicleanup --s3.region us-east-1              \
                   --s3.accesskey S3ACCESSKEY         \
                   --s3.secretkey S3SECRETKEY         \
                   --s3.bucket mydbs


## Considerations

S5 will pass whatever query you provide to the SQL database. If you run operations
such as JOIN, GROUP BY or DISTINCT etc.. or even DROP TABLE, they'll be
passed directly to your database!

Besides, I suggest avoiding any operation beyond sequential sweeps. There are
myriads of tools our there to carry out the _T_ part of your _ETL_s.

Depending on the storage engine, your query may not result in a consistent
snapshot of your data.

##Development

S5 is written in Go and can be built and tested by golang's standard toolchain:

    go test ./...
    go build ./...

### Future development

Pull requests are welcome.

### TODO
- More comments for packages and some functions.
- Add tests for the S3 package.
- Packaging for Debian.
- Pluggable output format where CSV/TSV is not sufficient.
- Configurable compression.

### Credits

- This tool is originally made for [Brainly](https://github.com/brainly)
