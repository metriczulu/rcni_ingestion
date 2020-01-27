# rcni_ingestion
Basic class for ingesting multiple RCNI files

Only has ability to ingest locally right now.  As an example, to create a combined RCNI file from a local drive:

```python
df = IngestRCNILocally("C:/Data/RCNI-1"). \
    files_from_dir(). \
    toPandasDF()
````

The RCNI ingestion package needs to be extended to work on an S3 bucket.  The next step is to create a new class that inherits from IngestRCNIMixin and overrides the *_files_list()* method to generate a list of the RCNI files to ingest and combined in the specified s3 bucket directory.
