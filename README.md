# rcni_ingestion
Basic class for ingesting multiple RCNI files

Only has ability to ingest locally right now.  As an example, to create a combined RCNI file from a local drive:

```python
df = IngestRCNILocally("C:/Data/RCNI-1"). \
    files_from_dir(). \
    toPandasDF()
````
