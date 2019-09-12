# poc-snapshot

## Overview

The [poc_snapshot.py](poc_snapshot.py) utility takes a snapshot of the current state of the records loaded in a Senzing repository. Taking a snapshot is much like performing an export.  If you use G2Export.py to export senzing data in csv format, consider using this as an alternative.  

See https://senzing.zendesk.com/hc/en-us/articles/115004915547-G2Export-How-to-Consume-Resolved-Entity-Data

The poc_snapshot exports all the same fields as G2Export except for entity_name and json_data.  The fields included in a snapshot are:
- RESOLVED_ENTITY_ID
- RELATED_ENTITY_ID
- MATCH_LEVEL
- MATCH_KEY
- DATA_SOURCE
- RECORD_ID

However, the biggest difference between a poc_snapshot and a G2Export is that statistics and examples are captured during the export in a companion json file.  This json file can then be loaded into the poc_viewer.py so that its statistics and examples can be browsed in an interactive report style.

See https://github.com/Senzing/poc-viewer

The poc-snapshot utility computes the following statistical reports ...
- **dataSourceSummary** This report shows the matches, possible matches and relationships within each data source.
- **crossSourceSummary** This report shows the matches, possible matches and relationships across two different data sources.
- **entitySizeBreakdown** This report categorizes entities by their size (how many records they contain) and selects a list of entities to review that may be over-matched due to multiple names, addresses, DOBs, etc. 

Usage:

```console
python3 poc_snapshot.py --help
usage: poc_snapshot.py [-h] [-o OUTPUT_FILE_ROOT] [-c INI_FILE_NAME]
                       [-s SAMPLE_SIZE] [-f RELATIONSHIP_FILTER] [-n]
                       [-k CHUNK_SIZE]

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUT_FILE_ROOT, --output_file_root OUTPUT_FILE_ROOT
                        root name for files created such as
                        "/project/audit/run1"
  -c INI_FILE_NAME, --ini_file_name INI_FILE_NAME
                        name of the g2.ini file, defaults to
                        /opt/senzing/g2/python/G2Module.ini
  -s SAMPLE_SIZE, --sample_size SAMPLE_SIZE
                        defaults to 1000
  -f RELATIONSHIP_FILTER, --relationship_filter RELATIONSHIP_FILTER
                        filter options 1=No Relationships, 2=Include possible
                        matches, 3=Include possibly related and disclosed.
                        Defaults to 3
  -n, --no_csv_export   compute json stats only, do not export csv file
  -k CHUNK_SIZE, --chunk_size CHUNK_SIZE
                        chunk size: number of records to query at a time,
                        defaults to 1000000
```

## Contents

1. [Prerequisites](#Prerequisites)
2. [Installation](#Installation)
3. [Typical use](#Typical-use)

### Prerequisites
- python 3.6 or higher
- Senzing API version 1.7 or higher

### Installation

1. Simply place the the following files in a directory of your choice ...  (Ideally along with poc-viewer.py)
    - [poc_viewer.py](poc_snapshot.py) 

2. Set PYTHONPATH environment variable to python directory where you installed Senzing.
    - Example: export PYTHONPATH=/opt/senzing/g2/python

3. Set the SZ_INI_FILE_NAME environment variable for the senzing instance you want to explore.
    - Example: export SZ_INI_FILE_NAME=/opt/senzing/g2/python/G2Module.ini

Its a good idea to place these settings in your .bashrc file to make sure the enviroment is always setup and ready to go.

### Typical use
```console
python3 poc_viewer.py 
```
The -c configuration parameter is only required if the SZ_INI_FILE_NAME environment variable is not set.

The -s snapshot file parameter is for convenience if you just took a snapshot and want to load it.

