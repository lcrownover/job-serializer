# job-serializer

This is pretty specific to our org, so you probably can't use it ;)

```
Usage of ./bin/job-serializer:
  -basedir string
        path to directory containing job directories
  -date string
        date to parse in the format YYYY-MM-DD
  -force
        force re-parsing of jobs
  -outputdir string
        path to directory for output files
  -workers int
        number of threads to use. max is 10000 (default 2000)
```

Examples:

Parse all dates into data.json files. 
Skips any directories that have already been parsed.

```bash
job-serializer -basedir /path/to/date/directories -outputdir /path/to/output/directory
```

Parse a single date:

```bash
job-serializer -basedir /path/to/date/directories -outputdir /path/to/output/directory -date 2022-01-01
```

Force re-parse:

```bash
job-serializer -basedir /path/to/date/directories -outputdir /path/to/output/directory -date 2022-01-01 -force
```

