# ontoform

Simple CLI tool that transforms ontologies into NDJSONs optimized for Open Targets pipeline.

## Running

### Locally

[Install uv](https://docs.astral.sh/uv/getting-started/installation/).

```bash
uv run ontoform --help
```

### Docker

```bash
docker build -t ontoform .
docker run ontoform --help
```

## Tools

The `./tools` folder contains some useful utilities:

* `test_efo.sh`: A script to test the EFO output against old PIS.
* `schema.jq`: A JQ script to generate a first draft of the schema of an ontology in JSON format.

## Copyright
Copyright 2014-2024 EMBL - European Bioinformatics Institute, Genentech, GSK, MSD, Pfizer, Sanofi and Wellcome Sanger Institute

This software was developed as part of the Open Targets project. For more information please
see: http://www.opentargets.org

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
