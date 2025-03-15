> [!NOTE]
>
> Ontoform is now deprecated in favour of [PTS](https://github.com/opentargets/pts). Development will continue there.

# ontoform

Simple CLI tool that transforms ontologies and other input data into The format used by the Open Targets pipeline.

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
