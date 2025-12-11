# ACROSS Data Ingestion

This is the codebase for the NASA ACROSS Data Ingestion Server. It runs ingestion tasks on a predetermined schedule as defined per task and delivers data to the across-server database.

## Contents

- [Getting Started](#getting-started)
  - [Development](#development)
  - [Debugging](#debugging)
  - [VS Code Setup](#vs-code-setup)

## Getting Started

It is assumed that the user has completed and installed the following:

- Clone the repository
- [Docker Desktop Installation](https://docs.docker.com/desktop/)

Then simply run

```zsh
make init
```

That's it! This is a [`Makefile target`](https://makefiletutorial.com/#targets) command that will:

- Ask to install [`uv`](https://docs.astral.sh/uv/getting-started/installation/) if it's not installed. (This is a required step.)
- Install dependencies
- Install [`pre-commit`](https://pre-commit.com/) git hooks.
- Create a `.env` config file
- Build the containers

If everything completed successfully, you should be able to access the generated OpenAPI docs locally at [http://127.0.0.1:8001/docs](http://127.0.0.1:8001/docs).

Logs should populate with tasks running in the background as they reach their trigger condition.

General documentation for other project commands can be found with `make help`.

### Development

The across-server is assumed to be running locally alongside the ingestion server. The across-server can be run through a standard docker container in the background. Please see the [across-server documentation](https://github.com/ACROSS-Team/across-server/blob/main/README.md) for more information.

In order to run the server through the CLI run

```zsh
make dev
```

This will start up the development server in your terminal which will run outside of the container.

If you already have the container running, you may need to stop the currently running server to free up the port using

```zsh
make stop
```

While it is possible to develop using the server running on the docker container, it may not always be ideal. Specifically, logs will output to the container itself. A tail of the logs can be output to your local terminal through the following command for ease. Under the hood it runs a docker command.

```zsh
make tail_log
```

### Debugging

In the `Run and Debug` sidebar panel in vscode, launch `local: Start Data Ingestion`. This will start the development server with an attached debugger. More information on debugging in vscode can be found in [here](https://code.visualstudio.com/docs/editor/debugging).

### VS Code Setup

#### Python Interpreter

When working on a python project is that VS Code will need to be told where the interpreter lives. The interpreter will be in the root of this project under `.venv/bin/python`.

This should be automatically set when the extensions load due to the workspace setting `python.defaultInterpreterPath`. However, it can be set manually with the following steps:

1. `CMD + SHIFT + P` to open the command palette
2. Search for `Python: Select Interpreter`
3. Click on `Python 3.12.4 ('.venv')`

#### Workspace

This should handle any project specific configuration that is needed along with any required extension recommendations, spelling, launch, tasks, etc.

### Project Structure

The data-ingestion server is as follows:
across-data-ingestion # Your named directory where the repo lives
├── .github/    # Contains GH actions and workflows for CI/CD
├── across-data-ingestion
│   ├── core/               # Any shared ACROSS dependencies
│   │   ├── enums/
│   │   ├── schemas.py
│   │   ├── config.py
│   │   ├── constants.py
│   │   ├── logging.py
│   │   └── exceptions.py
│   ├── tasks/               # Any shared ACROSS dependencies
│   │   ├── example/
│   │   ├── schedules/       # schedule ingestion for each observatory
│   │   │   └── [observatory]/
│   │   │        └── [schedule-fidelity-status].py  # ingest the schedule of noted fidelity and status
│   │   ├── [task]/
│   │   └── task_loader.py  # import and assign crons to each task
│   ├── routes/
│   ├── util/
│   │   ├── [util or external service].py    # file or directory for a utility or external service
│   │   └── across_server/  # ACROSS SERVER SDK WRAPPER
│   └── main.py             # Entrypoint to the server
├── tests/  # mirrors the source code project structure
├── requirements/   # project dependencies
├── .env
├── .gitignore
├── pyproject.toml
├── Makefile
├── README.md
└── ...

## Notice

NASA Docket No. GSC-19,469-1, and identified as "Astrophysics Cross-Observatory
Science Support (ACROSS) System

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.“

Copyright © 2025 United States Government as represented by the Administrator
of the National Aeronautics and Space Administration and The Penn State
Research Foundation.  All rights reserved. This software is licensed under the
Apache 2.0 License.
