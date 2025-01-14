# ACROSS Data Ingestion

This is the codebase for the NASA ACROSS Data Ingestion Server. It runs ingestion tasks on a predetermined schedule as defined per task and delivers data to the across-server database.

## Contents

- [Getting Started](#getting-started)
  - [Development](#development)
  - [Testing Routes Locally](#testing-routes-locally)
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

General documentation for other project commands can be found with `make help`.

### Development

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

In the `Run and Debug` sidebar panel in vscode, launch `Uvicorn: Fastapi`. This will start the development server with an attached debugger. More information on debugging in vscode can be found in [here](https://code.visualstudio.com/docs/editor/debugging).

### VS Code Setup

#### Python Interpreter

When working on a python project is that VS Code will need to be told where the interpreter lives. The interpreter will be in the root of this project under `.venv/bin/python`.

This should be automatically set when the extensions load due to the workspace setting `python.defaultInterpreterPath`. However, it can be set manually with the following steps:

1. `CMD + SHIFT + P` to open the command palette
2. Search for `Python: Select Interpreter`
3. Click on `Python 3.12.4 ('.venv')`

#### Workspace

This should handle any project specific configuration that is needed along with any required extension recommendations, spelling, launch, tasks, etc.
