
## Project Overview

CaspyORM is a modern, Pythonic Object-Relational Mapper (ORM) for Apache Cassandra. Its design is heavily inspired by the Django ORM and Pydantic, aiming to provide a type-safe, intuitive, and high-performance interface for interacting with a Cassandra database.

### Key Features:

-   **Model-driven:** Database tables are defined using Python classes that inherit from `caspyorm.Model`.
-   **Type-Safe Fields:** Schema is defined using typed fields like `fields.Text()`, `fields.UUID()`, `fields.Integer()`, etc.
-   **Sync and Async Support:** The ORM is designed with both synchronous and asynchronous operations in mind (e.g., `Model.save()` and `Model.save_async()`). **Note:** The `aiocassandra`-based async functionality is currently disabled due to driver incompatibilities with Cassandra 4.x, so async operations will raise a `NotImplementedError`.
-   **Fluent QuerySet API:** Supports lazy, chainable queries (e.g., `Model.filter(field__gt=10).limit(5).all()`).
-   **Complex Types:** Natively supports Cassandra's collection types (`List`, `Set`, `Map`), `Tuple`, and User-Defined Types (UDTs) via `caspyorm.types.UserType`.
-   **FastAPI & Pydantic Integration:** Provides helpers for easy integration with FastAPI, including dependency injection for sessions and automatic serialization to Pydantic models.
-   **Command-Line Interface (CLI):** A powerful CLI tool named `caspy` (built with Typer and Rich) for interacting with models, running queries, and managing database schema migrations.
-   **Migration System:** A file-based migration system to manage schema evolution, similar to those in other web frameworks.

### Core Dependencies:

-   `cassandra-driver`: The official DataStax Python driver for Cassandra.
-   `pydantic`: Used for data validation and serialization, especially in the FastAPI integration.
-   `typer` & `rich`: For building the CLI.
-   `aiocassandra`: An optional dependency for async operations (currently disabled).

## Codebase Structure

The project is organized into the ORM library (`src/caspyorm`), a CLI tool (`src/caspyorm_cli`), tests, and utility scripts.

### 1. ORM Library (`src/caspyorm`)

-   **`core/`**: The heart of the ORM.
    -   `model.py`: Defines the base `Model` class.
    -   `fields.py`: Contains all field types (`Text`, `Integer`, `List`, `Map`, `UserDefinedType`, etc.).
    -   `query.py`: Implements the `QuerySet` class, which handles the construction and execution of database queries.
    -   `connection.py`: Manages the connection to the Cassandra cluster through the `ConnectionManager` class. This is where the sync/async connection logic resides.

-   **`_internal/`**: Contains the "magic" that makes the ORM work.
    -   `model_construction.py`: Defines `ModelMetaclass`, which intercepts `Model` class definitions, parses the fields, and builds an internal schema representation (`__caspy_schema__`).
    -   `query_builder.py`: A set of functions that generate CQL query strings (e.g., `INSERT`, `SELECT`, `CREATE TABLE`) from the model schema and `QuerySet` parameters.
    -   `schema_sync.py`: Handles the logic for `sync_table`, comparing the model's schema with the database schema and generating `ALTER TABLE` or `CREATE TABLE` statements.
    -   `serialization.py`: Manages the conversion of model instances to dictionaries, JSON, and dynamic Pydantic models.

-   **`types/`**: Defines complex, non-primitive types.
    -   `usertype.py`: Defines the `UserType` base class for creating Cassandra UDTs.
    -   `batch.py`: Implements the `BatchQuery` context manager for executing multiple operations in a single batch.

-   **`contrib/`**: Houses integrations with third-party libraries.
    -   `fastapi.py`: Provides dependencies and helpers for FastAPI applications, such as `get_async_session` for session injection and `as_response_model` for serializing query results.

-   **`utils/`**: Shared utilities.
    -   `exceptions.py`: Defines custom exceptions like `ValidationError`, `QueryError`, and `ConnectionError`.
    -   `logging.py`: Configures the logging for the library.

### 2. CLI (`src/caspyorm_cli`)

-   `main.py`: The entry point for the `caspy` command. It uses `typer` to define a rich set of subcommands.
-   **Model Discovery:** The CLI dynamically discovers user-defined models by searching specified Python paths (`models.py`, current directory, or paths set in `CASPY_MODELS_PATH`).
-   **Key Commands:**
    -   `caspy models`: Lists all discovered models.
    -   `caspy query <model> <command>`: Executes queries (`get`, `filter`, `count`, `delete`) against a model.
    -   `caspy migrate <command>`: Manages the migration workflow (`init`, `new`, `status`, `apply`, `downgrade`).
    -   `caspy connect`: Tests the database connection.

### 3. Migration System

-   The migration system is managed via the `caspy migrate` CLI command.
-   Migrations are stored as individual Python files in the `migrations/` directory.
-   Each migration file is expected to have an `upgrade()` function and an optional `downgrade()` function.
-   The ORM tracks which migrations have been applied in a dedicated Cassandra table named `caspyorm_migrations`, which is defined by the internal `Migration` model (`src/caspyorm/_internal/migration_model.py`).

### 4. Testing (`tests/`)

-   The project has a comprehensive test suite separated into `unit` and `integration` tests.
-   **Unit Tests:** Test individual components in isolation, often using mocks (e.g., field validation, batch logic).
-   **Integration Tests:** Require a running Cassandra instance (provided by `docker-compose.yml`). They test the full end-to-end functionality, including database interactions, the migration system, and the CLI itself.
-   The `tests/models.py` file defines a common `NYC311` model used in many tests, and `tests/data/nyc_311.csv` provides the data for it.
-   The scripts in `scripts/` are used to download and import this test data.

### 5. Packaging and Distribution

The project uses `hatchling` as its build backend, configured via `pyproject.toml`. This modern approach handles project metadata, dependencies, and package structure.

-   **`pyproject.toml`**: Defines project metadata (name, version, authors, description, license, classifiers), runtime dependencies, optional dependencies (e.g., `fastapi`, `async`), and entry points for the `caspy` CLI. It also specifies which files are included in the source distribution (`sdist`) and wheel (`wheel`) builds.
-   **`MANIFEST.in`**: Specifies non-Python files to be included in the source distribution, such as `README.md`, `LICENSE`, and `pyproject.toml`. It also defines patterns for excluding development-related files and directories.
-   **`setup.py`**: A minimal `setup.py` is present, primarily for compatibility, with the main build configuration managed by `pyproject.toml` and `hatchling`.
