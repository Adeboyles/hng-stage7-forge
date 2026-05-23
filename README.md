# Forge

Forge is a self-hosted CI/CD and artifact infrastructure platform that combines isolated pipeline execution, deterministic dependency resolution, real-time build orchestration, and an immutable artifact registry into a single production-grade system.

## Task No. 4: Artifact Registry + Dependency Resolver Implementation

This task implements the core artifact registry and dependency resolver logic for Forge. The work is focused on three registry modules:

- `registry/storage.py`
- `registry/metadata.py`
- `registry/resolver.py`

It also includes unit coverage in `tests/test_registry_core.py`.

The implementation is intentionally built in-house. It does not use an existing artifact registry, does not store metadata in JSON files, and does not rely on a third-party semver resolver library.

### Storage Layer

Artifact blobs are stored using content-addressable storage. Every uploaded blob is hashed with SHA-256, and the hash determines the final storage path.

The layout is:

```text
/data/artifacts/
  ab/
    ab1234567890abcdef...
  cd/
    cd9876543210fedcba...
```

The first two characters of the SHA-256 digest are used as the directory prefix. The full SHA-256 digest is used as the blob filename.

This gives Forge three important properties:

- Identical artifact contents are stored only once.
- A blob can always be verified by recomputing its SHA-256 hash.
- Metadata can point to immutable content by digest.

The main storage class is `ArtifactStorage`.

```python
storage = ArtifactStorage("/data/artifacts")
blob = storage.store(fileobj)
```

`store()` streams bytes from a binary file object, computes the SHA-256 digest server-side, writes the upload to a temporary file, and atomically moves it into the final content-addressed path.

The returned `StoredBlob` contains:

```python
StoredBlob(
    sha256="...",
    size=123,
    path=Path("...")
)
```

The storage layer also exposes task-spec helper functions:

```python
sha256 = store(file_bytes)
content = retrieve(sha256)
```

For upload verification, `store_verified()` accepts a declared checksum in the required format:

```text
sha256:<hex>
```

If the declared checksum does not match the bytes received by the server, storage raises `ChecksumMismatchError`. This is the error the API layer should map to HTTP `400`.

Invalid digest strings are rejected before path construction, preventing malformed paths from reaching the filesystem.

### Metadata Layer

Artifact metadata is stored in SQLite through `ArtifactMetadataStore`. Metadata is not stored in JSON files.

The required schema is created automatically:

```sql
CREATE TABLE IF NOT EXISTS artifacts (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  version TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  size INTEGER NOT NULL,
  publisher TEXT NOT NULL,
  published_at TEXT NOT NULL,
  deps TEXT NOT NULL,
  UNIQUE(name, version)
);
```

The `deps` column stores a JSON array of dependency declarations. The database remains the source of truth for package coordinates, versions, hashes, publishers, publish timestamps, and declared dependencies.

Publishing metadata is done through:

```python
store = ArtifactMetadataStore("/data/registry.db")
record = store.publish(
    name="lib-core",
    version="1.0.0",
    sha256="...",
    size=123,
    publisher="ci-token-name",
    deps=[
        {"name": "lib-base", "version": "^1.0.0"}
    ],
)
```

The metadata layer validates:

- Artifact name format
- Strict semver versions in `MAJOR.MINOR.PATCH` format
- SHA-256 digest format
- Non-negative artifact size
- Non-empty publisher identity
- Dependency entries with `name` and `version`

Artifact immutability is enforced at the database level with:

```sql
UNIQUE(name, version)
```

This is important for race safety. If two pipelines try to publish the same `(name, version)` at the same time, SQLite allows only one insert to win. The losing insert raises `DuplicateArtifactError`, which the API layer should map to HTTP `409`.

There is no overwrite path for an existing artifact coordinate.

### Dependency Resolver

Dependency resolution is implemented in `registry/resolver.py`.

The resolver is custom-built and does not use a semver resolver library.

The main entry point is:

```python
resolver = DependencyResolver(metadata_store)
lockfile = resolver.resolve([
    {"name": "lib-core", "version": "^1.0.0"}
])
```

The resolver supports the required semver constraint forms:

```text
1.0.0                 exact version
^1.2.3                >=1.2.3 <2.0.0
~1.2.3                >=1.2.3 <1.3.0
>=1.0.0 <2.0.0        comparator range
```

The `SemverConstraint` class parses each constraint into internal comparators and checks whether a version satisfies all comparator rules.

Example:

```python
constraint = SemverConstraint.parse("^1.2.3")
constraint.satisfies("1.9.9")  # True
constraint.satisfies("2.0.0")  # False
```

Versions are parsed numerically as `major`, `minor`, and `patch`. This avoids incorrect string sorting such as treating `1.10.0` as lower than `1.2.0`.

### Resolution Algorithm

The resolver performs deterministic transitive dependency resolution:

1. Normalize and sort root dependencies.
2. Load all published versions for each package from SQLite metadata.
3. Select the highest version satisfying all known constraints for that package.
4. Read that selected version's declared dependencies.
5. Add transitive dependency constraints to the graph.
6. Repeat until the selected graph and constraints stop changing.
7. Detect dependency cycles.
8. Produce a deterministic lockfile.

Selection is deterministic because:

- Dependency names are processed in sorted order.
- Versions are sorted by numeric semver components.
- The highest satisfying version is selected consistently.
- Lockfile package keys are emitted in sorted order.
- JSON lockfile output uses stable key ordering and compact separators.

The resolver also rebuilds constraints from the currently selected graph on each pass. This prevents stale transitive dependencies from remaining in the lockfile if a package is later reselected to a different version.

### Conflict Detection

If no published version can satisfy all constraints for a package, the resolver raises `ResolutionConflictError`.

Example conflict:

```text
lib-a -> lib-core ^1.0.0
lib-b -> lib-core <1.0.0
```

If only `lib-core@1.0.0` exists, those constraints cannot both be satisfied. The resolver fails before a build should start.

The error message includes the conflicting package and the constraint origins so the pipeline engine can report a clear `conflict_failure`.

### Cycle Detection

The resolver detects dependency cycles in the selected dependency graph.

Example:

```text
lib-a@1.0.0 -> lib-b@1.0.0 -> lib-a@1.0.0
```

When a cycle is found, the resolver raises `DependencyCycleError` with the cycle path. The pipeline engine can map this to `cycle_failure`.

Cycle detection runs during resolution, before lockfile generation, so cyclic dependency graphs do not produce lockfiles.

### Lockfile Format

The resolver produces a lockfile with exact versions and SHA-256 hashes:

```json
{
  "resolved": {
    "lib-core": {
      "version": "1.0.0",
      "sha256": "abc123...",
      "resolved_by": "^1.0.0"
    }
  }
}
```

The lockfile is deterministic for the same input dependencies and registry state. This is required so the same pipeline and same registry metadata produce byte-for-byte identical lockfile JSON.

The lockfile object exposes:

```python
lockfile.to_dict()
lockfile.to_json()
```

`to_json()` uses sorted keys and compact separators for stable output.

### Error Types

The registry core exposes explicit error classes so the API layer can map failures cleanly:

- `ChecksumMismatchError`: upload checksum mismatch, should become HTTP `400`
- `DuplicateArtifactError`: duplicate `(name, version)`, should become HTTP `409`
- `InvalidVersionError`: non-semver published version
- `InvalidDigestError`: invalid SHA-256 digest
- `ResolutionConflictError`: dependency constraints cannot be satisfied
- `DependencyCycleError`: dependency graph contains a cycle
- `PackageNotFoundError`: requested package or matching version does not exist

### Unit Tests

Tests live in:

```text
tests/test_registry_core.py
```

They cover:

- Content-addressable blob paths
- Blob deduplication for identical content
- Blob retrieval by SHA-256
- Declared checksum mismatch rejection
- SQLite schema initialization
- Metadata publish and fetch
- Duplicate publish immutability
- Non-semver version rejection
- Exact semver constraints
- Caret semver constraints
- Tilde semver constraints
- Comparator range constraints
- Highest satisfying version selection
- Transitive dependency walking
- Conflict detection
- Cycle detection
- Deterministic lockfile output
- Prevention of stale dependencies from superseded selected versions

Run the tests with:

```bash
python3 -B -m unittest discover -s tests -v
```

Expected result:

```text
Ran 19 tests

OK
```
