import hashlib
import json
import tempfile
import unittest
from io import BytesIO
from pathlib import Path

from registry.metadata import (
    ArtifactMetadataStore,
    DuplicateArtifactError,
    InvalidVersionError,
)
from registry.resolver import (
    DependencyCycleError,
    DependencyResolver,
    ResolutionConflictError,
    SemverConstraint,
)
from registry.storage import ArtifactStorage, ChecksumMismatchError, InvalidDigestError
from registry.storage import retrieve as retrieve_blob
from registry.storage import store as store_blob


def fake_sha(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


class StorageTests(unittest.TestCase):
    def test_store_uses_content_addressed_sha_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = ArtifactStorage(tmp)
            blob = storage.store_bytes(b"package bytes")

            expected = hashlib.sha256(b"package bytes").hexdigest()
            self.assertEqual(blob.sha256, expected)
            self.assertEqual(blob.size, len(b"package bytes"))
            self.assertEqual(blob.path, Path(tmp) / expected[:2] / expected)
            self.assertEqual(storage.retrieve(expected), b"package bytes")

    def test_identical_content_reuses_same_blob(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = ArtifactStorage(tmp)
            first = storage.store_bytes(b"same")
            second = storage.store_bytes(b"same")

            self.assertEqual(first.sha256, second.sha256)
            self.assertEqual(first.path, second.path)
            self.assertEqual(len(list((Path(tmp) / first.sha256[:2]).iterdir())), 1)

    def test_invalid_digest_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = ArtifactStorage(tmp)
            with self.assertRaises(InvalidDigestError):
                storage.retrieve("not-a-sha")

    def test_declared_checksum_mismatch_is_rejected_before_storing(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = ArtifactStorage(tmp)
            wrong = "sha256:" + hashlib.sha256(b"wrong").hexdigest()

            with self.assertRaises(ChecksumMismatchError) as caught:
                storage.store_verified(BytesIO(b"actual"), wrong)

            actual = hashlib.sha256(b"actual").hexdigest()
            self.assertEqual(caught.exception.actual, actual)
            self.assertFalse((Path(tmp) / actual[:2] / actual).exists())

    def test_task_spec_store_and_retrieve_helpers(self):
        with tempfile.TemporaryDirectory() as tmp:
            sha256 = store_blob(b"helper bytes", root=tmp)

            self.assertEqual(sha256, hashlib.sha256(b"helper bytes").hexdigest())
            self.assertEqual(retrieve_blob(sha256, root=tmp), b"helper bytes")


class MetadataTests(unittest.TestCase):
    def test_publish_fetch_and_list_versions(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ArtifactMetadataStore(Path(tmp) / "registry.db")
            record = store.publish(
                name="lib-core",
                version="1.0.0",
                sha256=fake_sha("core"),
                size=10,
                publisher="ci",
                deps=[{"name": "lib-base", "version": "^1.0.0"}],
            )

            fetched = store.get("lib-core", "1.0.0")
            self.assertEqual(fetched, record)
            self.assertEqual(store.list_versions("lib-core"), ["1.0.0"])
            self.assertEqual(
                fetched.to_dict()["deps"],
                [{"name": "lib-base", "version": "^1.0.0"}],
            )

    def test_duplicate_publish_is_immutable(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ArtifactMetadataStore(Path(tmp) / "registry.db")
            kwargs = {
                "name": "lib-core",
                "version": "1.0.0",
                "sha256": fake_sha("core"),
                "size": 10,
                "publisher": "ci",
                "deps": [],
            }
            store.publish(**kwargs)
            with self.assertRaises(DuplicateArtifactError):
                store.publish(**kwargs)

    def test_non_semver_version_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ArtifactMetadataStore(Path(tmp) / "registry.db")
            with self.assertRaises(InvalidVersionError):
                store.publish(
                    name="lib-core",
                    version="latest",
                    sha256=fake_sha("core"),
                    size=10,
                    publisher="ci",
                    deps=[],
                )

    def test_invalid_metadata_sha_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ArtifactMetadataStore(Path(tmp) / "registry.db")
            with self.assertRaises(Exception):
                store.publish(
                    name="lib-core",
                    version="1.0.0",
                    sha256="not-a-sha",
                    size=10,
                    publisher="ci",
                    deps=[],
                )


class SemverTests(unittest.TestCase):
    def test_exact_constraint(self):
        constraint = SemverConstraint.parse("1.0.0")
        self.assertTrue(constraint.satisfies("1.0.0"))
        self.assertFalse(constraint.satisfies("1.0.1"))

    def test_caret_constraint(self):
        constraint = SemverConstraint.parse("^1.2.3")
        self.assertTrue(constraint.satisfies("1.2.3"))
        self.assertTrue(constraint.satisfies("1.9.9"))
        self.assertFalse(constraint.satisfies("2.0.0"))

    def test_tilde_constraint(self):
        constraint = SemverConstraint.parse("~1.2.3")
        self.assertTrue(constraint.satisfies("1.2.3"))
        self.assertTrue(constraint.satisfies("1.2.9"))
        self.assertFalse(constraint.satisfies("1.3.0"))

    def test_comparator_range(self):
        constraint = SemverConstraint.parse(">=1.0.0 <2.0.0")
        self.assertTrue(constraint.satisfies("1.0.0"))
        self.assertTrue(constraint.satisfies("1.5.0"))
        self.assertFalse(constraint.satisfies("2.0.0"))


class ResolverTests(unittest.TestCase):
    def make_store(self, tmp):
        return ArtifactMetadataStore(Path(tmp) / "registry.db")

    def publish(self, store, name, version, deps=None):
        return store.publish(
            name=name,
            version=version,
            sha256=fake_sha(f"{name}-{version}"),
            size=10,
            publisher="ci",
            deps=deps or [],
        )

    def test_resolves_highest_satisfying_transitive_versions(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(tmp)
            self.publish(store, "lib-core", "1.0.0")
            self.publish(store, "lib-core", "1.1.0")
            self.publish(
                store,
                "lib-http",
                "1.0.0",
                deps=[{"name": "lib-core", "version": "^1.0.0"}],
            )

            lockfile = DependencyResolver(store).resolve(
                [{"name": "lib-http", "version": "1.0.0"}]
            )

            self.assertEqual(
                lockfile.to_dict(),
                {
                    "resolved": {
                        "lib-core": {
                            "version": "1.1.0",
                            "sha256": fake_sha("lib-core-1.1.0"),
                            "resolved_by": "^1.0.0",
                        },
                        "lib-http": {
                            "version": "1.0.0",
                            "sha256": fake_sha("lib-http-1.0.0"),
                            "resolved_by": "1.0.0",
                        },
                    }
                },
            )

    def test_select_version_accepts_direct_constraint(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(tmp)
            self.publish(store, "lib-core", "1.0.0")
            self.publish(store, "lib-core", "1.1.0")

            selected = DependencyResolver(store).select_version("lib-core", "^1.0.0")

            self.assertEqual(selected.version, "1.1.0")

    def test_conflict_detection_names_constraints(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(tmp)
            self.publish(store, "lib-core", "1.0.0")
            self.publish(
                store,
                "lib-a",
                "1.0.0",
                deps=[{"name": "lib-core", "version": "^1.0.0"}],
            )
            self.publish(
                store,
                "lib-b",
                "1.0.0",
                deps=[{"name": "lib-core", "version": "<1.0.0"}],
            )

            with self.assertRaises(ResolutionConflictError) as caught:
                DependencyResolver(store).resolve(
                    [
                        {"name": "lib-a", "version": "1.0.0"},
                        {"name": "lib-b", "version": "1.0.0"},
                    ]
                )

            message = str(caught.exception)
            self.assertIn("version conflict for lib-core", message)
            self.assertIn("^1.0.0", message)
            self.assertIn("<1.0.0", message)

    def test_cycle_detection_names_cycle(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(tmp)
            self.publish(
                store,
                "lib-a",
                "1.0.0",
                deps=[{"name": "lib-b", "version": "1.0.0"}],
            )
            self.publish(
                store,
                "lib-b",
                "1.0.0",
                deps=[{"name": "lib-a", "version": "1.0.0"}],
            )

            with self.assertRaises(DependencyCycleError) as caught:
                DependencyResolver(store).resolve([{"name": "lib-a", "version": "1.0.0"}])

            self.assertIn("lib-a@1.0.0 -> lib-b@1.0.0 -> lib-a@1.0.0", str(caught.exception))

    def test_lockfile_json_is_deterministic(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(tmp)
            self.publish(store, "lib-core", "1.0.0")
            self.publish(store, "lib-http", "1.0.0")
            resolver = DependencyResolver(store)

            deps = [
                {"name": "lib-http", "version": "1.0.0"},
                {"name": "lib-core", "version": "1.0.0"},
            ]
            first = resolver.resolve(deps).to_json()
            second = resolver.resolve(list(reversed(deps))).to_json()

            self.assertEqual(first, second)
            self.assertEqual(json.loads(first)["resolved"]["lib-core"]["version"], "1.0.0")

    def test_dependencies_from_superseded_version_do_not_linger(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = self.make_store(tmp)
            self.publish(store, "extra-lib", "1.0.0")
            self.publish(
                store,
                "lib-core",
                "1.0.0",
                deps=[],
            )
            self.publish(
                store,
                "lib-core",
                "1.1.0",
                deps=[{"name": "extra-lib", "version": "1.0.0"}],
            )
            self.publish(
                store,
                "lib-pin",
                "1.0.0",
                deps=[{"name": "lib-core", "version": "1.0.0"}],
            )

            lockfile = DependencyResolver(store).resolve(
                [
                    {"name": "lib-core", "version": "^1.0.0"},
                    {"name": "lib-pin", "version": "1.0.0"},
                ]
            )

            resolved = lockfile.to_dict()["resolved"]
            self.assertEqual(resolved["lib-core"]["version"], "1.0.0")
            self.assertNotIn("extra-lib", resolved)


if __name__ == "__main__":
    unittest.main()
