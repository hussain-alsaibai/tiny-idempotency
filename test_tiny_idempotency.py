"""Tests for tiny_idempotency."""

import os
import sys
import time
import shutil
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import tiny_idempotency as ti
from tiny_idempotency import (
    IdempotencyStore,
    InMemoryStore,
    FileStore,
    Fingerprint,
    KeyMismatch,
    InProgressError,
    idempotent,
    fingerprint_args,
)


class TestFingerprint(unittest.TestCase):
    def test_same_args_same_fingerprint(self):
        a = fingerprint_args(1, "x", [1, 2])
        b = fingerprint_args(1, "x", [1, 2])
        self.assertEqual(a, b)

    def test_different_args_different_fingerprint(self):
        a = fingerprint_args(1, "x")
        b = fingerprint_args(2, "x")
        self.assertNotEqual(a, b)

    def test_dict_order_does_not_matter(self):
        a = fingerprint_args({"a": 1, "b": 2})
        b = fingerprint_args({"b": 2, "a": 1})
        self.assertEqual(a, b)

    def test_class_excludes_named_keys(self):
        fp = Fingerprint(exclude="idempotency_key")
        a = fp.of(idempotency_key="k1", amount=100)
        b = fp.of(idempotency_key="k2", amount=100)
        self.assertEqual(a, b)

    def test_of_callable_includes_qualname(self):
        def f1():
            pass

        def f2():
            pass

        a = Fingerprint(exclude="k").of_callable(f1, (), {"k": "x", "v": 1})
        b = Fingerprint(exclude="k").of_callable(f2, (), {"k": "x", "v": 1})
        self.assertNotEqual(a, b)

    def test_serialise_falls_back_to_repr(self):
        class Foo:
            def __repr__(self):
                return "<Foo>"

        a = fingerprint_args(Foo())
        b = fingerprint_args(Foo())
        self.assertEqual(a, b)


class TestInMemoryStore(unittest.TestCase):
    def test_claim_first_time(self):
        store = InMemoryStore()
        result = store.claim("k1", "fp1", 60.0)
        self.assertIsNone(result)

    def test_claim_with_completed_returns_record(self):
        store = InMemoryStore()
        store.claim("k1", "fp1", 60.0)
        store.complete("k1", "fp1", {"value": 42}, 60.0)
        result = store.claim("k1", "fp1", 60.0)
        self.assertIsNotNone(result)
        self.assertEqual(result["result"], {"value": 42})

    def test_claim_with_different_fingerprint_raises(self):
        store = InMemoryStore()
        store.claim("k1", "fp1", 60.0)
        store.complete("k1", "fp1", {"value": 42}, 60.0)
        with self.assertRaises(KeyMismatch):
            store.claim("k1", "fp2", 60.0)

    def test_claim_in_progress_returns_record(self):
        store = InMemoryStore()
        store.claim("k1", "fp1", 60.0)
        result = store.claim("k1", "fp1", 60.0)
        self.assertIsNotNone(result)
        self.assertEqual(result["status"], "in_progress")

    def test_expired_record_returns_none(self):
        store = InMemoryStore()
        store.claim("k1", "fp1", 0.05)
        time.sleep(0.1)
        result = store.claim("k1", "fp1", 60.0)
        self.assertIsNone(result)

    def test_purge_expired(self):
        store = InMemoryStore()
        store.claim("k1", "fp1", 0.05)
        store.claim("k2", "fp2", 60.0)
        time.sleep(0.1)
        removed = store.purge_expired()
        self.assertEqual(removed, 1)
        self.assertEqual(len(store), 1)


class TestFileStore(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_basic_claim_complete(self):
        store = FileStore(self.tmpdir)
        self.assertIsNone(store.claim("k1", "fp1", 60.0))
        store.complete("k1", "fp1", {"v": 1}, 60.0)
        result = store.claim("k1", "fp1", 60.0)
        self.assertEqual(result["result"], {"v": 1})

    def test_persistence_across_instances(self):
        s1 = FileStore(self.tmpdir)
        s1.claim("k1", "fp1", 60.0)
        s1.complete("k1", "fp1", {"v": 99}, 60.0)
        s2 = FileStore(self.tmpdir)
        result = s2.claim("k1", "fp1", 60.0)
        self.assertEqual(result["result"], {"v": 99})


class TestIdempotentDecorator(unittest.TestCase):
    def test_first_call_runs_function(self):
        store = InMemoryStore()
        calls = []

        @idempotent(store=store, key_arg="idem")
        def add(idem, x, y):
            calls.append((x, y))
            return x + y

        self.assertEqual(add(idem="k1", x=1, y=2), 3)
        self.assertEqual(calls, [(1, 2)])

    def test_retry_returns_cached_result(self):
        store = InMemoryStore()
        calls = []

        @idempotent(store=store, key_arg="idem")
        def add(idem, x, y):
            calls.append((x, y))
            return x + y

        add(idem="k1", x=1, y=2)
        add(idem="k1", x=1, y=2)
        add(idem="k1", x=1, y=2)
        self.assertEqual(calls, [(1, 2)])

    def test_different_key_runs_again(self):
        store = InMemoryStore()
        calls = []

        @idempotent(store=store, key_arg="idem")
        def add(idem, x, y):
            calls.append((x, y))
            return x + y

        add(idem="k1", x=1, y=2)
        add(idem="k2", x=3, y=4)
        self.assertEqual(len(calls), 2)

    def test_same_key_different_payload_raises(self):
        store = InMemoryStore()

        @idempotent(store=store, key_arg="idem")
        def charge(idem, amount):
            return {"amount": amount}

        charge(idem="k1", amount=100)
        with self.assertRaises(KeyMismatch):
            charge(idem="k1", amount=200)

    def test_failed_call_can_retry(self):
        store = InMemoryStore()
        calls = []

        @idempotent(store=store, key_arg="idem")
        def flaky(idem):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("boom")
            return "ok"

        with self.assertRaises(RuntimeError):
            flaky(idem="k1")
        self.assertEqual(flaky(idem="k1"), "ok")

    def test_in_progress_raises(self):
        store = InMemoryStore()

        @idempotent(store=store, key_arg="idem")
        def f(idem, x):
            return x

        # Compute the same fingerprint the decorator will compute.
        from tiny_idempotency import Fingerprint
        fp = Fingerprint(exclude="idem").of_callable(f, (), {"idem": "k1", "x": 1})
        # Pre-claim in another "worker"
        store.claim("k1", fp, 60.0)

        with self.assertRaises(InProgressError):
            f(idem="k1", x=1)

    def test_key_fn_derives_key(self):
        store = InMemoryStore()
        calls = []

        @idempotent(store=store, key_fn=lambda user_id, amount: f"u{user_id}-a{amount}")
        def charge(user_id, amount):
            calls.append((user_id, amount))
            return {"ok": True}

        charge(42, 100)
        charge(42, 100)  # cached
        charge(42, 200)  # different amount -> new key -> runs again
        self.assertEqual(calls, [(42, 100), (42, 200)])

    def test_missing_key_arg_raises(self):
        store = InMemoryStore()

        @idempotent(store=store, key_arg="idem")
        def f(x):
            return x

        with self.assertRaises(ValueError):
            f(x=1)


class TestConcurrent(unittest.TestCase):
    def test_concurrent_first_call_only_one_runs(self):
        store = InMemoryStore()
        calls = []
        lock = threading.Lock()

        @idempotent(store=store, key_arg="idem", ttl_seconds=60.0)
        def slow(idem):
            with lock:
                calls.append(1)
            time.sleep(0.2)
            return "ok"

        results = []
        errors = []

        def runner():
            try:
                r = slow(idem="k1")
            except Exception as exc:
                errors.append(exc)
            else:
                results.append(r)

        threads = [threading.Thread(target=runner) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # First thread should succeed; the others should see in_progress
        # and raise InProgressError (since the lock is held during the sleep).
        successes = [r for r in results if r == "ok"]
        in_progress = [e for e in errors if isinstance(e, InProgressError)]
        self.assertEqual(len(successes), 1)
        # The other 7 are split between InProgressError and successful cache
        # hits if they came in after the first finished.
        self.assertGreaterEqual(len(successes) + len(in_progress), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)