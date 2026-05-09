"""In-memory backend registry."""
from __future__ import annotations

import threading
from collections.abc import Iterable, Iterator
from contextlib import contextmanager

from .backend import Backend


class BackendRegistry:
    """Thread-safe in-memory registry of Backend objects.

    Lookup is by ``backend_id``; iteration yields a snapshot list to avoid
    holding the lock during selection.
    """

    def __init__(self, backends: Iterable[Backend] = ()):
        self._lock = threading.Lock()
        self._by_id: dict[str, Backend] = {b.backend_id: b for b in backends}

    # ───── mutate ─────

    def add(self, backend: Backend) -> None:
        with self._lock:
            self._by_id[backend.backend_id] = backend

    def remove(self, backend_id: str) -> bool:
        with self._lock:
            return self._by_id.pop(backend_id, None) is not None

    def replace_all(self, backends: Iterable[Backend]) -> None:
        with self._lock:
            self._by_id = {b.backend_id: b for b in backends}

    # ───── read ─────

    def get(self, backend_id: str) -> Backend | None:
        with self._lock:
            return self._by_id.get(backend_id)

    def all(self) -> list[Backend]:
        with self._lock:
            return list(self._by_id.values())

    def __len__(self) -> int:
        with self._lock:
            return len(self._by_id)

    def __iter__(self) -> Iterator[Backend]:
        return iter(self.all())

    def __contains__(self, backend_id: object) -> bool:
        with self._lock:
            return backend_id in self._by_id

    # ───── locked access for atomic state updates ─────

    @contextmanager
    def edit(self, backend_id: str) -> Iterator[Backend | None]:
        """Hold the registry lock while inspecting/mutating one backend.

        Use this when you need to update health/breaker state without
        racing against concurrent probes.
        """
        with self._lock:
            yield self._by_id.get(backend_id)
