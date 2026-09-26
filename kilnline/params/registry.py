"""Generation-stamped parameter sets with durable publication."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from kilnline.errors import NotFoundError, ValidationError
from kilnline.ledger.records import LedgerRecord
from kilnline.ledger.stream import EventStream
from kilnline.params.generation import GenerationCounter, digest_of
from kilnline.store.json_store import JsonFileStore

PARAMETER_DOCUMENT = "parameter-set"
PARAMETER_KEY = "params:active"


@dataclass(frozen=True)
class ParameterSet:
    """One immutable generation of process parameters."""

    generation: int
    values: dict[str, float]
    digest: str
    published_at: float
    author: str

    def value(self, key: str, default: float | None = None) -> float:
        if key in self.values:
            return float(self.values[key])
        if default is None:
            raise NotFoundError("parameter is not present in this generation", key=key)
        return float(default)

    def as_dict(self) -> dict[str, Any]:
        return {
            "generation": int(self.generation),
            "values": {str(key): float(value) for key, value in self.values.items()},
            "digest": self.digest,
            "published_at": float(self.published_at),
            "author": self.author,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ParameterSet":
        raw = payload.get("values", {})
        if not isinstance(raw, Mapping):
            raise ValidationError("parameter values must be a mapping")
        return cls(
            generation=int(payload.get("generation", 0)),
            values={str(key): float(value) for key, value in raw.items()},
            digest=str(payload.get("digest", "")),
            published_at=float(payload.get("published_at", 0.0)),
            author=str(payload.get("author", "")),
        )


class ParameterRegistry:
    """Publishes parameter generations and keeps the published history."""

    def __init__(
        self,
        store: JsonFileStore,
        stream: EventStream,
        *,
        document: str = PARAMETER_DOCUMENT,
        key: str = PARAMETER_KEY,
        history_limit: int = 50,
    ) -> None:
        self._store = store
        self._stream = stream
        self._document = document
        self._key = key
        self._history_limit = max(1, int(history_limit))
        self._history: list[ParameterSet] = self._load()

    @property
    def key(self) -> str:
        return self._key

    @property
    def generation(self) -> int:
        return 0 if not self._history else self._history[-1].generation

    @property
    def published_count(self) -> int:
        return len(self._history)

    def publish(
        self,
        values: Mapping[str, float],
        *,
        published_at: float,
        author: str,
        reason: str = "publish",
    ) -> ParameterSet:
        if not values:
            raise ValidationError("a parameter set must contain at least one value")
        normalised = {str(key): float(value) for key, value in values.items()}
        for name, value in normalised.items():
            if value != value:  # NaN guard, kept explicit for reviewers
                raise ValidationError("parameter values must be finite", key=name)
        generation = self.generation + 1
        parameter_set = ParameterSet(
            generation=generation,
            values=normalised,
            digest=digest_of(normalised),
            published_at=float(published_at),
            author=str(author),
        )
        self._history = [*self._history, parameter_set][-self._history_limit :]
        self._persist()
        self._stream.put(
            self._key,
            parameter_set.as_dict(),
            written_at=published_at,
            generation=generation,
            reason=reason,
        )
        self._stream.commit(committed_at=published_at)
        return parameter_set

    def current(self) -> ParameterSet:
        if not self._history:
            raise NotFoundError("no parameter generation has been published yet")
        return self._history[-1]

    def get(self, generation: int) -> ParameterSet:
        wanted = int(generation)
        for parameter_set in reversed(self._history):
            if parameter_set.generation == wanted:
                return parameter_set
        raise NotFoundError("unknown parameter generation", generation=wanted)

    def history(self) -> list[ParameterSet]:
        return list(self._history)

    def is_current(self, generation: int) -> bool:
        return bool(self._history) and self._history[-1].generation == int(generation)

    def age_seconds(self, now: float) -> float:
        return max(0.0, float(now) - self.current().published_at)

    def restore(self, records: Sequence[LedgerRecord]) -> ParameterSet | None:
        """Rebuild the published history from a ledger replay."""

        by_generation: dict[int, ParameterSet] = {}
        for record in records:
            if record.key != self._key or record.is_tombstone:
                continue
            candidate = ParameterSet.from_dict(record.payload)
            by_generation[candidate.generation] = candidate
        if not by_generation:
            return None
        ordered = [by_generation[number] for number in sorted(by_generation)]
        self._history = ordered[-self._history_limit :]
        return self._history[-1]

    def _persist(self) -> None:
        current = self._history[-1]
        payload = {
            "current": current.as_dict(),
            "history": [item.as_dict() for item in self._history],
        }
        self._store.write(self._document, payload, written_at=float(current.published_at))

    def _load(self) -> list[ParameterSet]:
        document = self._store.read_or_none(self._document)
        if document is None:
            return []
        raw_history = document.data.get("history")
        if isinstance(raw_history, list):
            entries = [
                ParameterSet.from_dict(item) for item in raw_history if isinstance(item, Mapping)
            ]
            entries.sort(key=lambda item: item.generation)
            return entries[-self._history_limit :]
        # Documents written before the history list existed carry only "current".
        current = document.data.get("current")
        if not isinstance(current, Mapping):
            return []
        return [ParameterSet.from_dict(current)]
