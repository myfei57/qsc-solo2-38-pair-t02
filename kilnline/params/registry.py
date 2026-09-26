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
        self._generations: list[ParameterSet] = self._load()
        self._current: ParameterSet | None = self._generations[-1] if self._generations else None

    @property
    def key(self) -> str:
        return self._key

    @property
    def generation(self) -> int:
        return 0 if self._current is None else self._current.generation

    @property
    def published_count(self) -> int:
        return len(self._generations)

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
        self._persist(parameter_set)
        self._stream.put(
            self._key,
            parameter_set.as_dict(),
            written_at=published_at,
            generation=generation,
            reason=reason,
        )
        self._stream.commit(committed_at=published_at)
        self._current = parameter_set
        self._generations.append(parameter_set)
        self._generations = self._generations[-self._history_limit :]
        return parameter_set

    def current(self) -> ParameterSet:
        if self._current is None:
            raise NotFoundError("no parameter generation has been published yet")
        return self._current

    def get(self, generation: int) -> ParameterSet:
        wanted = int(generation)
        for parameter_set in self._generations:
            if parameter_set.generation == wanted:
                return parameter_set
        raise NotFoundError("unknown parameter generation", generation=wanted)

    def history(self) -> list[ParameterSet]:
        """Every retained generation, oldest first."""

        return list(self._generations)

    def is_current(self, generation: int) -> bool:
        return self._current is not None and self._current.generation == int(generation)

    def age_seconds(self, now: float) -> float:
        return max(0.0, float(now) - self.current().published_at)

    def restore(self, records: Sequence[LedgerRecord]) -> ParameterSet | None:
        """Rebuild every published parameter generation from a ledger replay."""

        recovered: dict[int, ParameterSet] = {
            parameter_set.generation: parameter_set for parameter_set in self._generations
        }
        for record in records:
            if record.key != self._key or record.is_tombstone:
                continue
            candidate = ParameterSet.from_dict(record.payload)
            recovered[candidate.generation] = candidate
        generations = sorted(recovered.values(), key=lambda parameter_set: parameter_set.generation)
        self._generations = generations[-self._history_limit :]
        self._current = self._generations[-1] if self._generations else None
        return self._current

    def _persist(self, parameter_set: ParameterSet) -> None:
        retained = self._generations[-self._history_limit + 1 :] + [parameter_set]
        payload = {"current": parameter_set.as_dict(), "generations": [item.as_dict() for item in retained]}
        written_at = float(parameter_set.published_at)
        self._store.write(self._document, payload, written_at=written_at)

    def _load(self) -> list[ParameterSet]:
        document = self._store.read_or_none(self._document)
        if document is None:
            return []
        raw = document.data.get("generations")
        if not isinstance(raw, list):
            # Backwards compatibility: older documents only carried "current".
            raw = [document.data.get("current")]
        generations: list[ParameterSet] = []
        seen: set[int] = set()
        for entry in raw:
            if not isinstance(entry, Mapping):
                continue
            parameter_set = ParameterSet.from_dict(entry)
            if parameter_set.generation in seen:
                continue
            generations.append(parameter_set)
            seen.add(parameter_set.generation)
        generations.sort(key=lambda parameter_set: parameter_set.generation)
        return generations[-self._history_limit :]
