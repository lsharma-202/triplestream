"""Lineage context bridged from Prefect runtime to PROV-O IRIs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from prefect.runtime import flow_run, task_run

from triplestream.lineage.uris import flow_agent_uri, task_activity_uri


@dataclass(frozen=True, slots=True)
class LineageContext:
    """Batch-scoped provenance context passed into transform tasks."""

    source_id: str
    batch_id: str
    batch_dir: Path

    @classmethod
    def from_batch_dir(cls, source_id: str, batch_dir: str | Path) -> LineageContext:
        path = Path(batch_dir)
        return cls(source_id=source_id, batch_id=path.name, batch_dir=path)

    @property
    def flow_agent(self):
        return flow_agent_uri(str(flow_run.get_id()))

    @property
    def task_activity(self):
        return task_activity_uri(str(task_run.get_id()))

    @property
    def flow_run_id(self) -> str:
        return str(flow_run.get_id())

    @property
    def task_run_id(self) -> str:
        return str(task_run.get_id())
