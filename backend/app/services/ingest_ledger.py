"""上送接收台账（持久化幂等账本）。

边缘重发与标定读取可能交叠，单靠内存状态无法判断一个分片是否已经完整接收、
是否已经标定。本模块在数据目录下持久化两类产物：

- ``ingest_ledger/received.jsonl``：只追加的接收/标定事件流水，崩溃后可追溯；
- ``ingest_ledger/state_<测点>.json``：按测点压缩后的当前状态，记录每个分片的
  采样范围、校验值、上送状态以及已经持久化的标定采样出处。

所有判断都以 ``(测点编号, 分片编号)`` 为键，保证：已完整接收的分片不会被重复
记账；校验值不一致的重发会被拒绝；已完成标定的样本出处可被操作员查询并用于
重新标定。
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from app.services.base import DATA_ROOT


class ShardConflictError(Exception):
    """同一分片重发内容与已持久化接收记录不一致（校验值冲突）。"""


class IngestLedger:
    def __init__(self) -> None:
        self.ledger_dir = DATA_ROOT / "ingest_ledger"
        self.ledger_dir.mkdir(parents=True, exist_ok=True)
        self.event_file = self.ledger_dir / "received.jsonl"

    # -- 持久化 helpers --

    def _state_file(self, point_key: str) -> Path:
        return self.ledger_dir / f"state_{point_key}.json"

    def _load_point_state(self, point_key: str) -> dict[str, Any]:
        path = self._state_file(point_key)
        if not path.exists():
            return {"point": point_key, "shards": {}, "calibrations": []}
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_point_state(self, state: dict[str, Any]) -> None:
        point_key = state["point"]
        tmp = self._state_file(point_key).with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2, default=str)
        tmp.replace(self._state_file(point_key))

    def _append_event(self, event: dict[str, Any]) -> None:
        event["at"] = time.time()
        with open(self.event_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")

    @staticmethod
    def _sample_range(records: list[dict[str, Any]]) -> tuple[int, int, int]:
        indices = [int(r["sample_index"]) for r in records if str(r.get("sample_index", "")).strip()]
        if indices:
            return min(indices), max(indices), len(records)
        return 1, len(records), len(records)

    # -- 接收记账 --

    def record_received(
        self, point_key: str, shard_key: str, records: list[dict[str, Any]], checksum: str
    ) -> dict[str, Any]:
        """登记一个完整接收的分片；同内容重发幂等跳过，异内容重发抛出冲突。"""
        state = self._load_point_state(point_key)
        shards = state.setdefault("shards", {})
        idx_min, idx_max, count = self._sample_range(records)
        existing = shards.get(shard_key)
        if existing is not None:
            if existing.get("checksum") != checksum:
                self._append_event({
                    "event": "receive_conflict", "point": point_key, "shard": shard_key,
                    "existing_checksum": existing.get("checksum"), "incoming_checksum": checksum,
                })
                raise ShardConflictError(
                    f"分片 {shard_key} 重发内容校验值 {checksum} 与已接收记录 "
                    f"{existing.get('checksum')} 不一致，拒绝覆盖"
                )
            self._append_event({
                "event": "receive_duplicate_skipped", "point": point_key,
                "shard": shard_key, "checksum": checksum,
            })
            return {"deduped": True, "shard_state": existing}
        shard_state = {
            "point": point_key,
            "shard": shard_key,
            "sample_index_min": idx_min,
            "sample_index_max": idx_max,
            "sample_count": count,
            "checksum": checksum,
            "received_at": time.time(),
            "batches": [],
            "upload_status": "待上送",
            "calibration_ids": [],
            "calibration_status": "未标定",
        }
        shards[shard_key] = shard_state
        self._save_point_state(state)
        self._append_event({
            "event": "received", "point": point_key, "shard": shard_key,
            "sample_index_min": idx_min, "sample_index_max": idx_max,
            "sample_count": count, "checksum": checksum,
        })
        return {"deduped": False, "shard_state": shard_state}

    def mark_upload(self, point_key: str, shard_key: str, batch_key: str, status: str) -> None:
        """推进分片的上送状态并登记归属批次（重复调用不重复记账）。"""
        state = self._load_point_state(point_key)
        shard_state = state["shards"].setdefault(shard_key, {
            "point": point_key, "shard": shard_key, "sample_index_min": 0,
            "sample_index_max": 0, "sample_count": 0, "checksum": "",
            "received_at": time.time(), "batches": [],
            "upload_status": "待上送", "calibration_ids": [],
            "calibration_status": "未标定",
        })
        if batch_key not in shard_state["batches"]:
            shard_state["batches"].append(batch_key)
        shard_state["upload_status"] = status
        self._save_point_state(state)
        self._append_event({
            "event": "upload_status", "point": point_key, "shard": shard_key,
            "batch": batch_key, "status": status,
        })

    def mark_calibration(
        self, point_key: str, shard_key: str, calibration_id: str, status: str,
        source: dict[str, Any],
    ) -> None:
        """登记标定记录及其持久化采样出处；同标定号重复写入只更新状态、不新增出处。"""
        state = self._load_point_state(point_key)
        shard_state = state["shards"].setdefault(shard_key, {
            "point": point_key, "shard": shard_key, "sample_index_min": 0,
            "sample_index_max": 0, "sample_count": 0, "checksum": "",
            "received_at": time.time(), "batches": [],
            "upload_status": "未知", "calibration_ids": [],
            "calibration_status": "未标定",
        })
        if calibration_id not in shard_state["calibration_ids"]:
            shard_state["calibration_ids"].append(calibration_id)
        shard_state["calibration_status"] = status
        existing = next(
            (c for c in state["calibrations"] if c.get("calibration_id") == calibration_id),
            None,
        )
        provenance = {
            "calibration_id": calibration_id,
            "point": point_key,
            "shard": shard_key,
            "batch": source.get("batch", ""),
            "status": status,
            "source_file": source.get("source_file", ""),
            "sample_index_min": source.get("sample_index_min", 0),
            "sample_index_max": source.get("sample_index_max", 0),
            "sample_count": source.get("sample_count", 0),
            "checksum": source.get("checksum", ""),
            "output_file": source.get("output_file", ""),
        }
        if existing is None:
            state["calibrations"].append(provenance)
        else:
            existing.update(provenance)
        self._save_point_state(state)
        self._append_event({"event": "calibration_status", **provenance})

    # -- 查询 --

    def get_shard_state(self, point_key: str, shard_key: str) -> dict[str, Any] | None:
        return self._load_point_state(point_key)["shards"].get(shard_key)

    def is_received(self, point_key: str, shard_key: str) -> bool:
        return self.get_shard_state(point_key, shard_key) is not None

    def received_shards(self, point_key: str) -> list[dict[str, Any]]:
        """按接收先后返回测点下已接收分片，供下游确定可处理的实际范围。"""
        shards = list(self._load_point_state(point_key)["shards"].values())
        return sorted(shards, key=lambda s: (s.get("received_at", 0), s.get("shard", "")))

    def calibration_sources(self, point_key: str) -> list[dict[str, Any]]:
        """返回测点下每条标定记录持久化的原始序列出处，供操作员判断能否重新标定。"""
        return [dict(c) for c in self._load_point_state(point_key).get("calibrations", [])]

    def find_calibration_for_shard(self, point_key: str, shard_key: str) -> dict[str, Any] | None:
        for cal in self.calibration_sources(point_key):
            if cal.get("shard") == shard_key and cal.get("status") == "已标定":
                return cal
        return None
