"""单位标定领域服务：维护传感器原始值到目标计量单位的换算系数与零点偏移标定。

标定与接收可能重叠，本服务以 ``源分片`` 为采样出处单位：

- 只读取归属分片的原始序列，未完整接收的分片不会进入标定；
- 每条标定记录把 (测点, 分片, 批次, 采样范围, 校验值, 产物文件) 持久化到
  接收台账，操作员可凭该出处重新标定；
- 已完成标定的记录不被未确认上送覆盖，重复确认幂等返回既有成果；
- 各分片标定成果单独归档后合并为测点级标定文件，下游（时间对齐、清洗、
  降采样等）仍按测点处理，合并时按 (分片, 采样序号) 去重。
"""
from __future__ import annotations

import json
import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.ingest_ledger import IngestLedger
from app.store import store


class UnitCalibrateService(BaseModuleService):
    MODULE = "unit_calibrate"
    ENTITY = "标定记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["标定编号", "测点编号", "源分片", "源批次", "原始单位", "目标单位", "换算系数", "零点偏移", "标定方式", "标定时间"]
    REQUIRED_FIELDS = ["标定编号", "测点编号", "原始单位"]
    STATUS_ORDER = ["草稿", "待标定", "已标定", "已失效"]
    ACTION_RULES = {"提交标定": "待标定", "确认标定": "已标定", "标记失效": "已失效"}
    NEGATIVE_ACTIONS = ["标记失效"]

    def _ledger(self) -> IngestLedger:
        return IngestLedger()

    def _resolve_shard(self, entry: dict[str, Any]) -> str:
        """解析标定采样出处分片：优先显式源分片，其次由标定编号反解批次分片。"""
        shard = str(entry.get("源分片") or "").strip()
        if shard:
            return shard
        cal_id = entry.get("标定编号", "")
        if cal_id.startswith("UNIT-UPLO-"):
            return cal_id.removeprefix("UNIT-UPLO-")
        return entry["测点编号"]

    def _merge_point_output(self, point_key: str) -> int:
        """将测点下各分片标定成果合并为测点级文件，按 (分片, 采样序号) 去重。"""
        ledger = self._ledger()
        calibrated_dir = self._data_dir("calibrated")
        point_file = calibrated_dir / f"{point_key}_calibrated.jsonl"
        merged: dict[tuple[str, Any], dict[str, Any]] = {}

        def _absorb(records: list[dict[str, Any]]) -> None:
            for rec in records:
                if rec.get("sample_index") is not None and str(rec.get("sample_index")).strip():
                    key = (rec.get("shard", point_key), rec["sample_index"])
                else:
                    # 历史记录没有采样序号：用时间戳兜底，保证跨次合并仍能去重
                    key = (rec.get("shard", point_key),
                           f"legacy-ts:{rec.get('timestamp', id(rec))}")
                merged.setdefault(key, rec)

        # 台账登记的分片出处优先（带状态与采样范围），是当前有效标定成果
        ledger_shards = {
            s["shard"] for s in ledger.calibration_sources(point_key)
            if s.get("status") == "已标定"
        }
        for shard in ledger_shards:
            _absorb(self._read_jsonl(
                calibrated_dir / f"{point_key}_{shard}_calibrated.jsonl"))
        # 兜底：扫描该测点全部分片成果，覆盖未登记台账的手工标定
        for shard_file in calibrated_dir.glob(f"{point_key}_*_calibrated.jsonl"):
            _absorb(self._read_jsonl(shard_file))
        # 兼容未走接收台账的历史/手工标定：仅在没有任何台账管理的分片成果时，
        # 才保留测点级文件里的旧行，避免与分片成果重复计数
        if not ledger_shards:
            _absorb(self._read_jsonl(point_file))

        def _sort_key(rec: dict[str, Any]) -> tuple[str, int]:
            raw_idx = rec.get("sample_index")
            try:
                return str(rec.get("shard", "")), int(raw_idx)
            except (TypeError, ValueError):
                return str(rec.get("shard", "")), 0

        ordered = sorted(merged.values(), key=_sort_key)
        tmp_file = point_file.with_suffix(".jsonl.tmp")
        with open(tmp_file, "w", encoding="utf-8") as f:
            for rec in ordered:
                f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        tmp_file.replace(point_file)
        return len(ordered)

    def calibrate_values(self, calibrate_id: int) -> dict[str, Any]:
        """对标定记录归属分片的原始数据执行单位换算并写入标定结果归档文件。"""
        cal = store.find(self.MODULE, calibrate_id)
        if cal is None:
            return {"error": f"标定记录 {calibrate_id} 不存在"}
        # 已完成标定的样本不被未确认上送覆盖：幂等返回既有成果与出处
        ledger = self._ledger()
        existing_provenance = next(
            (c for c in ledger.calibration_sources(cal["测点编号"])
             if c.get("calibration_id") == cal["标定编号"] and c.get("status") == "已标定"),
            None,
        )
        if cal.get("status") == "已标定" and existing_provenance is not None:
            return {"calibrate_id": cal["标定编号"], "point": cal["测点编号"],
                    "shard": existing_provenance["shard"],
                    "records_calibrated": existing_provenance.get("sample_count", 0),
                    "scale": float(cal.get("换算系数", "1")),
                    "offset": float(cal.get("零点偏移", "0")),
                    "deduped": True,
                    "source": existing_provenance}
        point_key = cal["测点编号"]
        shard = self._resolve_shard(cal)
        scale = float(cal.get("换算系数", "1"))
        offset = float(cal.get("零点偏移", "0"))
        # 只读取归属分片的原始采样；同时核对台账中的分片范围与采样出处
        samples_file = self._data_dir("edge_samples") / f"{shard}.jsonl"
        raw_samples = self._read_jsonl(samples_file)
        samples = [s for s in raw_samples if str(s.get("point", point_key)) == point_key]
        if not samples:
            # 原始序列尚未完整接收：保持待标定，等待重发后重试，不产生空标定成果
            return {"error": f"测点 {point_key} 分片 {shard} 暂无可标定原始序列",
                    "calibrate_id": cal["标定编号"], "records_calibrated": 0,
                    "retryable": True}
        shard_state = ledger.get_shard_state(point_key, shard)
        idx_values = [int(s["sample_index"]) for s in samples if str(s.get("sample_index", "")).strip()]
        idx_min = min(idx_values) if idx_values else 1
        idx_max = max(idx_values) if idx_values else len(samples)
        # 执行单位标定计算：原始值 × 换算系数 + 零点偏移
        calibrated = []
        for s in samples:
            raw = float(s.get("raw_value", s.get("raw_value_mV", 0)))
            cal_val = round(raw * scale + offset, 4)
            calibrated.append({
                "point": point_key,
                "shard": shard,
                "sample_index": s.get("sample_index"),
                "timestamp": s["timestamp"],
                "raw_value_mV": raw,
                "calibrated_value_V": cal_val,
                "scale": scale,
                "offset": offset,
                # 数据转换输出：将标定计算结果写入持久化标定数据JSONL文件存储
                "calibrate_id": cal["标定编号"],
            })
        # 分片级标定成果单独归档（重写而非追加，重复标定不产生重复行）
        shard_output = self._data_dir("calibrated") / f"{point_key}_{shard}_calibrated.jsonl"
        with open(shard_output, "w", encoding="utf-8") as f:
            for rec in calibrated:
                f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        written = len(calibrated)
        # 先落盘成果再推进状态，避免“状态已标定但写成果失败”的不一致
        entry, _ = self.run_action(calibrate_id, "确认标定")
        entry["标定时间"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
        checksum = shard_state.get("checksum", "") if shard_state else ""
        ledger.mark_calibration(
            point_key, shard, entry["标定编号"], "已标定",
            {"batch": entry.get("源批次", ""), "source_file": str(samples_file),
             "sample_index_min": idx_min, "sample_index_max": idx_max,
             "sample_count": written, "checksum": checksum,
             "output_file": str(shard_output)},
        )
        # 合并为测点级文件供下游（对齐/清洗/降采样）消费
        point_total = self._merge_point_output(point_key)
        return {"calibrate_id": entry["标定编号"], "point": point_key, "shard": shard,
                "records_calibrated": written, "point_records_available": point_total,
                "sample_index_min": idx_min, "sample_index_max": idx_max,
                "scale": scale, "offset": offset, "deduped": False}

    def recalibrate(self, calibrate_id: int) -> dict[str, Any]:
        """基于已持久化的采样出处重新标定；失效或失败记录可重算，已标定记录受保护需先失效。"""
        cal = store.find(self.MODULE, calibrate_id)
        if cal is None:
            return {"error": f"标定记录 {calibrate_id} 不存在"}
        ledger = self._ledger()
        provenance = next(
            (c for c in ledger.calibration_sources(cal["测点编号"])
             if c.get("calibration_id") == cal["标定编号"]),
            None,
        )
        if provenance is None:
            return {"error": f"标定 {cal['标定编号']} 无持久化采样出处，无法重新标定",
                    "retryable": False}
        if cal.get("status") == "已标定":
            return {"error": "已完成标定受保护，请先标记失效后再重新标定",
                    "calibrate_id": cal["标定编号"], "source": provenance,
                    "retryable": False}
        # 回到待标定后按原始出处重新计算
        cal["status"] = "待标定"
        cal["pending"] = True
        cal["abnormal"] = False
        return self.calibrate_values(calibrate_id)

    def calibration_provenance(self, point_key: str) -> dict[str, Any]:
        """查询测点下每条标定记录基于哪个分片、哪个采样范围的原始序列。"""
        ledger = self._ledger()
        sources = ledger.calibration_sources(point_key)
        received = ledger.received_shards(point_key)
        return {"point": point_key, "received_shards": received,
                "calibration_sources": sources}

    def batch_calibrate(self, point_keys: list[str]) -> dict[str, Any]:
        """批量执行多个测点的单位标定转换，返回标定汇总数据集。"""
        results = []
        for pk in point_keys:
            cals = [r for r in store.rows(self.MODULE)
                    if r.get("测点编号") == pk and r.get("status") in ("待标定", "已标定")]
            for cal in cals:
                r = self.calibrate_values(cal["id"])
                results.append(r)
        # 写入批量标定汇总日志文件
        self._write_jsonl(
            self._data_dir("calibrated") / "batch_calibrate_summary.jsonl",
            [{"action": "batch_calibrate", "points": len(point_keys),
              "executed": len(results), "at": time.time()}]
        )
        return {"total_points": len(point_keys), "executed": len(results), "results": results}
