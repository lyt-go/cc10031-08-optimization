"""批量上送领域服务：维护缓冲分片按范围批量上送的记录条数、压缩方式与批次校验值。

接收与标定可能重叠，本服务在启动上送时同时核对分片范围与持久化接收台账
（``ingest_ledger``）：

- 同一分片内容一致的边缘重发幂等跳过，已完整接收的分片不会重复记账；
- 校验值不一致的重发判为分片冲突，批次置失败并保留原接收记录，不允许覆盖；
- 上送完成触发的标定与水位按 (测点, 分片) 去重，已完成标定的样本不会被
  未确认上送覆盖；触发失败可通过 ``retry_downstream`` 单独重试。
"""
from __future__ import annotations

import json
import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.ingest_ledger import IngestLedger, ShardConflictError
from app.store import store


class UploadBatchService(BaseModuleService):
    MODULE = "upload_batch"
    ENTITY = "上送批次"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["批次编号", "分片范围", "测点编号", "测点数量", "记录条数", "压缩方式", "批次校验值", "上送时长", "批次水位"]
    REQUIRED_FIELDS = ["批次编号", "分片范围", "测点数量"]
    STATUS_ORDER = ["草稿", "待上送", "上送中", "已完成", "已失败"]
    ACTION_RULES = {"启动上送": "上送中", "标记完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = ["标记失败"]

    def _ledger(self) -> IngestLedger:
        return IngestLedger()

    def _resolve_point(self, batch: dict[str, Any], shard: str) -> str:
        """解析批次归属测点：优先批次字段，其次边缘缓冲分片，最后退回分片键。"""
        point = str(batch.get("测点编号") or "").strip()
        if point:
            return point
        edge_row = next(
            (r for r in store.rows("edge_buffer") if r.get("分片编号") == shard),
            None,
        )
        if edge_row and edge_row.get("测点编号"):
            return edge_row["测点编号"]
        return shard

    # 数据导入操作：读取边缘采样文件启动批次上送写入采集数据校验集记录
    def submit_batch(self, batch_id: int) -> dict[str, Any]:
        """启动批次上送并模拟压缩传输，将上送结果写入批次交付数据文件。"""
        batch = store.find(self.MODULE, batch_id)
        if batch is None:
            return {"error": f"上送批次 {batch_id} 不存在"}
        shard = batch.get("分片范围", batch["批次编号"])
        point_key = self._resolve_point(batch, shard)
        batch["测点编号"] = point_key
        # 读取对应的边缘采样文件模拟上送
        samples_file = self._data_dir("edge_samples") / f"{shard}.jsonl"
        samples = self._read_jsonl(samples_file)
        checksum = self._compute_checksum(json.dumps(samples, ensure_ascii=False, default=str))
        ledger = self._ledger()

        # 幂等恢复：上送中/已完成的批次再次提交只回传现状，不重复记账
        if batch.get("status") in ("上送中", "已完成"):
            shard_state = ledger.get_shard_state(point_key, shard)
            self._write_jsonl(
                self._data_dir("upload_batches") / f"{batch['批次编号']}.jsonl",
                [{"action": "resubmit_ignored", "batch": batch["批次编号"],
                  "shard": shard, "point": point_key, "status": batch.get("status"),
                  "at": time.time()}]
            )
            return {"batch_key": batch["批次编号"], "shard": shard, "point": point_key,
                    "records": len(samples), "duration": float(batch.get("上送时长", 0) or 0),
                    "status": batch["status"], "deduped": True,
                    "shard_state": shard_state}

        # 已失败批次允许修正后重试：重新进入上送中
        entry, _ = self.run_action(batch_id, "启动上送")
        # 与历史登记的校验值核对，防止重发内容被篡改后覆盖已接收分片
        previous_checksum = str(batch.get("批次校验值") or "").strip()
        if previous_checksum and previous_checksum != checksum:
            self.run_action(batch_id, "标记失败")
            return {"error": f"分片 {shard} 重发校验值 {checksum} 与已登记 {previous_checksum} 冲突，拒绝覆盖",
                    "batch_key": batch["批次编号"], "shard": shard, "point": point_key,
                    "status": "已失败"}

        # 持久化接收台账：同内容重发幂等，异内容重发冲突
        try:
            receipt = ledger.record_received(point_key, shard, samples, checksum)
        except ShardConflictError as exc:
            self.run_action(batch_id, "标记失败")
            return {"error": str(exc), "batch_key": batch["批次编号"],
                    "shard": shard, "point": point_key, "status": "已失败"}
        deduped = bool(receipt.get("deduped"))
        shard_state = receipt["shard_state"]
        ledger.mark_upload(point_key, shard, batch["批次编号"], "上送中")

        upload_duration = len(samples) * 0.01
        batch["上送时长"] = f"{upload_duration:.2f}"
        batch["记录条数"] = str(len(samples))
        batch["批次校验值"] = checksum
        batch["批次水位"] = str(shard_state.get("sample_index_max", len(samples)))
        # 写入上送批次持久化记录文件，记录传输摘要（重发只记跳过事件，不重复记账）
        self._write_jsonl(
            # 批次校验：将上送交付记录写入持久化日志数据文件
            self._data_dir("upload_batches") / f"{batch['批次编号']}.jsonl",
            [{"action": "start_upload", "batch": batch["批次编号"], "shard": shard,
              "point": point_key, "record_count": len(samples),
              "upload_duration_s": round(upload_duration, 2),
              "compressed_size": len(samples) * 12, "deduped": deduped,
              "sample_index_min": shard_state.get("sample_index_min"),
              "sample_index_max": shard_state.get("sample_index_max"),
              "checksum": checksum, "at": time.time()}]
        )
        return {"batch_key": batch["批次编号"], "shard": shard, "point": point_key,
                "records": len(samples), "duration": round(upload_duration, 2),
                "status": "上送中", "deduped": deduped, "shard_state": shard_state}

    def _ensure_calibration_seed(
        self, point_key: str, shard: str, batch_key: str
    ) -> dict[str, Any]:
        """按 (测点, 分片) 创建或取回标定记录，重复上送不会产生重复标定。"""
        cal_key = f"UNIT-{batch_key}"
        existing = next(
            (r for r in store.rows("unit_calibrate") if r.get("标定编号") == cal_key),
            None,
        )
        if existing is not None:
            return {"entry": existing, "deduped": True}
        cal_svc = self._safe_import("app.services.unit_calibrate", "UnitCalibrateService")()
        entry, missing = cal_svc.create_entry({
            "标定编号": cal_key,
            "测点编号": point_key,
            "源分片": shard,
            "源批次": batch_key,
            "原始单位": "mV",
            "目标单位": "V",
            "换算系数": "0.001",
            "零点偏移": "0",
            "标定方式": "线性换算",
            "标定时间": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
        })
        if entry is None:
            raise ValueError(f"标定种子缺少必填字段：{'、'.join(missing)}")
        # 已随上送交付进入待标定，等待操作员确认标定
        entry["status"] = "待标定"
        entry["pending"] = True
        return {"entry": entry, "deduped": False}

    def _ensure_watermark_seed(
        self, point_key: str, shard: str, batch_key: str
    ) -> dict[str, Any]:
        """按 (测点, 分片) 创建或取回接入水位，重复上送不重复推进。"""
        wm_key = f"WATER-{batch_key}"
        existing = next(
            (r for r in store.rows("ingest_watermark") if r.get("水位编号") == wm_key),
            None,
        )
        if existing is not None:
            return {"entry": existing, "deduped": True}
        now = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
        entry = self._auto_create_seed("ingest_watermark", {
            "水位编号": wm_key,
            "测点编号": point_key,
            "源分片": shard,
            "事件时间水位": now,
            "处理时间水位": now,
            "滞后秒数": "0",
            "迟到点数": "0",
            "推进方式": "自动推进",
            "检查时间": now,
        })
        return {"entry": entry, "deduped": False}

    def complete_batch(self, batch_id: int) -> dict[str, Any]:
        """确认批次上送完成并写入上送记录到交付日志数据集，自动触发单位标定和接入水位推进。"""
        batch = store.find(self.MODULE, batch_id)
        if batch is None:
            return {"error": f"上送批次 {batch_id} 不存在"}
        batch_key = batch["批次编号"]
        shard = batch.get("分片范围", batch_key)
        point_key = self._resolve_point(batch, shard)
        batch["测点编号"] = point_key
        ledger = self._ledger()

        # 幂等：已完成批次的重复确认直接回传既有归属，不重复触发下游
        if batch.get("status") == "已完成":
            self._write_jsonl(
                self._data_dir("upload_batches") / f"{batch_key}.jsonl",
                [{"action": "complete_duplicate_ignored", "batch": batch_key,
                  "shard": shard, "point": point_key, "at": time.time()}]
            )
            # 本次确认未新建任何下游记录，回传时按本次调用语义标记为去重
            triggers = {
                name: ({**detail, "deduped": True} if isinstance(detail, dict) else detail)
                for name, detail in batch.get("last_triggers", {}).items()
            }
            return {"batch_entry": batch, "batch_key": batch_key, "point": point_key,
                    "shard": shard, "completed_at": time.time(), "deduped": True,
                    "auto_triggers": triggers}

        entry, _ = self.run_action(batch_id, "标记完成")
        ledger.mark_upload(point_key, shard, batch_key, "已完成")
        # 将上送记录写入持久化交付文件
        self._write_jsonl(
            self._data_dir("upload_batches") / f"{batch_key}.jsonl",
            [{"action": "complete", "batch": batch_key, "shard": shard, "point": point_key,
              "total_records": entry.get("记录条数", "0"), "completed_at": time.time()}]
        )
        triggers: dict[str, Any] = {}
        trigger_failed = False
        # 上送完成后自动触发单位标定（按 测点+分片 去重）
        try:
            cal_result = self._ensure_calibration_seed(point_key, shard, batch_key)
            cal_entry = cal_result["entry"]
            triggers["unit_calibrate"] = {
                "created": cal_entry["标定编号"], "point": point_key, "shard": shard,
                "status": cal_entry.get("status"), "deduped": cal_result["deduped"],
            }
        except Exception as e:
            trigger_failed = True
            triggers["unit_calibrate"] = {"error": str(e), "shard": shard, "point": point_key}
        # 上送完成后自动推进接入水位（按 测点+分片 去重）
        try:
            wm_result = self._ensure_watermark_seed(point_key, shard, batch_key)
            wm_entry = wm_result["entry"]
            triggers["ingest_watermark"] = {
                "created": wm_entry["水位编号"], "point": point_key, "shard": shard,
                "status": wm_entry.get("status"), "deduped": wm_result["deduped"],
            }
        except Exception as e:
            trigger_failed = True
            triggers["ingest_watermark"] = {"error": str(e), "shard": shard, "point": point_key}

        # 一次传输结束可能赶上下游写成果失败：批次保持已完成但标记异常，等待重试
        if trigger_failed:
            batch["abnormal"] = True
            self._write_jsonl(
                self._data_dir("upload_batches") / f"{batch_key}.jsonl",
                [{"action": "downstream_trigger_failed", "batch": batch_key,
                  "shard": shard, "point": point_key, "triggers": triggers,
                  "at": time.time()}]
            )
        batch["last_triggers"] = triggers
        return {
            "batch_entry": entry,
            "batch_key": batch_key,
            "point": point_key,
            "shard": shard,
            "completed_at": time.time(),
            "deduped": False,
            "trigger_failed": trigger_failed,
            "auto_triggers": triggers,
        }

    def retry_downstream(self, batch_id: int) -> dict[str, Any]:
        """重试已完成批次失败的下游触发（标定/水位），成功创建的部分不重复记账。"""
        batch = store.find(self.MODULE, batch_id)
        if batch is None:
            return {"error": f"上送批次 {batch_id} 不存在"}
        if batch.get("status") != "已完成":
            return {"error": f"批次 {batch['批次编号']} 当前状态 {batch.get('status')}，仅已完成批次可重试下游"}
        batch_key = batch["批次编号"]
        shard = batch.get("分片范围", batch_key)
        point_key = self._resolve_point(batch, shard)
        triggers: dict[str, Any] = {}
        try:
            cal_result = self._ensure_calibration_seed(point_key, shard, batch_key)
            triggers["unit_calibrate"] = {
                "created": cal_result["entry"]["标定编号"], "point": point_key,
                "shard": shard, "status": cal_result["entry"].get("status"),
                "deduped": cal_result["deduped"],
            }
        except Exception as e:
            triggers["unit_calibrate"] = {"error": str(e)}
        try:
            wm_result = self._ensure_watermark_seed(point_key, shard, batch_key)
            triggers["ingest_watermark"] = {
                "created": wm_result["entry"]["水位编号"], "point": point_key,
                "shard": shard, "status": wm_result["entry"].get("status"),
                "deduped": wm_result["deduped"],
            }
        except Exception as e:
            triggers["ingest_watermark"] = {"error": str(e)}
        trigger_failed = any("error" in v for v in triggers.values())
        if not trigger_failed:
            batch["abnormal"] = False
        batch["last_triggers"] = triggers
        self._write_jsonl(
            self._data_dir("upload_batches") / f"{batch_key}.jsonl",
            [{"action": "retry_downstream", "batch": batch_key, "shard": shard,
              "point": point_key, "triggers": triggers, "at": time.time()}]
        )
        return {"batch_key": batch_key, "point": point_key, "shard": shard,
                "trigger_failed": trigger_failed, "auto_triggers": triggers}

    def fail_batch(self, batch_id: int, reason: str = "") -> dict[str, Any]:
        """显式标记批次失败（模拟确认中断/传输失败），保留接收台账供重发核对。"""
        batch = store.find(self.MODULE, batch_id)
        if batch is None:
            return {"error": f"上送批次 {batch_id} 不存在"}
        entry, _ = self.run_action(batch_id, "标记失败")
        shard = batch.get("分片范围", batch["批次编号"])
        point_key = self._resolve_point(batch, shard)
        self._ledger().mark_upload(point_key, shard, batch["批次编号"], "已失败")
        self._write_jsonl(
            self._data_dir("upload_batches") / f"{batch['批次编号']}.jsonl",
            [{"action": "fail", "batch": batch["批次编号"], "shard": shard,
              "point": point_key, "reason": reason, "at": time.time()}]
        )
        return {"batch_entry": entry, "batch_key": batch["批次编号"],
                "shard": shard, "point": point_key, "reason": reason}
