"""边缘缓冲领域服务：维护边缘侧高频采样数据的分片落盘缓冲与分片水位推进。
按分片窗口采集采样点数据写入 JSONL 持久化文件，落盘完成后触发上送批次创建。

同一测点先后上送多个分片时，由 ``open_shard`` 在同一测点下追加新的缓冲分片，
每个分片独立维护采样文件与水位；落盘触发上送时携带真实测点编号，并对已生成
过的上送批次去重，避免边缘重发产生重复记账。"""
from __future__ import annotations

import json
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class EdgeBufferService(BaseModuleService):
    MODULE = "edge_buffer"
    ENTITY = "缓冲分片"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["分片编号", "测点编号", "起始时刻", "结束时刻", "采样点数", "缓冲字节", "落盘方式", "分片水位"]
    REQUIRED_FIELDS = ["分片编号", "测点编号", "起始时刻"]
    STATUS_ORDER = ["草稿", "缓冲中", "已落盘", "已上送"]
    ACTION_RULES = {"启动落盘": "已落盘", "确认上送": "已上送", "标记丢失": "已落盘"}
    NEGATIVE_ACTIONS = []

    def open_shard(self, point_id: int, shard_key: str) -> dict[str, Any]:
        """在已投运测点下追加一个新的缓冲分片（同一测点的第二、第三个分片）。

        分片编号在 edge_buffer 内全局唯一；重复打开同一分片时幂等返回既有记录，
        保证边缘侧重连后再次打开不会重复建账。
        """
        point_entry = store.find("sensor_point", point_id)
        if point_entry is None:
            return {"error": f"测点档案 {point_id} 不存在"}
        point_key = point_entry["测点编号"]
        existing = next(
            (r for r in store.rows(self.MODULE) if r.get("分片编号") == shard_key),
            None,
        )
        if existing is not None:
            return {"buffer_entry": existing, "point_key": point_key, "deduped": True}
        entry, missing = self.create_entry({
            "分片编号": shard_key,
            "测点编号": point_key,
            "起始时刻": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
            "结束时刻": "",
            "采样点数": "0",
            "缓冲字节": "0",
            "落盘方式": "定时落盘",
            "分片水位": "0",
        })
        if entry is None:
            return {"error": f"分片登记缺少必填字段：{'、'.join(missing)}"}
        # 新建分片进入缓冲中，等待采集与落盘
        entry["status"] = "缓冲中"
        entry["pending"] = True
        self._write_jsonl(
            self._data_dir("edge_flush") / f"{shard_key}_lifecycle.jsonl",
            [{"action": "open_shard", "shard": shard_key, "point": point_key,
              "at": time.time()}]
        )
        return {"buffer_entry": entry, "point_key": point_key, "deduped": False}

    def collect_samples(self, buffer_id: int, sample_count: int = 50) -> dict[str, Any]:
        """按分片窗口采集传感器高频采样数据写入边缘缓冲文件，推进分片落盘水位。"""
        buffer_entry = store.find(self.MODULE, buffer_id)
        if buffer_entry is None:
            # 数据采集操作：写入边缘采样数据JSONL文件推进分片落盘水位
            return {"error": f"缓冲分片 {buffer_id} 不存在"}
        shard_key = buffer_entry["分片编号"]
        point_key = buffer_entry["测点编号"]
        # 读取上次采集的检查点水位（按分片独立记账）
        cp = self._read_checkpoint(f"edge_{shard_key}") or {"offset": 0, "sample_count": 0}
        base_time = time.time() - sample_count * 0.1
        samples = []
        for i in range(sample_count):
            ts = base_time + i * 0.1
            samples.append({
                "shard": shard_key,
                "point": point_key,
                "sample_index": cp["sample_count"] + i + 1,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.", time.localtime(ts)) + f"{int((ts % 1) * 1000000):06d}",
                "raw_value": round(23.5 + (i % 7) * 0.3, 3),
                "unit": "mV",
            })
        # 写入边缘采样数据 JSONL 文件
        output_file = self._data_dir("edge_samples") / f"{shard_key}.jsonl"
        written = self._write_jsonl(output_file, samples)
        # 更新分片水位检查点
        new_count = cp["sample_count"] + written
        self._write_checkpoint(f"edge_{shard_key}", {
            "shard_key": shard_key,
            "point_key": point_key,
            "sample_count": new_count,
            "offset": cp["offset"] + sum(1 for _ in samples),
            "last_collected_at": time.time(),
        })
        buffer_entry["采样点数"] = str(new_count)
        buffer_entry["缓冲字节"] = str(sum(len(str(s).encode()) for s in samples))
        buffer_entry["分片水位"] = str(new_count)
        return {
            "shard_key": shard_key,
            "point_key": point_key,
            "sample_count": sample_count,
            "total_samples": new_count,
            "written": written,
        }

    def flush_to_disk(self, buffer_id: int) -> dict[str, Any]:
        """将缓冲分片落盘到采集数据文件，完成落盘后自动触发上送批次创建。"""
        buffer_entry = store.find(self.MODULE, buffer_id)
        if buffer_entry is None:
            return {"error": f"缓冲分片 {buffer_id} 不存在"}
        shard_key = buffer_entry["分片编号"]
        point_key = buffer_entry["测点编号"]
        buf_file = self._data_dir("edge_samples") / f"{shard_key}.jsonl"
        records = self._read_jsonl(buf_file)
        batch_key = f"UPLO-{shard_key}"
        # 边缘重发：同一分片已落盘并生成过批次时不重复建账，只回传既有归属
        existing_batch = next(
            (r for r in store.rows("upload_batch") if r.get("批次编号") == batch_key),
            None,
        )
        triggers: dict[str, Any] = {}
        if existing_batch is not None:
            self._write_jsonl(
                self._data_dir("edge_flush") / f"{shard_key}_flush.jsonl",
                [{"action": "flush_dedup_skipped", "shard": shard_key, "point": point_key,
                  "batch": batch_key, "at": time.time()}]
            )
            triggers["upload_batch"] = {
                "created": batch_key, "deduped": True,
                "status": existing_batch.get("status"), "records": len(records),
            }
            return {"buffer_entry": buffer_entry, "records_flushed": len(records),
                    "deduped": True, "auto_triggers": triggers}
        # 标记落盘
        entry, _ = self.run_action(buffer_id, "启动落盘")
        # 写入落盘日志文件记录批次信息
        self._write_jsonl(
            self._data_dir("edge_flush") / f"{shard_key}_flush.jsonl",
            [{"action": "flush", "shard": shard_key, "point": point_key,
              "records": len(records), "at": time.time()}]
        )
        buffer_entry["结束时刻"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
        # 落盘后自动触发批量上送
        try:
            self._safe_import("app.services.upload_batch", "UploadBatchService")()
            up_entry = self._auto_create_seed("upload_batch", {
                "批次编号": batch_key,
                "分片范围": shard_key,
                "测点编号": point_key,
                "测点数量": "1",
                "记录条数": str(len(records)),
                "压缩方式": "gzip",
                "批次校验值": self._compute_checksum(json.dumps(records, ensure_ascii=False, default=str)),
                "上送时长": "0",
                "批次水位": "0",
            })
            if up_entry:
                triggers["upload_batch"] = {"created": batch_key, "records": len(records),
                                            "deduped": False}
        except Exception as e:
            triggers["upload_batch"] = {"error": str(e)}
        return {"buffer_entry": entry, "records_flushed": len(records),
                "deduped": False, "auto_triggers": triggers}
