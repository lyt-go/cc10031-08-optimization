"""失败恢复与接收/标定重叠场景集成测试。

复现操作员场景：同一测点先后上送两个分片，第一份标定写入后中断确认，
随后重送分片并继续标定。核对：
- 接收台账按 (测点, 分片) 记账，已完整接收的分片重发不重复记账；
- 校验值冲突的重发被拒绝，已接收分片不被覆盖；
- 已完成标定的样本不被未确认上送覆盖，标定出处（分片/批次/采样范围）可查询；
- 边缘落盘、上送完成触发的下游种子均按分片去重；
- 下游测点级标定文件只反映实际可处理范围，两个分片合并后无重复行。
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from app.services.base import DATA_ROOT


@pytest.fixture(autouse=True)
def setup_data_dir():
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        with patch("app.services.base.DATA_ROOT", Path(tmp)):
            import app.services.edge_buffer as eb
            import app.services.upload_batch as ub
            import app.services.unit_calibrate as uc
            import app.services.ingest_ledger as il
            for mod in (eb, ub, uc, il):
                mod.DATA_ROOT = Path(tmp)
            yield Path(tmp)


def _line_count(path: Path) -> int:
    return len(path.read_text(encoding="utf-8").strip().splitlines()) if path.exists() else 0


def _onboard_point(point_key: str) -> int:
    from app.services.sensor_point import SensorPointService
    from app.store import store
    svc = SensorPointService()
    entry = svc.onboard_sensor({
        "测点编号": point_key, "所属产线": "LINE-R", "传感器型号": "PT100",
    })
    svc.activate_point(entry["id"])
    return next(r for r in store.rows("edge_buffer") if r.get("测点编号") == point_key)["id"]


def _find_batch(batch_key: str):
    from app.store import store
    return next(r for r in store.rows("upload_batch") if r.get("批次编号") == batch_key)


def _find_cal(cal_key: str):
    from app.store import store
    return next(r for r in store.rows("unit_calibrate") if r.get("标定编号") == cal_key)


class TestShardResendAndCalibrationOverlap:
    POINT = "SENS-REC"
    SHARD1 = "EDGE-SENS-REC"   # 测点投运自动生成的首个分片
    SHARD2 = "EDGE-REC-2"      # 同测点追加的第二个分片

    def test_two_shards_interrupted_ack_resend_then_continue(self, setup_data_dir):
        from app.services.edge_buffer import EdgeBufferService
        from app.services.upload_batch import UploadBatchService
        from app.services.unit_calibrate import UnitCalibrateService
        from app.services.ingest_ledger import IngestLedger, ShardConflictError
        from app.store import store

        edge_svc = EdgeBufferService()
        batch_svc = UploadBatchService()
        cal_svc = UnitCalibrateService()
        ledger = IngestLedger()

        # 测点投运自动生成第一个分片
        shard1_buffer_id = _onboard_point(self.POINT)
        edge_svc.collect_samples(shard1_buffer_id, sample_count=20)
        flush1 = edge_svc.flush_to_disk(shard1_buffer_id)
        assert flush1["records_flushed"] == 20
        assert flush1["auto_triggers"]["upload_batch"]["deduped"] is False
        batch1_key = flush1["auto_triggers"]["upload_batch"]["created"]
        batch1 = _find_batch(batch1_key)
        # 批次必须携带真实测点编号，而不是把分片当测点
        assert batch1["测点编号"] == self.POINT
        assert batch1["分片范围"] == self.SHARD1

        # 同一测点追加第二个分片并采集（分片范围 21-40）
        point_id = next(r["id"] for r in store.rows("sensor_point") if r["测点编号"] == self.POINT)
        open2 = edge_svc.open_shard(point_id, self.SHARD2)
        assert open2["deduped"] is False
        # 重复打开同一分片必须幂等
        assert edge_svc.open_shard(point_id, self.SHARD2)["deduped"] is True
        shard2_buffer_id = open2["buffer_entry"]["id"]
        edge_svc.collect_samples(shard2_buffer_id, sample_count=20)

        # ---- 分片1：启动上送 -> 完成确认前中断 ----
        batch1_id = batch1["id"]
        submit1 = batch_svc.submit_batch(batch1_id)
        assert submit1["records"] == 20
        assert submit1["deduped"] is False
        assert submit1["point"] == self.POINT
        assert submit1["shard_state"]["sample_index_min"] == 1
        assert submit1["shard_state"]["sample_index_max"] == 20

        # 边缘重发：再次提交同一批次，内容一致 -> 幂等跳过，不重复记账
        resubmit1 = batch_svc.submit_batch(batch1_id)
        assert resubmit1["deduped"] is True
        assert resubmit1["status"] == "上送中"
        ledger_events = (setup_data_dir / "ingest_ledger" / "received.jsonl").read_text(encoding="utf-8")
        assert ledger_events.count('"event": "received"') == 1
        # 台账层对同内容重发同样幂等：直接重放接收也只记跳过事件、不新增分片
        samples1 = batch_svc._read_jsonl(
            setup_data_dir / "edge_samples" / f"{self.SHARD1}.jsonl")
        checksum1 = batch1["批次校验值"]
        replay = ledger.record_received(self.POINT, self.SHARD1, samples1, checksum1)
        assert replay["deduped"] is True
        ledger_events = (setup_data_dir / "ingest_ledger" / "received.jsonl").read_text(encoding="utf-8")
        assert ledger_events.count('"event": "received"') == 1
        assert ledger_events.count('"event": "receive_duplicate_skipped"') == 1
        assert len(ledger.received_shards(self.POINT)) == 1

        # ---- 确认完成：触发标定种子与水位种子（各一份） ----
        complete1 = batch_svc.complete_batch(batch1_id)
        assert complete1["trigger_failed"] is False
        cal1_key = complete1["auto_triggers"]["unit_calibrate"]["created"]
        wm1_key = complete1["auto_triggers"]["ingest_watermark"]["created"]
        assert complete1["auto_triggers"]["unit_calibrate"]["deduped"] is False
        cal1 = _find_cal(cal1_key)
        assert cal1["测点编号"] == self.POINT
        assert cal1["源分片"] == self.SHARD1
        assert cal1["源批次"] == batch1_key

        # 中断后重复确认必须幂等，不重复触发下游
        complete1_again = batch_svc.complete_batch(batch1_id)
        assert complete1_again["deduped"] is True
        assert complete1_again["auto_triggers"]["unit_calibrate"]["deduped"] is True
        assert sum(1 for r in store.rows("unit_calibrate") if r.get("标定编号") == cal1_key) == 1
        assert sum(1 for r in store.rows("ingest_watermark") if r.get("水位编号") == wm1_key) == 1

        # ---- 第一份标定写入成功 ----
        cal1_id = cal1["id"]
        cal_result1 = cal_svc.calibrate_values(cal1_id)
        assert cal_result1["records_calibrated"] == 20
        assert cal_result1["shard"] == self.SHARD1
        assert cal_result1["sample_index_min"] == 1
        assert cal_result1["sample_index_max"] == 20
        shard1_cal_file = setup_data_dir / "calibrated" / f"{self.POINT}_{self.SHARD1}_calibrated.jsonl"
        point_cal_file = setup_data_dir / "calibrated" / f"{self.POINT}_calibrated.jsonl"
        assert _line_count(shard1_cal_file) == 20
        # 此时下游测点级文件只能看到分片1的实际范围
        assert _line_count(point_cal_file) == 20

        # 已完成标定受保护：重复确认幂等返回既有出处，未确认上送无法覆盖
        cal_repeat = cal_svc.calibrate_values(cal1_id)
        assert cal_repeat["deduped"] is True
        assert cal_repeat["source"]["shard"] == self.SHARD1
        assert _line_count(shard1_cal_file) == 20
        assert _find_cal(cal1_key)["status"] == "已标定"

        # ---- 分片2：落盘、上送、完成 ----
        flush2 = edge_svc.flush_to_disk(shard2_buffer_id)
        assert flush2["records_flushed"] == 20
        batch2_key = flush2["auto_triggers"]["upload_batch"]["created"]
        # 边缘重复落盘不能重复建批次
        flush2_again = edge_svc.flush_to_disk(shard2_buffer_id)
        assert flush2_again["deduped"] is True
        batch2 = _find_batch(batch2_key)
        batch2_id = batch2["id"]
        submit2 = batch_svc.submit_batch(batch2_id)
        assert submit2["records"] == 20
        # 分片独立维护 1..N 采样序号，全局唯一性由 (分片, 序号) 保证
        assert submit2["shard_state"]["sample_index_min"] == 1
        assert submit2["shard_state"]["sample_index_max"] == 20
        complete2 = batch_svc.complete_batch(batch2_id)
        cal2_key = complete2["auto_triggers"]["unit_calibrate"]["created"]
        assert cal2_key != cal1_key

        # ---- 继续标定分片2，下游可处理范围扩展到 40 条且无重复 ----
        cal2_id = _find_cal(cal2_key)["id"]
        cal_result2 = cal_svc.calibrate_values(cal2_id)
        assert cal_result2["records_calibrated"] == 20
        assert cal_result2["sample_index_min"] == 1
        assert cal_result2["sample_index_max"] == 20
        assert _line_count(point_cal_file) == 40
        point_rows = [json.loads(line) for line in point_cal_file.read_text().strip().splitlines()]
        # 归属可追溯：每行带分片与采样序号，且 (分片, 序号) 全局唯一
        keys = [(r["shard"], r["sample_index"]) for r in point_rows]
        assert len(keys) == len(set(keys))
        assert {r["shard"] for r in point_rows} == {self.SHARD1, self.SHARD2}

        # ---- 操作员视角：标定出处（哪个分片/批次/采样范围）可查询 ----
        provenance = cal_svc.calibration_provenance(self.POINT)
        assert [s["shard"] for s in provenance["received_shards"]] == [self.SHARD1, self.SHARD2]
        sources = provenance["calibration_sources"]
        assert len(sources) == 2
        by_shard = {s["shard"]: s for s in sources}
        assert by_shard[self.SHARD1]["batch"] == batch1_key
        assert by_shard[self.SHARD1]["sample_index_max"] == 20
        assert by_shard[self.SHARD2]["batch"] == batch2_key
        assert by_shard[self.SHARD2]["sample_index_min"] == 1
        assert by_shard[self.SHARD2]["sample_index_max"] == 20
        assert all(s["status"] == "已标定" and s["output_file"] for s in sources)

        # ---- 篡改重发：同分片异内容必须被拒绝，已接收分片与标定成果不变 ----
        samples_file = setup_data_dir / "edge_samples" / f"{self.SHARD1}.jsonl"
        original = samples_file.read_text(encoding="utf-8")
        tampered = original.replace('"raw_value": 23.5', '"raw_value": 999.9', 1)
        samples_file.write_text(tampered, encoding="utf-8")
        # 批次需回到可重发状态（模拟边缘重新发起）
        batch_svc.fail_batch(batch1_id, reason="模拟确认中断后边缘重发")
        assert _find_batch(batch1_key)["status"] == "已失败"
        conflict = batch_svc.submit_batch(batch1_id)
        assert "冲突" in conflict["error"]
        assert conflict["status"] == "已失败"
        # 台账保留原接收记录与校验值，标定成果未被覆盖
        shard_state = ledger.get_shard_state(self.POINT, self.SHARD1)
        assert shard_state["sample_count"] == 20
        with pytest.raises(ShardConflictError):
            ledger.record_received(
                self.POINT, self.SHARD1,
                [{"sample_index": 1, "raw_value": 1}], "deadbeefchecksum")
        # 恢复原始内容后重发成功且仍然不重复记账
        samples_file.write_text(original, encoding="utf-8")
        retry = batch_svc.submit_batch(batch1_id)
        assert retry["status"] == "上送中"
        assert retry["deduped"] is True
        assert ledger.get_shard_state(self.POINT, self.SHARD1)["sample_count"] == 20
        assert _line_count(point_cal_file) == 40


class TestDownstreamTriggerFailureRetry:
    POINT = "SENS-FAIL"
    SHARD = "EDGE-FAIL-1"

    def test_complete_survives_trigger_failure_and_retry_repairs(self, setup_data_dir):
        from app.services.edge_buffer import EdgeBufferService
        from app.services.upload_batch import UploadBatchService
        from app.store import store

        edge_svc = EdgeBufferService()
        batch_svc = UploadBatchService()

        buffer_id = _onboard_point(self.POINT)
        edge_svc.collect_samples(buffer_id, sample_count=5)
        flush = edge_svc.flush_to_disk(buffer_id)
        batch_key = flush["auto_triggers"]["upload_batch"]["created"]
        batch = _find_batch(batch_key)

        batch_svc.submit_batch(batch["id"])

        # 一次传输结束赶上标定写成果（种子创建）失败
        original = batch_svc._ensure_calibration_seed

        def broken(point_key, shard, bk):
            raise RuntimeError("标定种子持久化失败")

        batch_svc._ensure_calibration_seed = broken  # type: ignore[method-assign]
        result = batch_svc.complete_batch(batch["id"])
        batch_svc._ensure_calibration_seed = original  # type: ignore[method-assign]
        assert result["trigger_failed"] is True
        assert "error" in result["auto_triggers"]["unit_calibrate"]
        # 上送交付本身不回滚，批次保持已完成并标记异常
        assert batch["status"] == "已完成"
        assert batch["abnormal"] is True

        # 重试下游：水位此前已成功，不重复创建；标定补齐
        retry = batch_svc.retry_downstream(batch["id"])
        assert retry["trigger_failed"] is False
        assert retry["auto_triggers"]["unit_calibrate"]["created"] == f"UNIT-{batch_key}"
        assert retry["auto_triggers"]["ingest_watermark"]["deduped"] is True
        assert batch["abnormal"] is False
        assert sum(1 for r in store.rows("unit_calibrate")
                   if r.get("标定编号") == f"UNIT-{batch_key}") == 1


class TestRecalibrateFromProvenance:
    POINT = "SENS-RECAL"
    SHARD = "EDGE-SENS-RECAL"

    def test_recalibrate_requires_invalidation_and_uses_persisted_source(self, setup_data_dir):
        from app.services.edge_buffer import EdgeBufferService
        from app.services.upload_batch import UploadBatchService
        from app.services.unit_calibrate import UnitCalibrateService
        from app.store import store

        edge_svc = EdgeBufferService()
        batch_svc = UploadBatchService()
        cal_svc = UnitCalibrateService()

        buffer_id = _onboard_point(self.POINT)
        edge_svc.collect_samples(buffer_id, sample_count=8)
        flush = edge_svc.flush_to_disk(buffer_id)
        batch_key = flush["auto_triggers"]["upload_batch"]["created"]
        batch = _find_batch(batch_key)
        batch_svc.submit_batch(batch["id"])
        complete = batch_svc.complete_batch(batch["id"])
        cal_key = complete["auto_triggers"]["unit_calibrate"]["created"]
        cal = _find_cal(cal_key)
        first = cal_svc.calibrate_values(cal["id"])
        assert first["deduped"] is False

        # 已标定记录不允许直接重算，必须先失效（保护已确认成果）
        blocked = cal_svc.recalibrate(cal["id"])
        assert "受保护" in blocked["error"]

        # 失效后基于持久化出处重新标定成功，出处仍指向同一分片与采样范围
        cal_svc.run_action(cal["id"], "标记失效")
        redone = cal_svc.recalibrate(cal["id"])
        assert redone["shard"] == self.SHARD
        assert redone["records_calibrated"] == 8
        sources = cal_svc.calibration_provenance(self.POINT)["calibration_sources"]
        assert len(sources) == 1
        assert sources[0]["calibration_id"] == cal_key
        assert sources[0]["shard"] == self.SHARD
        assert sources[0]["status"] == "已标定"
        point_file = setup_data_dir / "calibrated" / f"{self.POINT}_calibrated.jsonl"
        assert _line_count(point_file) == 8  # 重算不产生重复行
