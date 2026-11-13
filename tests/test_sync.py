"""增量同步接入的端到端语义测试（标准库 unittest）。

覆盖需求：
1. 创建接入 -> 后台固定时刻快照就绪，分页读完初始数据
2. 初始数据覆盖快照时刻全部正式结果（一致性）
3. 初始阶段与变更阶段衔接不漏更、不重复跨界
4. 每次拉取返回下一「完整组」及新游标，不拆组；已发布组可重复读
5. 导入/撤销只把真正变化的键记成一个递增编号变更组，与正式结果同事务：
   upsert 返回完整新记录，删除只返回键，未变记录与失败操作不产变更
6. 快照构建不阻塞上传/撤销/取数，内存按页增长（页数与页大小可观察）
7. 重启：ready 接入保留供数，building 接入标失败且不供数
8. 游标校验：类型不匹配、跨接入、非法游标均被拒绝
9. 重放不变量：初始快照 + 顺序应用变更组 == 任意时刻正式库
"""
import csv
import io
import os
import shutil
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app.rebuild as rebuild_mod  # noqa: E402
from app.server import create_app  # noqa: E402
from app.sync import decode_cursor  # noqa: E402

rebuild_mod.PAGE_SIZE = 2
rebuild_mod.REBUILD_SLEEP = 0


def csv_bytes(rows, header=("站点", "采样日", "检测项目", "结果")):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")


class SyncBaseCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        os.environ["DATA_DIR"] = self.tmp
        self.db_path = os.path.join(self.tmp, "test.db")
        self.app = create_app(self.db_path)
        self.client = self.app.test_client()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def upload(self, rows, name="t.csv"):
        data = {"file": (io.BytesIO(csv_bytes(rows)), name)}
        return self.client.post(
            "/api/upload", data=data, content_type="multipart/form-data"
        )

    def results_map(self):
        r = self.client.get("/api/results").get_json()
        return {(x["station"], x["sample_day"], x["analyte"]): x for x in r}

    def create_access(self, page_size=2):
        r = self.client.post("/api/sync/accesses", json={"page_size": page_size})
        self.assertEqual(r.status_code, 200, r.get_json())
        access_id = r.get_json()["access"]["access_id"]
        return access_id

    def wait_access(self, access_id, timeout=20):
        deadline = time.time() + timeout
        while time.time() < deadline:
            a = self.client.get(f"/api/sync/accesses/{access_id}").get_json()
            if a["status"] in ("ready", "failed"):
                return a
            time.sleep(0.02)
        self.fail("access did not finish in time")

    def wait_job(self, timeout=20):
        deadline = time.time() + timeout
        while time.time() < deadline:
            jobs = self.client.get("/api/jobs").get_json()
            if jobs and jobs[0]["status"] in ("done", "failed"):
                return jobs[0]
            time.sleep(0.02)
        self.fail("job did not finish in time")

    def revoke(self, batch_id):
        return self.client.post("/api/revoke", json={"batch_id": batch_id})

    def read_all_initial(self, access_id):
        """翻完初始数据，返回 (记录列表, 变更阶段游标)。"""
        records = []
        cursor = None
        while True:
            url = f"/api/sync/accesses/{access_id}/initial"
            if cursor:
                url += f"?cursor={cursor}"
            page = self.client.get(url).get_json()
            records.extend(page["records"])
            cursor = page["next_cursor"]
            if page["next_phase"] == "changes":
                return records, cursor

    def pull_all_groups(self, access_id, change_cursor):
        """从给定游标开始拉完所有已发布组，返回 (组列表, 最终游标)。"""
        groups = []
        while True:
            ch = self.client.get(
                f"/api/sync/accesses/{access_id}/changes?cursor={change_cursor}"
            ).get_json()
            if ch["group"] is None:
                return groups, ch["next_cursor"]
            groups.append(ch)
            change_cursor = ch["next_cursor"]

    def replay(self, initial_records, groups):
        """把初始快照与变更组顺序重放为键 -> (result, source_batch)。"""
        state = {
            (r["station"], r["sample_day"], r["analyte"]):
                (r["result"], r["source_batch"])
            for r in initial_records
        }
        for g in groups:
            for e in g["entries"]:
                key = (e["station"], e["sample_day"], e["analyte"])
                if e["change_type"] == "delete":
                    state.pop(key, None)
                else:
                    state[key] = (e["result"], e["source_batch"])
        return state


class TestAccessLifecycle(SyncBaseCase):
    def test_create_building_then_ready_with_snapshot(self):
        self.upload([("S1", "d", "PM2.5", 1.0), ("S1", "d", "SO2", 2.0)])
        access_id = self.create_access(page_size=1)
        # 创建瞬间允许处于 building，且 building 阶段不供数
        a0 = self.client.get(f"/api/sync/accesses/{access_id}").get_json()
        self.assertIn(a0["status"], ("building", "ready"))
        if a0["status"] == "building":
            resp = self.client.get(f"/api/sync/accesses/{access_id}/initial")
            self.assertEqual(resp.status_code, 409)

        a = self.wait_access(access_id)
        self.assertEqual(a["status"], "ready")
        self.assertEqual(a["snapshot_rows"], 2)
        self.assertTrue(a["cursor"])
        # 页大小为 1：2 行应翻 2 页，验证分页与内存按页推进
        records, change_cursor = self.read_all_initial(access_id)
        self.assertEqual(len(records), 2)
        self.assertEqual(
            decode_cursor(change_cursor)["type"], "changes"
        )

    def test_snapshot_matches_current_results_at_fixed_point(self):
        self.upload([("S1", "d", "PM2.5", 1.0), ("S2", "d", "NO2", 3.0)])
        self.upload([("S1", "d", "PM2.5", 9.0)])
        expected = {
            k: (v["result"], v["source_batch"]) for k, v in self.results_map().items()
        }
        access_id = self.create_access()
        self.wait_access(access_id)
        records, _ = self.read_all_initial(access_id)
        snap = {
            (r["station"], r["sample_day"], r["analyte"]):
                (r["result"], r["source_batch"])
            for r in records
        }
        self.assertEqual(snap, expected)


class TestChangeGroups(SyncBaseCase):
    def test_import_group_contains_only_real_changes(self):
        self.upload([("S1", "d", "PM2.5", 1.0), ("S1", "d", "SO2", 2.0)])  # b1
        self.upload([("S1", "d", "PM2.5", 9.0), ("S2", "d", "NO2", 7.0)])  # b2
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        groups = conn.execute(
            "SELECT group_id, kind, ref_id, entry_count FROM change_groups ORDER BY group_id"
        ).fetchall()
        # 每次有效导入一个递增组
        self.assertEqual([g[0] for g in groups], [1, 2])
        self.assertTrue(all(g[1] == "import" for g in groups))
        entries = conn.execute(
            "SELECT group_id, change_type, station, analyte, result, source_batch"
            " FROM change_entries ORDER BY group_id, seq"
        ).fetchall()
        conn.close()
        # b1：两个新键 upsert；b2：一个同键更新 + 一个新键，均为 upsert 完整记录
        self.assertEqual(len(entries), 4)
        self.assertTrue(all(e[1] == "upsert" for e in entries))
        b2_entries = [e for e in entries if e[0] == 2]
        pm = next(e for e in b2_entries if e[3] == "PM2.5")
        self.assertEqual((pm[4], pm[5]), (9.0, 2))

    def test_failed_import_produces_no_group(self):
        self.upload([("S1", "d", "PM2.5", 1.0)])
        # 批内重复键 -> 整批拒绝
        resp = self.upload([("S1", "d", "PM2.5", 2.0), ("S1", "d", "PM2.5", 3.0)])
        self.assertEqual(resp.status_code, 400)
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        n_groups = conn.execute("SELECT COUNT(*) FROM change_groups").fetchone()[0]
        n_batches = conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0]
        conn.close()
        self.assertEqual((n_groups, n_batches), (1, 1))

    def test_revoke_group_upsert_and_delete_entries(self):
        self.upload([("S1", "d", "PM2.5", 1.0), ("S1", "d", "SO2", 2.0)])  # b1
        self.upload([("S1", "d", "PM2.5", 9.0), ("S2", "d", "NO2", 7.0)])  # b2 坏批
        self.upload([("S1", "d", "PM2.5", 3.0)])                            # b3 修正
        # 接入建在撤销之前：撤销组将作为后续变更组被完整拉取
        access_id = self.create_access()
        self.wait_access(access_id)
        initial, cursor = self.read_all_initial(access_id)

        self.revoke(2)
        job = self.wait_job()
        self.assertEqual(job["status"], "done")

        groups, _ = self.pull_all_groups(access_id, cursor)
        revoke_groups = [g for g in groups if g["group"]["kind"] == "revoke"]
        self.assertEqual(len(revoke_groups), 1)
        entries = revoke_groups[0]["entries"]
        # 撤销 b2 前 PM2.5 已被 b3 修正为 3.0、SO2 来自 b1：撤销 b2 后这两个键
        # 的值与来源批次都不变 -> 不产变更；只有坏批独有键 NO2 消失 -> delete 只存键。
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["change_type"], "delete")
        self.assertEqual(entries[0]["analyte"], "NO2")
        self.assertIsNone(entries[0]["result"])
        self.assertIsNone(entries[0]["source_batch"])
        # 整组在一次拉取中返回，未拆组
        self.assertEqual(len(entries), revoke_groups[0]["group"]["entry_count"])
        # 重放与正式库一致
        self.assertEqual(
            self.replay(initial, groups),
            {k: (v["result"], v["source_batch"]) for k, v in self.results_map().items()},
        )

    def test_revoke_group_includes_upsert_when_value_changes(self):
        self.upload([("S1", "d", "PM2.5", 1.0)])                    # b1
        self.upload([("S1", "d", "PM2.5", 9.0), ("S2", "d", "NO2", 7.0)])  # b2 坏批
        access_id = self.create_access()
        self.wait_access(access_id)
        initial, cursor = self.read_all_initial(access_id)

        self.revoke(2)
        job = self.wait_job()
        self.assertEqual(job["status"], "done")
        groups, _ = self.pull_all_groups(access_id, cursor)
        entries = groups[0]["entries"]
        # PM2.5 从 b2 的 9.0 回退到 b1 的 1.0 -> upsert 完整新记录；NO2 -> delete
        upserts = [e for e in entries if e["change_type"] == "upsert"]
        deletes = [e for e in entries if e["change_type"] == "delete"]
        pm = next(e for e in upserts if e["analyte"] == "PM2.5")
        self.assertEqual((pm["result"], pm["source_batch"]), (1.0, 1))
        self.assertEqual(deletes[0]["analyte"], "NO2")
        self.assertEqual(
            self.replay(initial, groups),
            {k: (v["result"], v["source_batch"]) for k, v in self.results_map().items()},
        )

    def test_revoke_without_any_effective_change_produces_no_group(self):
        self.upload([("S1", "d", "K1", 1.0), ("S1", "d", "K2", 2.0)])  # b1
        self.upload([("S2", "d", "K3", 3.0)])                          # b2 独有键
        self.upload([("S2", "d", "K3", 4.0)])                          # b3 覆盖 K3
        # 撤销 b2：K3 当前已来自 b3，撤销后值与来源批次都不变 -> 无任何真正变化
        self.revoke(2)
        self.assertEqual(self.wait_job()["status"], "done")
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        kinds = [r[0] for r in conn.execute("SELECT kind FROM change_groups")]
        k3 = conn.execute(
            "SELECT result, source_batch FROM current_results WHERE analyte = 'K3'"
        ).fetchone()
        conn.close()
        self.assertNotIn("revoke", kinds)
        self.assertEqual(k3, (4.0, 3))

    def test_failed_revoke_produces_no_group_and_keeps_results(self):
        self.upload([("S1", "d", "PM2.5", 1.0)])
        self.upload([("S1", "d", "PM2.5", 9.0)])
        original = rebuild_mod.RevokeManager._swap

        def boom(self, job_id, revoked_batch, shadow_path):
            raise RuntimeError("simulated swap failure")

        rebuild_mod.RevokeManager._swap = boom
        try:
            self.revoke(2)
            job = self.wait_job()
        finally:
            rebuild_mod.RevokeManager._swap = original
        self.assertEqual(job["status"], "failed")
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM change_groups WHERE kind='revoke'").fetchone()[0],
            0,
        )
        # 正式结果未被破坏
        self.assertEqual(
            conn.execute(
                "SELECT result, source_batch FROM current_results"
            ).fetchone(),
            (9.0, 2),
        )
        conn.close()


class TestCursorPullSemantics(SyncBaseCase):
    def test_each_pull_returns_one_whole_group_and_rereadable(self):
        self.upload([("S1", "d", "PM2.5", 1.0)])   # g1
        access_id = self.create_access(page_size=10)
        self.wait_access(access_id)
        _, cursor = self.read_all_initial(access_id)

        self.upload([("S2", "d", "NO2", 2.0)])     # g2
        self.upload([("S3", "d", "CO", 3.0)])      # g3
        first = self.client.get(
            f"/api/sync/accesses/{access_id}/changes?cursor={cursor}"
        ).get_json()
        # 一次只返回一个完整组
        self.assertEqual(first["group"]["group_id"], 2)
        self.assertEqual(len(first["entries"]), first["group"]["entry_count"])
        # 同一游标重复读取得到同一个组（已发布组不可变、可重复读）
        again = self.client.get(
            f"/api/sync/accesses/{access_id}/changes?cursor={cursor}"
        ).get_json()
        self.assertEqual(again["group"]["group_id"], 2)
        self.assertEqual(
            [e["station"] for e in again["entries"]],
            [e["station"] for e in first["entries"]],
        )
        # 推进游标后返回下一组，不重不漏
        second = self.client.get(
            f"/api/sync/accesses/{access_id}/changes?cursor={first['next_cursor']}"
        ).get_json()
        self.assertEqual(second["group"]["group_id"], 3)
        third = self.client.get(
            f"/api/sync/accesses/{access_id}/changes?cursor={second['next_cursor']}"
        ).get_json()
        self.assertIsNone(third["group"])

    def test_no_loss_or_duplicate_across_initial_and_changes(self):
        # 快照前导入两批
        self.upload([("S1", "d", "PM2.5", 1.0), ("S1", "d", "SO2", 2.0)])
        self.upload([("S1", "d", "PM2.5", 9.0)])
        access_id = self.create_access(page_size=1)
        access = self.wait_access(access_id)
        # 快照恰好覆盖前两个组
        self.assertEqual(access["snapshot_max_group"], 2)
        initial, change_cursor = self.read_all_initial(access_id)

        # 快照之后：新键导入 + 撤销（产生删除）+ 再导入
        self.upload([("S9", "d", "X", 5.0)])             # b3
        self.revoke(2)
        self.wait_job()
        self.upload([("S1", "d", "SO2", 20.0)])          # b4 更新旧键
        groups, final_cursor = self.pull_all_groups(access_id, change_cursor)
        # 组号严格递增且都大于快照截止组
        gids = [g["group"]["group_id"] for g in groups]
        self.assertEqual(gids, sorted(gids))
        self.assertTrue(all(g > access["snapshot_max_group"] for g in gids))
        self.assertEqual(len(gids), len(set(gids)))  # 无重复

        # 重放结果必须与当前正式库完全一致（不漏更、不重复跨界）
        replayed = self.replay(initial, groups)
        current = {
            k: (v["result"], v["source_batch"]) for k, v in self.results_map().items()
        }
        self.assertEqual(replayed, current)

        # 最终游标再拉为空，且稳定可重复
        tail = self.client.get(
            f"/api/sync/accesses/{access_id}/changes?cursor={final_cursor}"
        ).get_json()
        self.assertIsNone(tail["group"])
        self.assertEqual(tail["next_cursor"], final_cursor)

    def test_cursor_validation(self):
        self.upload([("S1", "d", "PM2.5", 1.0)])
        a1 = self.create_access()
        self.wait_access(a1)
        a2 = self.create_access()
        self.wait_access(a2)

        # 非法游标
        resp = self.client.get(f"/api/sync/accesses/{a1}/changes?cursor=garbage")
        self.assertEqual(resp.status_code, 409)
        # 跨接入游标
        meta = self.client.get(f"/api/sync/accesses/{a1}").get_json()
        resp = self.client.get(f"/api/sync/accesses/{a2}/changes?cursor={meta['cursor']}")
        self.assertEqual(resp.status_code, 409)
        self.assertIn("不属于该接入", resp.get_json()["error"])
        # 类型不匹配：initial 游标打 changes 接口
        resp = self.client.get(f"/api/sync/accesses/{a1}/changes?cursor={meta['cursor']}")
        self.assertEqual(resp.status_code, 409)
        # changes 游标打 initial 接口
        _, change_cursor = self.read_all_initial(a1)
        resp = self.client.get(
            f"/api/sync/accesses/{a1}/initial?cursor={change_cursor}"
        )
        self.assertEqual(resp.status_code, 409)
        # 不存在的接入
        self.assertEqual(self.client.get("/api/sync/accesses/999/initial").status_code, 404)

    def test_invalid_page_size_rejected(self):
        for bad in (0, -1, 100000, "abc"):
            resp = self.client.post("/api/sync/accesses", json={"page_size": bad})
            self.assertEqual(resp.status_code, 400, bad)
        # 非法 limit
        access_id = self.create_access()
        self.wait_access(access_id)
        self.assertEqual(
            self.client.get(f"/api/sync/accesses/{access_id}/initial?limit=0").status_code,
            409,
        )
        # 接入列表接口
        listing = self.client.get("/api/sync/accesses").get_json()
        self.assertTrue(any(a["access_id"] == access_id for a in listing))


class TestAvailabilityDuringSnapshot(SyncBaseCase):
    def test_upload_revoke_and_reads_work_while_building(self):
        # 造足够多的数据，并把接入页大小调到 1，让构建处于多页进行中
        rows = [("S%d" % i, "d", "PM2.5", float(i)) for i in range(20)]
        self.upload(rows)
        access_id = self.create_access(page_size=1)
        # 构建期间读正式结果始终可用
        ok_read = self.client.get("/api/results").status_code == 200
        self.assertTrue(ok_read)
        # 构建期间上传可用并立即生效
        up = self.upload([("SNEW", "d", "CO", 42.0)])
        self.assertTrue(up.get_json()["ok"])
        self.assertIn(("SNEW", "d", "CO"), self.results_map())
        # 构建期间撤销也可用
        self.revoke(1)
        job = self.wait_job()
        self.assertEqual(job["status"], "done")
        access = self.wait_access(access_id)
        self.assertEqual(access["status"], "ready")
        # 快照冻结在创建时刻：撤销发生在快照之后，故快照仍含被撤销键的旧状态
        initial, cursor = self.read_all_initial(access_id)
        snap_keys = {(r["station"], r["sample_day"], r["analyte"]) for r in initial}
        self.assertIn(("S0", "d", "PM2.5"), snap_keys)
        # 后续变更组能追平到撤销后的当前状态
        groups, _ = self.pull_all_groups(access_id, cursor)
        self.assertEqual(self.replay(initial, groups), {
            k: (v["result"], v["source_batch"]) for k, v in self.results_map().items()
        })


class TestRestartPersistence(SyncBaseCase):
    def test_ready_survives_restart_building_marked_failed(self):
        self.upload([("S1", "d", "PM2.5", 1.0)])
        ready_id = self.create_access(page_size=10)
        ready = self.wait_access(ready_id)
        ready_cursor = ready["cursor"]

        # 手工造一个 building 接入（模拟构建中进程被 kill）
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        shadow = os.path.join(self.tmp, "sync_snapshot_2.db")
        conn.execute(
            "INSERT INTO sync_accesses (status, snapshot_max_group, page_size,"
            " shadow_path, created_at) VALUES ('building', 0, 10, ?, '2026-01-01T00:00:00+00:00')",
            (shadow,),
        )
        conn.commit()
        conn.close()
        sh = sqlite3.connect(shadow)
        sh.execute(
            "CREATE TABLE snapshot_results (station TEXT, sample_day TEXT,"
            " analyte TEXT, result REAL, source_batch INTEGER)"
        )
        sh.commit()
        sh.close()

        # 新进程接管同一数据库
        app2 = create_app(self.db_path)
        client2 = app2.test_client()
        statuses = {a["access_id"]: a["status"]
                    for a in client2.get("/api/sync/accesses").get_json()}
        self.assertEqual(statuses[ready_id], "ready")
        self.assertEqual(statuses[2], "failed")
        # 失败接入不供数且影子文件被清理
        self.assertEqual(
            client2.get("/api/sync/accesses/2/initial").status_code, 409
        )
        self.assertFalse(os.path.exists(shadow))
        # ready 接入用重启前保存的游标继续读初始数据
        page = client2.get(
            f"/api/sync/accesses/{ready_id}/initial?cursor={ready_cursor}"
        ).get_json()
        self.assertEqual(page["phase"], "initial")
        self.assertEqual(len(page["records"]), 1)

        # 重启后的新导入产生变更组，旧接入游标可继续拉取
        data = {"file": (io.BytesIO(csv_bytes([("S9", "d", "Z", 3.0)])), "x.csv")}
        client2.post("/api/upload", data=data, content_type="multipart/form-data")
        _, change_cursor = self.read_all_initial(ready_id)
        groups, _ = self.pull_all_groups(ready_id, change_cursor)
        self.assertTrue(any(
            any(e["station"] == "S9" for e in g["entries"]) for g in groups
        ))


if __name__ == "__main__":
    unittest.main(verbosity=2)
