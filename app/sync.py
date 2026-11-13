"""同步接入：固定时刻的完整正式结果快照 + 起始游标 + 后续变更组拉取。

整体模型
========
- 「接入」(sync access) 由后台线程构建：在一个只读事务里冻结正式库某一时刻的
  一致快照，把完整正式结果分页写入**独立影子 SQLite 文件**。初始扫描全程不占
  正式库写事务，每次只处理一页键，内存不随结果行数增长；扫描期间上传、撤销
  和取数继续可用。
- 接入就绪后，消费方先用起始游标分页读取初始数据（快照内容），再按游标拉取
  快照之后的变更组。每次拉取返回**下一完整组**及新游标，绝不拆组；
  已发布的变更组只增不改，可重复读取。
- 衔接保证：快照事务冻结了「快照时刻最大变更组 G」，初始数据恰好覆盖
  1..G 的全部效果；游标从 G 开始，后续只返回 group_id > G 的组，不漏不重。
- 重启：影子文件、游标与变更组都在磁盘上；只有未完成（building）的接入会被
  标记 failed 且不供数，ready 接入继续可用。

游标是不透明的 base64url 字符串（含 access_id，跨接入互验）：
- initial：初始分页游标，next_token 给出下一页或转为变更游标
- changes：变更游标，after_group 之后的下一组即待返回组
"""
from __future__ import annotations

import base64
import json
import os
import sqlite3
import threading

from .db import connect, utcnow_iso

DEFAULT_PAGE_SIZE = int(os.environ.get("SYNC_PAGE_SIZE", "100"))
MAX_PAGE_SIZE = int(os.environ.get("SYNC_MAX_PAGE_SIZE", "1000"))


class SyncError(Exception):
    """面向 API 的同步错误。"""


# ---------------- 游标编解码 ----------------

def encode_cursor(payload: dict) -> str:
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(token: str) -> dict:
    try:
        padding = "=" * (-len(token) % 4)
        raw = base64.urlsafe_b64decode(token + padding)
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise SyncError("游标无法解析") from exc
    if not isinstance(payload, dict) or "type" not in payload or "access_id" not in payload:
        raise SyncError("游标内容不合法")
    return payload


def start_cursor(access_id: int, snapshot_max_group: int) -> str:
    """接入的起始游标：既用于第一页初始数据，也用于拉取后续变更。"""
    return encode_cursor(
        {"type": "initial", "access_id": access_id, "after_key": None,
         "after_group": snapshot_max_group}
    )


# ---------------- 管理器 ----------------

class SyncManager:
    def __init__(self, app):
        self.app = app
        self.db_path = app.config["DB_PATH"]
        self.data_dir = app.config["DATA_DIR"]
        self._threads: dict[int, threading.Thread] = {}
        self._lock = threading.Lock()

    def shadow_path(self, access_id: int) -> str:
        return os.path.join(self.data_dir, f"sync_snapshot_{access_id}.db")

    # ---------- 生命周期 ----------

    def create_access(self, page_size: int | None = None) -> dict:
        """登记一个接入并启动后台快照构建，返回接入信息。"""
        size = page_size if page_size is not None else DEFAULT_PAGE_SIZE
        if isinstance(size, bool) or not isinstance(size, int) or not (1 <= size <= MAX_PAGE_SIZE):
            raise SyncError(f"page_size 须为 1..{MAX_PAGE_SIZE} 的整数")

        conn = connect(self.db_path)
        try:
            max_group = conn.execute(
                "SELECT COALESCE(MAX(group_id), 0) AS m FROM change_groups"
            ).fetchone()["m"]
            cur = conn.execute(
                "INSERT INTO sync_accesses (status, snapshot_max_group, page_size,"
                " created_at) VALUES ('building', ?, ?, ?)",
                (max_group, size, utcnow_iso()),
            )
            access_id = cur.lastrowid
            path = self.shadow_path(access_id)
            conn.execute(
                "UPDATE sync_accesses SET shadow_path = ? WHERE access_id = ?",
                (path, access_id),
            )
            conn.commit()
        finally:
            conn.close()

        self._spawn(access_id)
        return self.get_access(access_id)

    def resume_on_startup(self) -> None:
        """重启恢复：未完成（building）的接入一律标 failed 且不供数。

        快照构建不做断点续建——构建期间正式库照常服务，重新发起接入即可获得
        新时刻的完整快照。
        """
        conn = connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT access_id, shadow_path FROM sync_accesses WHERE status = 'building'"
            ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE sync_accesses SET status = 'failed',"
                    " error = '进程重启时接入尚未完成，已标记失败，请重新创建',"
                    " ready_at = NULL WHERE access_id = ?",
                    (row["access_id"],),
                )
                self._remove_shadow(row["shadow_path"])
            conn.commit()
        finally:
            conn.close()

    def list_accesses(self) -> list[dict]:
        conn = connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT * FROM sync_accesses ORDER BY access_id DESC"
            ).fetchall()
            out = []
            for r in rows:
                item = dict(r)
                item["cursor"] = (
                    start_cursor(r["access_id"], r["snapshot_max_group"])
                    if r["status"] == "ready"
                    else None
                )
                out.append(item)
            return out
        finally:
            conn.close()

    def get_access(self, access_id: int) -> dict | None:
        conn = connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT * FROM sync_accesses WHERE access_id = ?", (access_id,)
            ).fetchone()
            if row is None:
                return None
            item = dict(row)
            if row["status"] == "ready":
                item["cursor"] = start_cursor(access_id, row["snapshot_max_group"])
                item["rows_done"] = row["snapshot_rows"]
            elif row["status"] == "building":
                item["rows_done"] = self._shadow_count(row["shadow_path"])
            else:
                item["rows_done"] = 0
            return item
        finally:
            conn.close()

    # ---------- 后台构建：初始快照（不占正式库写事务） ----------

    def _spawn(self, access_id: int) -> None:
        with self._lock:
            t = self._threads.get(access_id)
            if t is not None and t.is_alive():
                return
            t = threading.Thread(
                target=self._run_guarded, args=(access_id,), daemon=True
            )
            self._threads[access_id] = t
        t.start()

    def _run_guarded(self, access_id: int) -> None:
        with self.app.app_context():
            try:
                self._build(access_id)
            except Exception as exc:  # 构建失败：标 failed，不供数
                self.app.logger.exception("sync access %s build failed", access_id)
                self._mark_failed(access_id, exc)

    def _build(self, access_id: int) -> None:
        access = self.get_access(access_id)
        if access is None or access["status"] != "building":
            return
        path = access["shadow_path"]
        page_size = access["page_size"]

        # 影子库：完整正式结果的分页落点，构建期间与正式库完全解耦
        if os.path.exists(path):
            os.remove(path)
        sh = sqlite3.connect(path)
        sh.execute("PRAGMA journal_mode=WAL")
        sh.execute(
            """
            CREATE TABLE snapshot_results (
                station      TEXT NOT NULL,
                sample_day   TEXT NOT NULL,
                analyte      TEXT NOT NULL,
                result       REAL NOT NULL,
                source_batch INTEGER NOT NULL,
                PRIMARY KEY (station, sample_day, analyte)
            )
            """
        )
        sh.commit()

        # 只读长事务冻结正式库某一时刻的一致快照。WAL 下读不阻塞写：
        # 期间上传/撤销的写事务照常提交，本连接始终读到冻结时刻的状态。
        main = sqlite3.connect(self.db_path, timeout=30)
        main.row_factory = sqlite3.Row
        main.isolation_level = None
        main.execute("PRAGMA query_only = ON")
        main.execute("BEGIN")
        try:
            frozen_max_group = main.execute(
                "SELECT COALESCE(MAX(group_id), 0) AS m FROM change_groups"
            ).fetchone()["m"]

            last_key: tuple[str, str, str] | None = None
            total = 0
            while True:
                # keyset 分页：每页只持有一页数据，内存不随总行数增长
                if last_key is None:
                    page = main.execute(
                        "SELECT station, sample_day, analyte, result, source_batch"
                        " FROM current_results"
                        " ORDER BY station, sample_day, analyte LIMIT ?",
                        (page_size,),
                    ).fetchall()
                else:
                    page = main.execute(
                        "SELECT station, sample_day, analyte, result, source_batch"
                        " FROM current_results"
                        " WHERE (station, sample_day, analyte) > (?, ?, ?)"
                        " ORDER BY station, sample_day, analyte LIMIT ?",
                        (last_key[0], last_key[1], last_key[2], page_size),
                    ).fetchall()
                if not page:
                    break
                sh.executemany(
                    "INSERT INTO snapshot_results"
                    " (station, sample_day, analyte, result, source_batch)"
                    " VALUES (?, ?, ?, ?, ?)",
                    [tuple(r) for r in page],
                )
                sh.commit()
                total += len(page)
                last_key = (page[-1]["station"], page[-1]["sample_day"], page[-1]["analyte"])
        finally:
            main.execute("COMMIT")  # 释放快照，正式库 WAL 可正常 checkpoint
            main.close()

        # 构建全程未写正式库；就绪标记是整个构建中唯一一次正式库写事务
        conn = connect(self.db_path)
        try:
            # 以后台只读事务冻结时刻的组号为准更新边界（登记后、构建前
            # 若有新提交，这里把边界推进到冻结值，保证不漏更）
            cur = conn.execute(
                "UPDATE sync_accesses SET status = 'ready', snapshot_rows = ?,"
                " snapshot_max_group = ?, ready_at = ?"
                " WHERE access_id = ? AND status = 'building'",
                (total, frozen_max_group, utcnow_iso(), access_id),
            )
            if cur.rowcount == 0:
                raise SyncError("接入已不在 building 状态（可能被重启恢复标记失败）")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
            sh.close()

    def _mark_failed(self, access_id: int, exc: Exception) -> None:
        conn = connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT shadow_path FROM sync_accesses WHERE access_id = ?", (access_id,)
            ).fetchone()
            if row is None:
                return
            conn.execute(
                "UPDATE sync_accesses SET status = 'failed', error = ? WHERE access_id = ?",
                (repr(exc), access_id),
            )
            conn.commit()
            self._remove_shadow(row["shadow_path"])
        finally:
            conn.close()

    @staticmethod
    def _remove_shadow(path: str | None) -> None:
        if not path:
            return
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(path + suffix)
            except OSError:
                pass

    @staticmethod
    def _shadow_count(path: str | None) -> int:
        if not path or not os.path.exists(path):
            return 0
        try:
            sh = sqlite3.connect(path)
            try:
                return sh.execute("SELECT COUNT(*) FROM snapshot_results").fetchone()[0]
            finally:
                sh.close()
        except sqlite3.Error:
            return 0

    def _open_shadow(self, access: dict) -> sqlite3.Connection:
        path = access["shadow_path"]
        if not path or not os.path.exists(path):
            raise SyncError("快照文件缺失，接入不可用，请重新创建")
        sh = sqlite3.connect(path)
        sh.row_factory = sqlite3.Row
        return sh

    # ---------- 取数：初始分页 ----------

    def read_initial_page(self, access_id: int, cursor_token: str | None,
                          limit: int | None) -> dict:
        access = self._require_ready(access_id)
        if cursor_token is None:
            cursor_token = start_cursor(access_id, access["snapshot_max_group"])
        cursor = decode_cursor(cursor_token)
        self._check_cursor(cursor, access_id, "initial")
        page_size = self._resolve_limit(limit, access["page_size"])

        sh = self._open_shadow(access)
        try:
            after_key = cursor.get("after_key")
            if after_key is None:
                rows = sh.execute(
                    "SELECT station, sample_day, analyte, result, source_batch"
                    " FROM snapshot_results"
                    " ORDER BY station, sample_day, analyte LIMIT ?",
                    (page_size,),
                ).fetchall()
            else:
                rows = sh.execute(
                    "SELECT station, sample_day, analyte, result, source_batch"
                    " FROM snapshot_results"
                    " WHERE (station, sample_day, analyte) > (?, ?, ?)"
                    " ORDER BY station, sample_day, analyte LIMIT ?",
                    (after_key[0], after_key[1], after_key[2], page_size),
                ).fetchall()

            records = [dict(r) for r in rows]
            if len(rows) == page_size:
                last = rows[-1]
                next_cursor = encode_cursor(
                    {"type": "initial", "access_id": access_id,
                     "after_key": [last["station"], last["sample_day"], last["analyte"]],
                     "after_group": access["snapshot_max_group"]}
                )
                phase = "initial"
            else:
                # 初始数据读完：游标无缝切换为变更游标，after_group 停在快照组号
                next_cursor = encode_cursor(
                    {"type": "changes", "access_id": access_id,
                     "after_group": access["snapshot_max_group"]}
                )
                phase = "changes"
        finally:
            sh.close()

        return {
            "access_id": access_id,
            "phase": "initial",
            "next_phase": phase,
            "page_size": page_size,
            "records": records,
            "next_cursor": next_cursor,
            "snapshot_max_group": access["snapshot_max_group"],
        }

    # ---------- 取数：后续变更组（每次恰好一个完整组） ----------

    def read_next_changes(self, access_id: int, cursor_token: str | None) -> dict:
        access = self._require_ready(access_id)
        if cursor_token is None:
            cursor = {"type": "changes", "access_id": access_id,
                      "after_group": access["snapshot_max_group"]}
        else:
            cursor = decode_cursor(cursor_token)
        self._check_cursor(cursor, access_id, "changes")
        after_group = int(cursor["after_group"])

        conn = connect(self.db_path)
        try:
            group = conn.execute(
                "SELECT * FROM change_groups WHERE group_id > ?"
                " ORDER BY group_id LIMIT 1",
                (after_group,),
            ).fetchone()
            if group is None:
                # 没有新组：返回同一游标，可安全轮询、重复读取
                return {
                    "access_id": access_id,
                    "phase": "changes",
                    "group": None,
                    "entries": [],
                    "next_cursor": encode_cursor(
                        {"type": "changes", "access_id": access_id,
                         "after_group": after_group}
                    ),
                }
            entries = conn.execute(
                "SELECT change_type, station, sample_day, analyte, result, source_batch"
                " FROM change_entries WHERE group_id = ? ORDER BY seq",
                (group["group_id"],),
            ).fetchall()
        finally:
            conn.close()

        next_cursor = encode_cursor(
            {"type": "changes", "access_id": access_id,
             "after_group": group["group_id"]}
        )
        return {
            "access_id": access_id,
            "phase": "changes",
            "group": dict(group),
            "entries": [dict(e) for e in entries],
            "next_cursor": next_cursor,
        }

    # ---------- 辅助 ----------

    def _require_ready(self, access_id: int) -> dict:
        access = self.get_access(access_id)
        if access is None:
            raise LookupError(f"接入 {access_id} 不存在")
        if access["status"] == "building":
            raise SyncError("接入仍在构建初始快照，暂不可取数")
        if access["status"] == "failed":
            raise SyncError(f"接入构建失败，不供数：{access.get('error') or '未知错误'}")
        return access

    @staticmethod
    def _check_cursor(cursor: dict, access_id: int, expected_type: str) -> None:
        if cursor.get("access_id") != access_id:
            raise SyncError("游标不属于该接入")
        if cursor.get("type") != expected_type:
            raise SyncError(
                f"游标类型不匹配：该接口需要 {expected_type} 游标，"
                f"实际为 {cursor.get('type')}"
            )

    @staticmethod
    def _resolve_limit(limit: int | None, default: int) -> int:
        if limit is None:
            return default
        if isinstance(limit, bool) or not isinstance(limit, int) or not (1 <= limit <= MAX_PAGE_SIZE):
            raise SyncError(f"limit 须为 1..{MAX_PAGE_SIZE} 的整数")
        return limit
