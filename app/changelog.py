"""变更日志：把每次导入/撤销中「真正变化的键」记成一个递增编号的变更组。

语义
----
- 一次导入或撤销在同一个数据库事务内提交：正式结果变更 + 一个变更组，
  失败整体回滚，失败操作不产生任何变更。
- 只有真正变化的键才进组：
    * upsert：该键不存在，或 result / source_batch 与旧值不同 -> 存完整新记录；
    * delete：撤销后该键再无任何有效批次 -> 只存键，不存值；
    * 值与来源批次都没变的记录不产生变更。
- 变更组只增不改：已发布的组永久可读、可重复读取。
- 组编号由 SQLite AUTOINCREMENT 单调递增，即使某次事务回滚也不会重用。
"""
from __future__ import annotations

import sqlite3

from .db import utcnow_iso


def _insert_group(conn: sqlite3.Connection, kind: str, ref_id: int, now: str) -> int:
    cur = conn.execute(
        "INSERT INTO change_groups (kind, ref_id, entry_count, created_at)"
        " VALUES (?, ?, 0, ?)",
        (kind, ref_id, now),
    )
    return cur.lastrowid


def record_import_changes(
    conn: sqlite3.Connection,
    batch_id: int,
    records: list[dict],
    old_source: dict[tuple[str, str, str], int | None],
) -> int | None:
    """在当前事务内为一次导入写变更组，返回组号；没有真正变化则不建组、返回 None。

    必须在 import_batch 的 upsert 之后、commit 之前调用，与正式结果同事务提交。
    ``old_source`` 是 upsert 之前抓取的各键旧来源批次（新键为 None）：
    只有旧来源批次与本次不同的键才真正被 upsert 接受，属于真正变化；
    未被接受（旧来源批次更大）的键不产变更。
    """
    changed: list[dict] = []
    for r in records:
        key = (r["station"], r["sample_day"], r["analyte"])
        if old_source.get(key) != batch_id:
            changed.append(r)

    if not changed:
        return None

    now = utcnow_iso()
    group_id = _insert_group(conn, "import", batch_id, now)
    conn.executemany(
        "INSERT INTO change_entries"
        " (group_id, change_type, station, sample_day, analyte, result, source_batch, seq)"
        " VALUES (?, 'upsert', ?, ?, ?, ?, ?, ?)",
        [
            (
                group_id,
                r["station"],
                r["sample_day"],
                r["analyte"],
                r["result"],
                batch_id,
                i,
            )
            for i, r in enumerate(changed)
        ],
    )
    conn.execute(
        "UPDATE change_groups SET entry_count = ? WHERE group_id = ?",
        (len(changed), group_id),
    )
    return group_id


def record_revoke_changes(
    conn: sqlite3.Connection, job_id: int, old_temp_table: str
) -> int | None:
    """在当前事务内为一次撤销写变更组，返回组号；无变化则不建组、返回 None。

    调用时机：current_results 已被重写为撤销后的新状态，旧状态保存在
    临时表 ``old_temp_table``（结构与 current_results 相同）。
    """
    diff_count = conn.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT o.station, o.sample_day, o.analyte
            FROM {old_temp_table} o
            LEFT JOIN current_results c
              ON c.station = o.station
             AND c.sample_day = o.sample_day
             AND c.analyte = o.analyte
            WHERE c.station IS NULL
               OR c.result <> o.result
               OR c.source_batch <> o.source_batch
            UNION ALL
            SELECT c.station, c.sample_day, c.analyte
            FROM current_results c
            LEFT JOIN {old_temp_table} o
              ON o.station = c.station
             AND o.sample_day = c.sample_day
             AND o.analyte = c.analyte
            WHERE o.station IS NULL
        )
        """
    ).fetchone()[0]
    if diff_count == 0:
        return None

    now = utcnow_iso()
    group_id = _insert_group(conn, "revoke", job_id, now)

    # 新出现或值/来源批次变化的键 -> upsert，携带完整新记录
    conn.execute(
        f"""
        INSERT INTO change_entries
            (group_id, change_type, station, sample_day, analyte,
             result, source_batch, seq)
        SELECT ?, 'upsert', c.station, c.sample_day, c.analyte,
               c.result, c.source_batch,
               ROW_NUMBER() OVER (ORDER BY c.station, c.sample_day, c.analyte) - 1
        FROM current_results c
        LEFT JOIN {old_temp_table} o
          ON o.station = c.station
         AND o.sample_day = c.sample_day
         AND o.analyte = c.analyte
        WHERE o.station IS NULL
           OR c.result <> o.result
           OR c.source_batch <> o.source_batch
        """,
        (group_id,),
    )
    # 新状态中消失的键 -> delete，只存键
    conn.execute(
        f"""
        INSERT INTO change_entries
            (group_id, change_type, station, sample_day, analyte,
             result, source_batch, seq)
        SELECT ?, 'delete', o.station, o.sample_day, o.analyte,
               NULL, NULL,
               (SELECT COUNT(*) FROM change_entries WHERE group_id = ?)
               + ROW_NUMBER() OVER (ORDER BY o.station, o.sample_day, o.analyte) - 1
        FROM {old_temp_table} o
        LEFT JOIN current_results c
          ON c.station = o.station
         AND c.sample_day = o.sample_day
         AND c.analyte = c.analyte
        WHERE c.station IS NULL
        """,
        (group_id, group_id),
    )
    total = conn.execute(
        "SELECT COUNT(*) FROM change_entries WHERE group_id = ?", (group_id,)
    ).fetchone()[0]
    conn.execute(
        "UPDATE change_groups SET entry_count = ? WHERE group_id = ?", (total, group_id)
    )
    return group_id
