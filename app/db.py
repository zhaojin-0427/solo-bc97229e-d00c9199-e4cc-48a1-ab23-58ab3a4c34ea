"""SQLite 连接管理与建表脚本。

正式库中保存六类数据：
- batches         批次元数据（active / revoking / revoked）
- raw_records     上传的原始 CSV 记录（只增不删，撤销批次永久保留但被过滤）
- current_results 正式结果表（对外可读的当前值，随导入 upsert、随撤销重建）
- revoke_jobs     撤销任务状态（支持重启接续与进度查询）
- change_groups   递增编号的变更组（每次导入/撤销中真正变化的键的集合）
- change_entries  变更条目（upsert 存完整新记录，delete 只存键）
- sync_accesses   同步接入（固定时刻的完整正式结果快照 + 起始游标）
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone

from flask import current_app, g

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    filename    TEXT NOT NULL,
    row_count   INTEGER NOT NULL,
    status      TEXT NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'revoking', 'revoked')),
    imported_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_records (
    batch_id   INTEGER NOT NULL REFERENCES batches(batch_id),
    station    TEXT NOT NULL,
    sample_day TEXT NOT NULL,
    analyte    TEXT NOT NULL,
    result     REAL NOT NULL,
    PRIMARY KEY (batch_id, station, sample_day, analyte)
);

CREATE TABLE IF NOT EXISTS current_results (
    station      TEXT NOT NULL,
    sample_day   TEXT NOT NULL,
    analyte      TEXT NOT NULL,
    result       REAL NOT NULL,
    source_batch INTEGER NOT NULL,
    updated_at   TEXT NOT NULL,
    PRIMARY KEY (station, sample_day, analyte)
);

CREATE TABLE IF NOT EXISTS revoke_jobs (
    job_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id       INTEGER NOT NULL REFERENCES batches(batch_id),
    status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'running', 'done', 'failed')),
    snapshot_batch INTEGER,
    keys_total     INTEGER NOT NULL DEFAULT 0,
    keys_done      INTEGER NOT NULL DEFAULT 0,
    shadow_path    TEXT,
    error          TEXT,
    created_at     TEXT NOT NULL,
    started_at     TEXT,
    finished_at    TEXT
);

CREATE INDEX IF NOT EXISTS idx_raw_batch ON raw_records(batch_id);
CREATE INDEX IF NOT EXISTS idx_raw_key ON raw_records(station, sample_day, analyte);

-- 数据库级保证：同一时刻至多一个未结束的撤销任务
CREATE UNIQUE INDEX IF NOT EXISTS uq_one_active_revoke
ON revoke_jobs(1) WHERE status IN ('pending', 'running');

-- 增量变更日志（只增不改：已发布的变更组永远不可改变，可重复读取）
CREATE TABLE IF NOT EXISTS change_groups (
    group_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL CHECK (kind IN ('import', 'revoke')),
    ref_id      INTEGER NOT NULL,          -- import=批次号，revoke=撤销任务号
    entry_count INTEGER NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS change_entries (
    group_id     INTEGER NOT NULL REFERENCES change_groups(group_id),
    change_type  TEXT NOT NULL CHECK (change_type IN ('upsert', 'delete')),
    station      TEXT NOT NULL,
    sample_day   TEXT NOT NULL,
    analyte      TEXT NOT NULL,
    result       REAL,                     -- delete 时为 NULL
    source_batch INTEGER,                  -- delete 时为 NULL
    seq          INTEGER NOT NULL,
    PRIMARY KEY (group_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_change_group ON change_entries(group_id);

-- 同步接入：后台固定同一时刻的完整正式结果（影子快照文件）+ 起始游标
CREATE TABLE IF NOT EXISTS sync_accesses (
    access_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    status             TEXT NOT NULL DEFAULT 'building'
                       CHECK (status IN ('building', 'ready', 'failed')),
    -- 快照时刻已存在的最大变更组；初始数据覆盖 1..snapshot_max_group 的全部效果
    snapshot_max_group INTEGER NOT NULL,
    snapshot_rows      INTEGER NOT NULL DEFAULT 0,
    page_size          INTEGER NOT NULL,
    shadow_path        TEXT,
    error              TEXT,
    created_at         TEXT NOT NULL,
    ready_at           TEXT
);
"""


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect(db_path: str) -> sqlite3.Connection:
    """打开一个 SQLite 连接。

    WAL 模式下读不阻塞写、写不阻塞读；busy_timeout 让切换的短写事务
    与并发导入自动排队而不是立刻报锁错误。
    """
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = connect(current_app.config["DB_PATH"])
    return g.db


def close_db(exc=None) -> None:
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()


def init_db(db_path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()
