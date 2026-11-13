"""Flask 应用工厂与 HTTP 路由。

页面/接口：
- GET  /                 单页 Web 入口（上传、撤销、进度、当前结果、下载、同步接入）
- POST /api/upload       上传 CSV 批次
- POST /api/revoke       请求撤销某批次
- GET  /api/jobs         撤销任务与进度
- GET  /api/batches      批次列表
- GET  /api/results      当前正式结果 JSON
- GET  /api/results.csv  下载当前结果 CSV

同步接入（增量取数）：
- POST /api/sync/accesses              创建接入（后台构建固定时刻快照）
- GET  /api/sync/accesses              接入列表与状态
- GET  /api/sync/accesses/<id>         单个接入状态（含起始游标）
- GET  /api/sync/accesses/<id>/initial 分页读取初始数据
- GET  /api/sync/accesses/<id>/changes 按游标拉取下一完整变更组
"""
from __future__ import annotations

import csv
import io
import os

from flask import Flask, Response, jsonify, render_template, request

from .db import close_db, get_db, init_db
from .importer import ImportError_, import_batch, parse_csv
from .rebuild import RevokeManager
from .sync import SyncError, SyncManager


def create_app(db_path: str | None = None) -> Flask:
    app = Flask(__name__)
    data_dir = os.environ.get("DATA_DIR", "/data")
    app.config["DATA_DIR"] = data_dir
    app.config["DB_PATH"] = db_path or os.path.join(data_dir, "warehouse.db")

    init_db(app.config["DB_PATH"])
    manager = RevokeManager(app)
    app.extensions["revoke_manager"] = manager
    manager.resume_on_startup()  # 重启接续未完成的撤销
    sync_manager = SyncManager(app)
    app.extensions["sync_manager"] = sync_manager
    sync_manager.resume_on_startup()  # 未完成接入标失败，ready 接入保留供数

    app.teardown_appcontext(close_db)

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.post("/api/upload")
    def upload():
        f = request.files.get("file")
        if f is None or not f.filename:
            return jsonify({"ok": False, "error": "请选择要上传的 CSV 文件"}), 400
        try:
            records = parse_csv(f.read())
            conn = get_db()
            batch_id = import_batch(conn, f.filename, records)
        except ImportError_ as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        except Exception as exc:  # 唯一性约束等意外错误，整批回滚
            return jsonify({"ok": False, "error": f"导入失败，已整批回滚：{exc!r}"}), 400
        return jsonify(
            {
                "ok": True,
                "batch_id": batch_id,
                "row_count": len(records),
                "message": f"批次 {batch_id} 导入成功，共 {len(records)} 条",
            }
        )

    @app.post("/api/revoke")
    def revoke():
        body = request.get_json(silent=True) or {}
        try:
            batch_id = int(body["batch_id"])
        except (KeyError, TypeError, ValueError):
            return jsonify({"ok": False, "error": "请求体需要整数 batch_id"}), 400
        try:
            job = manager.request_revoke(batch_id)
        except LookupError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 404
        except (ValueError, RuntimeError) as exc:
            return jsonify({"ok": False, "error": str(exc)}), 409
        return jsonify({"ok": True, "job": job, "message": f"批次 {batch_id} 已进入后台撤销"})

    @app.get("/api/batches")
    def batches():
        conn = get_db()
        rows = conn.execute(
            """
            SELECT b.batch_id, b.filename, b.row_count, b.status, b.imported_at,
                   (SELECT COUNT(*) FROM revoke_jobs j
                     WHERE j.batch_id = b.batch_id AND j.status = 'done') AS revoked_once
            FROM batches b ORDER BY b.batch_id
            """
        ).fetchall()
        return jsonify([dict(r) for r in rows])

    @app.get("/api/jobs")
    def jobs():
        return jsonify(manager.list_jobs())

    @app.get("/api/results")
    def results():
        conn = get_db()
        rows = conn.execute(
            "SELECT station, sample_day, analyte, result, source_batch"
            " FROM current_results ORDER BY station, sample_day, analyte"
        ).fetchall()
        return jsonify([dict(r) for r in rows])

    @app.get("/api/results.csv")
    def results_csv():
        conn = get_db()
        rows = conn.execute(
            "SELECT station, sample_day, analyte, result, source_batch"
            " FROM current_results ORDER BY station, sample_day, analyte"
        ).fetchall()
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["站点", "采样日", "检测项目", "结果", "来源批次"])
        for r in rows:
            writer.writerow([r["station"], r["sample_day"], r["analyte"], r["result"], r["source_batch"]])
        return Response(
            buf.getvalue(),
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": "attachment; filename=current_results.csv"},
        )

    # ---------------- 同步接入 ----------------

    @app.post("/api/sync/accesses")
    def create_sync_access():
        body = request.get_json(silent=True) or {}
        page_size = body.get("page_size")
        try:
            access = sync_manager.create_access(page_size)
        except SyncError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        return jsonify({"ok": True, "access": access,
                        "message": f"接入 {access['access_id']} 已开始后台构建快照"})

    @app.get("/api/sync/accesses")
    def list_sync_accesses():
        return jsonify(sync_manager.list_accesses())

    @app.get("/api/sync/accesses/<int:access_id>")
    def get_sync_access(access_id: int):
        access = sync_manager.get_access(access_id)
        if access is None:
            return jsonify({"ok": False, "error": f"接入 {access_id} 不存在"}), 404
        return jsonify(access)

    @app.get("/api/sync/accesses/<int:access_id>/initial")
    def sync_initial(access_id: int):
        cursor = request.args.get("cursor") or None
        limit_raw = request.args.get("limit")
        limit = None
        if limit_raw is not None:
            try:
                limit = int(limit_raw)
            except ValueError:
                return jsonify({"ok": False, "error": "limit 须为整数"}), 400
        try:
            return jsonify(sync_manager.read_initial_page(access_id, cursor, limit))
        except LookupError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 404
        except SyncError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 409

    @app.get("/api/sync/accesses/<int:access_id>/changes")
    def sync_changes(access_id: int):
        cursor = request.args.get("cursor") or None
        try:
            return jsonify(sync_manager.read_next_changes(access_id, cursor))
        except LookupError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 404
        except SyncError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 409

    return app
