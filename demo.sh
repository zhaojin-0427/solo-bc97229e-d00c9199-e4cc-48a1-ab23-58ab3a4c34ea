#!/usr/bin/env bash
# 完整演示脚本：
# 导入三批 -> 创建同步接入并分页读初始数据 -> 撤销坏批（期间并发导入）
# -> 按游标拉取完整变更组 -> 查看/下载结果
set -euo pipefail
BASE="${BASE:-http://127.0.0.1:8000}"
DIR="$(cd "$(dirname "$0")" && pwd)/demo_data"

echo "① 导入批次 1（正常）/ 2（仪器整批故障）/ 3（后续修正）"
for f in batch1_ok.csv batch2_bad_instrument.csv batch3_corrected.csv; do
  curl -s -F "file=@$DIR/$f" "$BASE/api/upload"; echo
done

echo; echo "② 当前结果（同键取最新批次，坏批值暂时生效）"
curl -s "$BASE/api/results" | python3 -m json.tool

echo; echo "③ 创建同步接入（后台固定时刻快照，page_size=2 便于观察分页）"
AID="$(curl -s -X POST -H 'Content-Type: application/json' -d '{"page_size":2}' \
  "$BASE/api/sync/accesses" | python3 -c 'import json,sys;print(json.load(sys.stdin)["access"]["access_id"])')"
echo "   access_id=$AID，等待快照就绪..."
for _ in $(seq 1 60); do
  status="$(curl -s "$BASE/api/sync/accesses/$AID" | python3 -c 'import json,sys;print(json.load(sys.stdin)["status"])')"
  echo "   status=$status"
  [ "$status" != "building" ] && break
  sleep 0.5
done

echo; echo "④ 分页读取初始数据（每页 2 行，翻完自动切换到变更游标）"
python3 - "$BASE" "$AID" <<'PY'
import json, sys, urllib.request
base, aid = sys.argv[1], sys.argv[2]
cursor = None
total = 0
while True:
    url = f"{base}/api/sync/accesses/{aid}/initial"
    if cursor:
        url += "?cursor=" + cursor
    page = json.load(urllib.request.urlopen(url))
    total += len(page["records"])
    print(f"   一页 {len(page['records'])} 条，累计 {total} 条")
    cursor = page["next_cursor"]
    if page["next_phase"] == "changes":
        print("   初始数据读完，变更阶段游标：", cursor[:48], "...")
        break
PY

echo; echo "⑤ 请求撤销批次 2（后台影子重建，约数秒，可在页面观察进度）"
curl -s -X POST -H 'Content-Type: application/json' -d '{"batch_id":2}' "$BASE/api/revoke"; echo

echo "⑥ 重建期间继续导入批次 4（立即生效，切换时自动合并，不被迟到计算覆盖）"
tmp="$(mktemp -d)/batch4.csv"
printf '站点,采样日,检测项目,结果\n城东站点,2026-11-01,PM2.5,34.5\n新南站,2026-11-03,CO,1.2\n' > "$tmp"
curl -s -F "file=@$tmp" "$BASE/api/upload"; echo

echo; echo "⑦ 等待撤销任务完成"
for _ in $(seq 1 60); do
  line="$(curl -s "$BASE/api/jobs" | python3 -c "import json,sys;j=json.load(sys.stdin)[0];print(j['status'],j['keys_done'],'/',j['keys_total'])")"
  echo "   $line"
  case "$line" in done*|failed*) break;; esac
  sleep 0.5
done

echo; echo "⑧ 按游标逐组拉取变更（每次恰好一个完整组，不拆组；可重复读）"
python3 - "$BASE" "$AID" <<'PY'
import json, sys, urllib.request
base, aid = sys.argv[1], sys.argv[2]
# 从起始游标翻完初始页，拿到变更游标
cursor = None
while True:
    url = f"{base}/api/sync/accesses/{aid}/initial"
    if cursor:
        url += "?cursor=" + cursor
    page = json.load(urllib.request.urlopen(url))
    cursor = page["next_cursor"]
    if page["next_phase"] == "changes":
        break
while True:
    ch = json.load(urllib.request.urlopen(
        f"{base}/api/sync/accesses/{aid}/changes?cursor=" + cursor))
    if ch["group"] is None:
        print("   无更多变更组（游标停在最新组，可安全轮询/重复读取）")
        break
    g = ch["group"]
    desc = [(e["change_type"], e["station"], e["analyte"],
             e.get("result"), e.get("source_batch")) for e in ch["entries"]]
    print(f"   组 #{g['group_id']} {g['kind']}（ref={g['ref_id']}，{g['entry_count']} 条）: {desc}")
    cursor = ch["next_cursor"]
PY

echo; echo "⑨ 撤销后的正式结果：坏批独有键已删除，其余键取最大未撤销批次"
curl -s "$BASE/api/results" | python3 -m json.tool
echo "⑩ 下载当前结果 CSV： $BASE/api/results.csv"
