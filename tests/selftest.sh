#!/usr/bin/env bash
# 端到端自测：在本机用便携 PostgreSQL + uvicorn 跑全链路验证。
# 用法：bash tests/selftest.sh
# 不依赖 Docker；Docker Compose 场景的启动方式见 README。
set -u
cd "$(dirname "$0")/.."
REPO=$PWD

# ------------------------------------------------------------- 环境定位
PGBIN=${PGBIN:-/tmp/pgdeb/extracted/usr/lib/postgresql/15/bin}
PGCLIENT=${PGCLIENT:-/tmp/pgdeb/client/usr/lib/postgresql/15/bin}
export PATH="$PGCLIENT:$PGBIN:$PATH"
PGDATA=${PGDATA:-/tmp/selftest-pgdata}
PGPORT=55433
PGSOCK=/tmp/selftest-pgsock
SQLDIR=/tmp/selftest-sqlite
APPPORT=8011
BASE="http://127.0.0.1:$APPPORT"
SHA64="a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
DSN="host=$PGSOCK port=$PGPORT dbname=postgres user=postgres"
PASS=0; FAIL=0

ok()   { echo "PASS: $1"; PASS=$((PASS+1)); }
bad()  { echo "FAIL: $1"; FAIL=$((FAIL+1)); }
check(){ if [ "$1" = "$2" ]; then ok "$3"; else bad "$3 (got '$1' want '$2')"; fi; }
api()  { curl -s -m 10 "http://127.0.0.1:$APPPORT$1"; }
wait_phase(){ # wait_phase <expected> <timeout_s>
  local want=$1 deadline=$((SECONDS+$2))
  while [ "$SECONDS" -lt "$deadline" ]; do
    [ "$(api /api/progress | jq -r .phase)" = "$want" ] && return 0
    sleep 0.5
  done
  return 1
}
start_app(){ # start_app <sqlite_dir> [init_sleep_ms]
  local dir=$1 sleep_ms=${2:-0}
  stop_app
  for _ in $(seq 1 20); do
    curl -s -m 1 "http://127.0.0.1:$APPPORT/api/progress" >/dev/null 2>&1 || break
    sleep 0.5
  done
  mkdir -p "$dir"
  PG_DSN="$DSN" SQLITE_PATH="$dir/history.db" CAPTURE_INIT_SLEEP_MS="$sleep_ms" \
    setsid nohup python3 -m uvicorn app.main:app --host 127.0.0.1 \
    --port "$APPPORT" > /tmp/selftest-app.log 2>&1 < /dev/null &
  local new_pid=$!
  echo "$new_pid" > /tmp/selftest-app.pid
  _wait_app_ready "$new_pid"
}
_wait_app_ready(){ # _wait_app_ready <expected_pid>
  local expected=$1
  for _ in $(seq 1 40); do
    if ! kill -0 "$expected" 2>/dev/null; then
      echo "new app exited early:"; cat /tmp/selftest-app.log; return 1
    fi
    if curl -s -m 1 "http://127.0.0.1:$APPPORT/api/progress" >/dev/null 2>&1; then
      # 确认响应者确实是新进程而不是占用端口的旧实例
      [ "$(app_pid)" = "$expected" ] && return 0
    fi
    sleep 0.5
  done
  echo "app failed to become ready:"; cat /tmp/selftest-app.log; return 1
}
stop_app(){
  local pid
  pid=$(app_pid)
  [ -n "$pid" ] && kill $pid 2>/dev/null || true
  for _ in $(seq 1 20); do
    app_pid | grep -q . || return 0
    sleep 0.5
  done
}
force_kill_app(){
  local pid
  pid=$(app_pid)
  [ -n "$pid" ] && kill -9 $pid 2>/dev/null || true
  sleep 1
}
app_pid(){ ps -eo pid,comm,args | awk '$2=="python3" && /uvicorn app.main:app/ {print $1}'; }
pg_ctl_q(){ pg_ctl -D "$PGDATA" -l /tmp/selftest-pg.log "$@"; }

cleanup(){
  stop_app || true
  pg_ctl_q stop -m fast >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ------------------------------------------------------------- 0. 准备源库
echo "=== 0. 准备全新源 PostgreSQL（wal_level=logical）==="
pg_ctl_q stop -m fast >/dev/null 2>&1 || true
rm -rf "$PGDATA" "$PGSOCK" "$SQLDIR"; mkdir -p "$PGSOCK"
initdb -D "$PGDATA" -U postgres --auth=trust -E UTF8 --no-sync >/dev/null
cat >> "$PGDATA/postgresql.conf" <<EOF
port = $PGPORT
unix_socket_directories = '$PGSOCK'
listen_addresses = ''
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
EOF
pg_ctl_q start -w >/dev/null
psql -h "$PGSOCK" -p "$PGPORT" -U postgres -q -f init/01_schema.sql
check "$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc 'select count(*) from artifacts')" \
      "3" "源表初始化 3 行示例数据"

# ------------------------------------------------------------- 1. 初始化未完成不开放历史（503）
echo "=== 1. 初始化窗口历史接口 503 ==="
start_app "$SQLDIR" 8000
sleep 1
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/points")
check "$code" "503" "INITIALIZING 期间 /api/points 返回 503"
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/directory?point=0/1")
check "$code" "503" "INITIALIZING 期间 /api/directory 返回 503"
check "$(api /api/progress | jq -r .phase)" "INITIALIZING" "进度接口暴露 INITIALIZING"
wait_phase STREAMING 15 && ok "初始化完成进入 STREAMING" || bad "初始化超时"
base_lsn=$(api /api/points | jq -r '.points[]|select(.kind=="BASELINE").lsn')
[ -n "$base_lsn" ] && ok "基线提交点存在: $base_lsn" || bad "缺少基线提交点"
check "$(api "/api/directory?point=$base_lsn" | jq '.artifacts|length')" "3" "基线目录 3 行"

# ------------------------------------------------------------- 2. 基线前开始、基线后提交的事务不漏
echo "=== 2. 跨基线事务（初始化窗口开启，基线后提交）==="
stop_app; sleep 1
rm -rf "$SQLDIR"
# 同步等待旧槽完全删除，避免新实例误走 resume 分支
for _ in $(seq 1 20); do
  psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
    "select pg_drop_replication_slot(slot_name) from pg_replication_slots" 2>/dev/null || true
  n=$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
    "select count(*) from pg_replication_slots")
  [ "$n" = "0" ] && break
  sleep 0.5
done
check "$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "select count(*) from pg_replication_slots")" "0" "旧复制槽已彻底删除，确保走全新基线"
# 清理历史失败运行可能残留的测试行（应用已停，不会再写入），
# 把源目录恢复到与 init/01_schema.sql 一致的 3 行初始状态
psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "delete from artifacts where artifact_no not in (1001,1002,1003)" >/dev/null
psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "insert into artifacts values
   (1001,'billing-gateway','1.4.2','a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90'),
   (1002,'risk-engine','2.7.0','0f1e2d3c4b5a69788796a5b4c3d2e1f009182736455463728190aabbccddeeff'),
   (1003,'notify-hub','0.9.3','ffeeddccbbaa9988776655443322110099887766554433221100998877665544')
   on conflict do nothing" >/dev/null
check "$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc "select count(*) from artifacts")" \
      "3" "源目录已重置为 3 行初始状态"
# 用应用内置测试钩子确定性构造"基线前开始、基线后提交"的事务：
# 应用在建槽/基线前打开源事务写入 2001 并持有，基线落盘后才提交。
start_app_hook(){
  stop_app
  for _ in $(seq 1 20); do
    curl -s -m 1 "http://127.0.0.1:$APPPORT/api/progress" >/dev/null 2>&1 || break
    sleep 0.5
  done
  mkdir -p "$SQLDIR"
  PG_DSN="$DSN" SQLITE_PATH="$SQLDIR/history.db" \
    TEST_INFLIGHT_ARTIFACT_NO=2001 TEST_INFLIGHT_SLEEP_S=3 \
    setsid nohup python3 -m uvicorn app.main:app --host 127.0.0.1 \
    --port "$APPPORT" > /tmp/selftest-app.log 2>&1 < /dev/null &
  local new_pid=$!
  echo "$new_pid" > /tmp/selftest-app.pid
  _wait_app_ready "$new_pid"
}
start_app_hook || bad "带钩子的应用启动失败"
wait_phase STREAMING 20 || bad "钩子场景初始化超时"
grep -q "TEST hook: inflight transaction opened" /tmp/selftest-app.log \
  && ok "测试钩子事务已在基线前打开" \
  || bad "测试钩子未生效: $(grep -c 'TEST hook' /tmp/selftest-app.log)"
# 基线快照里不允许出现 2001（事务尚未提交）
new_base=$(api /api/points | jq -r '.points[]|select(.kind=="BASELINE").lsn')
check "$(api "/api/directory?point=$new_base" | jq '[.artifacts[]|select(.artifact_no==2001)]|length')" \
      "0" "跨基线事务未混入基线快照（不重）"
# 等钩子事务在基线后提交并被复制流捕获
for _ in $(seq 1 30); do
  n=$(api /api/history/2001 | jq '[.history[]|select(.change_type=="INSERT")]|length')
  [ "$n" = "1" ] && break
  sleep 1
done
found=$(api /api/history/2001 | jq '[.history[]|select(.change_type=="INSERT")]|length')
check "$found" "1" "基线前开始、基线后提交的事务被复制流补捕获，无遗漏"
sleep 2
inflight_lsn=$(api /api/points | jq -r '.points[-1].lsn')
check "$(api "/api/directory?point=$inflight_lsn" | jq '[.artifacts[]|select(.artifact_no==2001)]|length')" \
      "1" "跨基线事务在其提交点目录中可见"

# ------------------------------------------------------------- 3. 多行事务整体可见 + 删除后重用编号
echo "=== 3. 多行事务原子性与编号重用 ==="
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[
  {\"op\":\"insert\",\"artifact_no\":3001,\"service_name\":\"g1\",\"version\":\"1\",\"sha256\":\"$SHA64\"},
  {\"op\":\"insert\",\"artifact_no\":3002,\"service_name\":\"g2\",\"version\":\"1\",\"sha256\":\"$SHA64\"},
  {\"op\":\"delete\",\"artifact_no\":1002}
 ]}" >/dev/null
sleep 2
points=$(api /api/points | jq '[.points[]|select(.kind=="TXN" and .change_count>=3)]|length')
[ "$points" -ge 1 ] && ok "三行操作只产生一个提交点（整体可见）" || bad "多行事务被拆分"
latest=$(api /api/points | jq -r '.points[-1].lsn')
dir3001=$(api "/api/directory?point=$latest" | jq '[.artifacts[]|select(.artifact_no==3001)]|length')
dir1002=$(api "/api/directory?point=$latest" | jq '[.artifacts[]|select(.artifact_no==1002)]|length')
check "$dir3001" "1" "同事务插入的 3001 在提交点可见"
check "$dir1002" "0" "同事务删除的 1002 在提交点消失"
# 重用 1002
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"insert\",\"artifact_no\":1002,\"service_name\":\"reborn\",\"version\":\"2\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
chain=$(api /api/history/1002 | jq -r '[.history[].change_type]|join(",")')
case "$chain" in
  *BASELINE*DELETE*INSERT*) ok "删除后重用编号，旧历史完整保留: $chain" ;;
  *) bad "编号重用历史链异常: $chain" ;;
esac

# ------------------------------------------------------------- 4. 失败整组回滚 + 输入校验
echo "=== 4. 回滚与固定结构校验 ==="
code=$(curl -s -o /tmp/r.json -w '%{http_code}' -X POST "$BASE/api/source/operations" \
  -H 'Content-Type: application/json' -d "{
 \"operations\":[
  {\"op\":\"insert\",\"artifact_no\":9001,\"service_name\":\"ghost\",\"version\":\"1\",\"sha256\":\"$SHA64\"},
  {\"op\":\"delete\",\"artifact_no\":999999}
 ]}")
check "$code" "400" "组内失败返回 400"
check "$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "select count(*) from artifacts where artifact_no=9001")" "0" "失败整组回滚，源库无残留"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/api/source/operations" \
  -H 'Content-Type: application/json' -d '{"operations":[{"op":"insert","artifact_no":1,"service_name":"x","version":"1","sha256":"bad"}]}')
check "$code" "422" "非法 sha256 被 pydantic 拒绝"
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/directory?point=not-a-lsn")
check "$code" "400" "非法 LSN 参数 400"

# ------------------------------------------------------------- 5. 强杀应用后续接：无重复、无丢失、槽位不超前落盘
echo "=== 5. 强杀重启一致性 ==="
before_txns=$(api /api/progress | jq .captured_transactions)
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"update\",\"artifact_no\":1001,\"service_name\":\"billing-gateway\",\"version\":\"7.7.7\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
after_txns=$(api /api/progress | jq .captured_transactions)
[ "$after_txns" -gt "$before_txns" ] && ok "新事务已捕获 ($before_txns -> $after_txns)" || bad "新事务未捕获"
kill -9 "$(app_pid)" 2>/dev/null || true; sleep 2
start_app "$SQLDIR"
wait_phase STREAMING 15 || true
sleep 3
check "$(api /api/progress | jq .captured_transactions)" "$after_txns" "强杀重启后事务数不重复"
check "$(api /api/history/1001 | jq '[.history[]|select(.version=="7.7.7")]|length')" "1" "强杀前最后一次更新仍在"
flush=$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "select confirmed_flush_lsn from pg_replication_slots where slot_name='artifacts_history_slot'")
last=$(api /api/progress | jq -r .last_commit_lsn)
if [ -n "$flush" ] && [ -n "$last" ]; then
  fnum=$(python3 -c "h,l='$flush'.split('/');print((int(h,16)<<32)+int(l,16))")
  lnum=$(python3 -c "h,l='$last'.split('/');print((int(h,16)<<32)+int(l,16))")
  [ "$fnum" -le "$lnum" ] && ok "槽 confirmed_flush_lsn($flush) 不超前已落盘提交点($last)" \
                            || bad "槽位超前落盘位置: $flush > $last"
fi
# 重启后再写一条，验证续接后的流仍然工作
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"update\",\"artifact_no\":1001,\"service_name\":\"billing-gateway\",\"version\":\"8.8.8\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
check "$(api /api/history/1001 | jq '[.history[]|select(.version=="8.8.8")]|length')" "1" "续接后新变更继续被捕获"

# ------------------------------------------------------------- 6. 源库断连恢复
echo "=== 6. 源库重启后自动重连续接 ==="
pg_ctl_q restart -w >/dev/null
sleep 1
for _ in $(seq 1 20); do
  api /api/progress | jq -e '.phase=="STREAMING"' >/dev/null && break
  sleep 1
done
check "$(api /api/progress | jq -r .phase)" "STREAMING" "源库重启后应用自动恢复 STREAMING"
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"update\",\"artifact_no\":1001,\"service_name\":\"billing-gateway\",\"version\":\"9.9.9\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
check "$(api /api/history/1001 | jq '[.history[]|select(.version=="9.9.9")]|length')" "1" "断连恢复后的变更未丢失"

# ------------------------------------------------------------- 7. 持久化校验
echo "=== 7. 两侧持久化 ==="
[ -f "$SQLDIR/history.db" ] && ok "SQLite 历史库落盘: $SQLDIR/history.db" || bad "SQLite 未落盘"
[ -d "$PGDATA/pg_wal" ] && ok "PostgreSQL 数据目录持久化: $PGDATA" || bad "PG 数据目录缺失"

# ------------------------------------------------------------- 8. 连续封存与网页核验
echo "=== 8. 链式 SHA-256 封存与核验 ==="
# 8.1 每个提交点都带封存序号，进度接口暴露封存链且轻量链接完好
seal_n=$(api /api/progress | jq -r '.seal.seal_count')
[ "$seal_n" -ge 2 ] && ok "封存记录随基线+每笔事务生成（$seal_n 条）" || bad "封存数量异常: $seal_n"
check "$(api /api/progress | jq -r '.seal.enabled')" "true" "封存已启用"
check "$(api /api/progress | jq -r '.seal.link_ok')" "true" "封存链链接完好"
nopts=$(api /api/points | jq '[.points[]|select(.seal_seq!=null)]|length')
nall=$(api /api/points | jq '.points|length')
check "$nopts" "$nall" "每个提交点都对应一条封存"
# 8.2 正常历史核验通过，回溯行可信
latest=$(api /api/points | jq -r '.points[-1].lsn')
check "$(api "/api/verify?point=$latest" | jq -r .status)" "VERIFIED" "未篡改时网页核验通过"
check "$(api "/api/verify?point=$latest" | jq '[.artifacts[]|select(.trust!="TRUSTED")]|length')" "0" \
      "核验通过时回溯结果全部可信"
check "$(api "/api/history/1001" | jq -r '.history[-1].trust')" "TRUSTED" "单编号历史带可信标记"
# 8.3 再写一笔事务并核验，确认链持续延伸
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"insert\",\"artifact_no\":4001,\"service_name\":\"seal-svc\",\"version\":\"1\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
latest=$(api /api/points | jq -r '.points[-1].lsn')
check "$(api "/api/verify?point=$latest" | jq -r .status)" "VERIFIED" "新事务封存后链仍完整"
new_n=$(api /api/progress | jq -r '.seal.seal_count')
[ "$new_n" -gt "$seal_n" ] && ok "封存链随事务连续延伸（$seal_n -> $new_n）" || bad "封存链未延伸"
seqs=$(python3 -c "
import sqlite3,sys
c=sqlite3.connect('$SQLDIR/history.db')
print(','.join(str(r[0]) for r in c.execute('select seq from seals order by seq')))")
case "$seqs" in 0,1,2*|0,1,2) ok "封存序号从 GENESIS=0 连续: $seqs" ;; *) bad "封存序号不连续: $seqs" ;; esac

# 8.4 强杀重启：封存不重复、链仍可核验（不漏封/不重封）
force_kill_app
start_app "$SQLDIR"
wait_phase STREAMING 15 || true
sleep 3
seals_after=$(api /api/progress | jq -r '.seal.seal_count')
check "$seals_after" "$new_n" "强杀重启后封存记录不重复（不重封）"
check "$(api "/api/verify?point=$latest" | jq -r .status)" "VERIFIED" "强杀重启后核验仍通过"

# 8.5 篡改检测：改值 / 缺行 / 删封存（乱序），均指出最早失效事务并标不可信
db="$SQLDIR/history.db"
# 8.5a 改值：把某条 TXN 封存覆盖的历史行版本改掉（先备份，可精确还原）
victim_seq=$(python3 -c "
import sqlite3
c=sqlite3.connect('$db')
print(c.execute(\"select min(seq) from seals where kind='TXN'\").fetchone()[0])")
python3 - "$db" "$victim_seq" <<'PY'
import sqlite3, sys
db, seq = sys.argv[1], int(sys.argv[2])
c = sqlite3.connect(db)
c.execute("CREATE TABLE _selftest_backup AS SELECT * FROM artifact_history WHERE seal_seq=?", (seq,))
c.execute("update artifact_history set version='TAMPERED' where seal_seq=?", (seq,))
c.commit()
PY
v=$(api "/api/verify?point=$latest")
check "$(echo "$v" | jq -r .status)" "INVALID" "改值后核验失败"
check "$(echo "$v" | jq -r .first_invalid_seq)" "$victim_seq" "指出最早失效事务 seq=$victim_seq"
check "$(echo "$v" | jq -r .first_invalid_reason)" "ENTRY_HASH_MISMATCH" "改值识别为摘要不一致"
untrusted=$(echo "$v" | jq '[.artifacts[]|select(.trust=="UNTRUSTED")]|length')
[ "$untrusted" -ge 1 ] && ok "受影响回溯结果标为不可信（$untrusted 行）" || bad "未标记不可信"
good=$(echo "$v" | jq '[.artifacts[]|select(.trust=="TRUSTED")]|length')
[ "$good" -ge 1 ] && ok "失效点之前的回溯仍可信（$good 行）" || bad "失效点前记录被误伤"
# 从备份精确还原，链必须恢复 VERIFIED
python3 - "$db" "$victim_seq" <<'PY'
import sqlite3, sys
db, seq = sys.argv[1], int(sys.argv[2])
c = sqlite3.connect(db)
c.execute("UPDATE artifact_history SET version=(SELECT version FROM _selftest_backup b"
          " WHERE b.id=artifact_history.id) WHERE seal_seq=?", (seq,))
c.execute("DROP TABLE _selftest_backup")
c.commit()
PY
check "$(api "/api/verify?point=$latest" | jq -r .status)" "VERIFIED" "精确还原改值后核验恢复通过"

# 8.5b 缺行：删除某封存下的一条历史行（备份后可插回，保持与下个场景独立）
python3 - "$db" "$victim_seq" <<'PY'
import sqlite3, sys
db, seq = sys.argv[1], int(sys.argv[2])
c = sqlite3.connect(db)
c.execute("CREATE TABLE _selftest_backup AS SELECT * FROM artifact_history WHERE id IN ("
          " SELECT id FROM artifact_history WHERE seal_seq=? ORDER BY id LIMIT 1)", (seq,))
c.execute("DELETE FROM artifact_history WHERE id IN ("
          " SELECT id FROM artifact_history WHERE seal_seq=? ORDER BY id LIMIT 1)", (seq,))
c.commit()
PY
v=$(api "/api/verify?point=$latest")
check "$(echo "$v" | jq -r .status)" "INVALID" "缺行后核验失败"
check "$(echo "$v" | jq -r .first_invalid_reason)" "ROW_COUNT_MISMATCH" "缺行识别为行数不一致"
check "$(echo "$v" | jq -r .first_invalid_seq)" "$victim_seq" "缺行定位到最早失效事务"
# 插回被删行（显式原 id），链恢复
python3 - "$db" <<'PY'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1])
cols = [r[1] for r in c.execute("PRAGMA table_info(_selftest_backup)")]
ph = ",".join("?" for _ in cols)
for row in c.execute(f"SELECT {','.join(cols)} FROM _selftest_backup"):
    c.execute(f"INSERT INTO artifact_history({','.join(cols)}) VALUES({ph})", row)
c.execute("DROP TABLE _selftest_backup")
c.commit()
PY
check "$(api "/api/verify?point=$latest" | jq -r .status)" "VERIFIED" "插回缺行后核验恢复通过"

# 8.5c 乱序/删封存：删除中间封存制造 seq 空洞与断链
mid_seq=$(python3 -c "
import sqlite3
c=sqlite3.connect('$db')
rows=[r[0] for r in c.execute(\"select seq from seals where kind='TXN' order by seq\")]
print(rows[1] if len(rows) > 1 else rows[0])")
python3 - "$db" "$mid_seq" <<'PY'
import sqlite3, sys
db, seq = sys.argv[1], int(sys.argv[2])
c = sqlite3.connect(db)
c.execute("delete from seals where seq=?", (seq,))
c.commit()
PY
v=$(api "/api/verify?point=$latest")
check "$(echo "$v" | jq -r .status)" "INVALID" "删除封存后核验失败"
echo "$v" | jq -e '.first_invalid_reason=="SEQ_GAP" or .first_invalid_reason=="CHAIN_BROKEN"' >/dev/null \
  && ok "删封存识别为序列空洞/断链: $(echo "$v" | jq -r .first_invalid_reason)" \
  || bad "删封存原因异常: $(echo "$v" | jq -r .first_invalid_reason)"

# 8.6 核验为只读，捕获仍在继续：核验后再写事务仍可被捕获
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"insert\",\"artifact_no\":4002,\"service_name\":\"live-svc\",\"version\":\"1\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
check "$(api /api/history/4002 | jq '[.history[]|select(.change_type=="INSERT")]|length')" "1" \
      "核验期间捕获继续，新事务未被阻断"

# 8.7 旧库首次启用封存：全新目录 + 旧版本库（无 seals 表）启动后旧记录真实性未知
echo "--- 8.7 旧库升级：启用前真实性未知 ---"
stop_app; sleep 1
LEGACY_DIR=/tmp/selftest-legacy-sqlite
rm -rf "$LEGACY_DIR"; mkdir -p "$LEGACY_DIR"
python3 - "$LEGACY_DIR/history.db" <<'PY'
import sqlite3, sys
sha = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
c = sqlite3.connect(sys.argv[1])
c.executescript(f"""
CREATE TABLE capture_state(key TEXT PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO capture_state VALUES('phase','INITIALIZING');
INSERT INTO capture_state VALUES('slot_name','artifacts_history_slot');
INSERT INTO capture_state VALUES('baseline_lsn','0/1000');
INSERT INTO capture_state VALUES('baseline_lsn_num','4096');
CREATE TABLE txns(xid INTEGER PRIMARY KEY, commit_lsn TEXT NOT NULL,
                  commit_lsn_num INTEGER NOT NULL, commit_ts TEXT);
CREATE TABLE artifact_history(
  id INTEGER PRIMARY KEY AUTOINCREMENT, artifact_no INTEGER NOT NULL,
  change_type TEXT NOT NULL, service_name TEXT, version TEXT, sha256 TEXT,
  xid INTEGER, commit_lsn TEXT, commit_lsn_num INTEGER, captured_at TEXT NOT NULL);
INSERT INTO artifact_history(artifact_no,change_type,service_name,version,sha256,
  xid,commit_lsn,commit_lsn_num,captured_at)
VALUES (7001,'BASELINE','legacy','1.0','{sha}',NULL,'0/1000',4096,'t0');
""")
c.commit()
PY
# 旧库已带基线，应用走 resume 分支：确保源库复制槽存在（不存在则补建）
psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "select pg_create_logical_replication_slot('artifacts_history_slot','test_decoding')" 2>/dev/null || true
# 旧库已有基线状态，应用走 resume 分支直接 STREAMING 并在构造时补 GENESIS
PG_DSN="$DSN" SQLITE_PATH="$LEGACY_DIR/history.db" \
  setsid nohup python3 -m uvicorn app.main:app --host 127.0.0.1 \
  --port "$APPPORT" > /tmp/selftest-app.log 2>&1 < /dev/null &
echo "$!" > /tmp/selftest-app.pid
_wait_app_ready "$!" || bad "旧库升级场景应用启动失败"
wait_phase STREAMING 15 || true
check "$(api /api/progress | jq -r '.seal.enabled')" "true" "旧库启动后自动启用封存"
check "$(api /api/progress | jq -r '.seal.legacy_upgrade')" "true" "标记为旧库首次启用封存"
v=$(api "/api/verify")
check "$(echo "$v" | jq -r .status)" "VERIFIED" "旧库封存链可复算通过（仅证明启用后未变）"
check "$(echo "$v" | jq -r '.seal_genesis.legacy_upgrade')" "true" "核验结果暴露封存起点为旧库升级"
check "$(api /api/history/7001 | jq -r '.history[0].trust')" "UNKNOWN" "启用前旧记录真实性标为未知"
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"insert\",\"artifact_no\":7002,\"service_name\":\"post-seal\",\"version\":\"1\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
check "$(api /api/history/7002 | jq -r '.history[0].trust')" "TRUSTED" "启用后新事务封存记录可信"

echo
echo "================ 自测结果: $PASS passed / $FAIL failed ================"
[ "$FAIL" -eq 0 ]
