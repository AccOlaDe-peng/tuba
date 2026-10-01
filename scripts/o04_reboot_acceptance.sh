#!/usr/bin/env bash
# O04 目标主机（248）重启恢复验收的对照脚本。
#
# 在运维端（本机 Git Bash）运行，经 SSH 对 248 执行，全程只读：
# 不重启、不停服务、不改配置。分三个阶段：
#
#   precheck   重启前检查：SSH 连通、crond/@reboot 入口、两份清单当前状态、磁盘水位。
#   baseline   重启前基线采集并落盘：boot_id、boot.log 水位、两份 Launcher 清单 status、
#              消费组 lag/位点、ES 各 alias 文档计数、DLQ topic 末端 offset、磁盘。
#   verify     重启后验收：逐项 PASS/FAIL——主机确实重启、tuba-boot 已执行、
#              两份清单全部 running、restarts、lag 追平耗时、ES 计数恢复增长且
#              不低于基线、DLQ 零新增、消费组集合不变。
#
# 幂等：baseline 拒绝覆盖已存在的非空目录（除非 --force）；verify 可反复重跑。
# 失败项附处置提示；详细步骤见 docs/RUNBOOK.md「O04 主机重启验收步骤」。
set -euo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
repo_root=$(cd -- "${script_dir}/.." && pwd -P)

# 248 侧路径（可用环境变量覆盖，便于将来环境变化时不改脚本）。
LAUNCHER_BIN=${O04_LAUNCHER_BIN:-/opt/tuba/bin/tuba-launcher}
MANIFEST_MAIN=${O04_MANIFEST_MAIN:-/etc/tuba/tuba-services.json}
MANIFEST_MON=${O04_MANIFEST_MON:-/etc/tuba/tuba-monitoring.json}
BOOT_LOG=${O04_BOOT_LOG:-/var/log/tuba/boot.log}
TUBA_ENV=${O04_TUBA_ENV:-/etc/tuba/tuba.env}
KAFKA_BIN=${O04_KAFKA_BIN:-/opt/adms/kafka/bin}
KAFKA_JAVA_HOME=${O04_KAFKA_JAVA_HOME:-/opt/adms/adms-jdk}
KAFKA_BOOTSTRAP=${O04_KAFKA_BOOTSTRAP:-10.6.68.248:29292}
KAFKA_ADMIN_PROPS=${O04_KAFKA_ADMIN_PROPS:-/opt/tuba/collector-live/kafka/admin.properties}
DLQ_TOPICS=${O04_DLQ_TOPICS:-"tuba.collector.tenant_a.dlq.v1 tuba.collector.zeek_validation_20260927_001.dlq.v1 tuba.collector.zeek_validation_20260927_001.source-adapter.dlq.v1"}

usage() {
  cat >&2 <<'USAGE'
Usage: o04_reboot_acceptance.sh <phase> [options]

Phases:
  precheck                      重启前检查（只读，逐项 PASS/FAIL，不落盘）
  baseline --out DIR            采集重启前基线到 DIR（拒绝覆盖非空目录，除非 --force）
  verify   --baseline DIR       重启后逐项验收并对账基线（只读，可重跑）
    [--lag-timeout SECONDS]       lag 追平等待上限（默认 1800）
    [--poll SECONDS]              lag 轮询间隔（默认 15）

环境：从仓库根 .env.local 读取 TUBA_SSH_USER/TUBA_SSH_HOST/TUBA_SSH_PASSWORD，
经 .codex-ssh-askpass.cmd 非交互登录 248。O04_* 环境变量可覆盖远端路径。

Exit codes: 0 全部通过；1 存在 FAIL；2 用法/环境错误。
USAGE
}

fail_count=0
warn_count=0
pass_count=0

report() { # level, item, message
  local level=$1 item=$2 message=$3
  case $level in
    PASS) pass_count=$((pass_count + 1)) ;;
    WARN) warn_count=$((warn_count + 1)) ;;
    FAIL) fail_count=$((fail_count + 1)) ;;
  esac
  printf '%-4s %-28s %s\n' "$level" "$item" "$message"
}

summary() {
  printf '%s\n' "----"
  printf 'PASS=%d WARN=%d FAIL=%d\n' "$pass_count" "$warn_count" "$fail_count"
  [[ $fail_count -eq 0 ]]
}

setup_ssh() {
  local env_file="${repo_root}/.env.local"
  if [[ ! -f $env_file ]]; then
    echo "missing ${env_file} (need TUBA_SSH_USER/TUBA_SSH_HOST/TUBA_SSH_PASSWORD)" >&2
    exit 2
  fi
  set -a
  # shellcheck source=/dev/null
  source "$env_file"
  set +a
  : "${TUBA_SSH_USER:?TUBA_SSH_USER not set in .env.local}"
  : "${TUBA_SSH_HOST:?TUBA_SSH_HOST not set in .env.local}"
  : "${TUBA_SSH_PASSWORD:?TUBA_SSH_PASSWORD not set in .env.local}"
  local win_root
  win_root=$(cd "$repo_root" && pwd -W 2>/dev/null || pwd)
  export SSH_ASKPASS="${win_root}/.codex-ssh-askpass.cmd"
  export SSH_ASKPASS_REQUIRE=force DISPLAY=:0
}

ssh248() {
  ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new \
    "$TUBA_SSH_USER@$TUBA_SSH_HOST" "$@"
}

# 在 248 上执行一段只读脚本；失败时返回非零且输出为空。
remote() {
  ssh248 bash -s 2>/dev/null
}

kafka_consumer_groups() {
  remote <<EOF
JAVA_HOME=${KAFKA_JAVA_HOME} ${KAFKA_BIN}/kafka-consumer-groups.sh \
  --bootstrap-server ${KAFKA_BOOTSTRAP} \
  --command-config ${KAFKA_ADMIN_PROPS} \
  --describe --all-groups
EOF
}

# 只保留形如 "GROUP TOPIC PARTITION CURRENT LOG-END LAG ..." 的数据行，
# 过滤 kafka 工具打到 stdout 的告警/表头。
kafka_group_rows() {
  awk '$1 ~ /^tuba/ && $3 ~ /^[0-9]+$/ && $6 ~ /^[0-9-]+$/'
}

kafka_dlq_offsets() {
  remote <<EOF
for t in ${DLQ_TOPICS}; do
  JAVA_HOME=${KAFKA_JAVA_HOME} ${KAFKA_BIN}/kafka-get-offsets.sh \
    --bootstrap-server ${KAFKA_BOOTSTRAP} \
    --command-config ${KAFKA_ADMIN_PROPS} \
    --topic "\$t" --time -1
done
EOF
}

es_counts() {
  # /etc/tuba/tuba.env 只含密钥；ES_URL 不在其中，248 实际值是 loopback 9200。
  remote <<EOF
set -a; . ${TUBA_ENV}; set +a
: "\${ES_URL:=http://127.0.0.1:9200}"
for a in \$(curl -sS -m 10 -H "Authorization: ApiKey \${ES_API_KEY}" \
    "\${ES_URL}/_cat/aliases/logs-ueba.*?h=alias" | sort -u); do
  c=\$(curl -sS -m 10 -H "Authorization: ApiKey \${ES_API_KEY}" \
    "\${ES_URL}/\${a}/_count" | sed -n 's/.*"count":\([0-9][0-9]*\).*/\1/p')
  printf '%s %s\n' "\$a" "\${c:-ERR}"
done
EOF
}

launcher_status() { # manifest
  ssh248 "${LAUNCHER_BIN} status --manifest $1" 2>/dev/null
}

collect_meta() {
  remote <<'EOF'
echo "captured_at=$(date -Is)"
echo "boot_id=$(cat /proc/sys/kernel/random/boot_id)"
echo "uptime_since=$(uptime -s 2>/dev/null || true)"
echo "hostname=$(hostname)"
EOF
}

collect_boot_log() {
  remote <<EOF
echo "lines=\$(wc -l < ${BOOT_LOG} 2>/dev/null || echo MISSING)"
tail -n 30 ${BOOT_LOG} 2>/dev/null || true
EOF
}

collect_disk() {
  remote <<'EOF'
df -P / /var/lib /opt 2>/dev/null
EOF
}

collect_crontab() {
  remote <<'EOF'
systemctl is-active crond 2>/dev/null || systemctl is-active cron 2>/dev/null || echo unknown
crontab -l 2>&1
EOF
}

# --- 解析辅助（本地，读采集到的文本） ----------------------------------------

# 输入: launcher status 文本; 输出: "running=X total=Y bad=badstate列表 restartsum=N"
parse_launcher_status() {
  awk '
    /^TUBA launcher/ { next }
    NF >= 4 {
      state=$2; total++
      if (state == "running") running++
      else bad = bad (bad == "" ? "" : ",") $1 ":" state
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^restarts=/) { sub(/^restarts=/, "", $i); restarts += $i }
      }
    }
    END { printf "running=%d total=%d bad=%s restartsum=%d\n", running+0, total+0, bad, restarts+0 }
  '
}

# 输入: consumer-groups 数据行; 输出活跃成员组的 lag 合计（无成员的孤儿组单列）。
lag_summary() { # rows-file
  awk '{ if ($(NF-2) == "-") orphan[$1] += $6; else active += $6 }
       END { printf "active_lag=%d orphan_groups=%d\n", active+0, length(orphan) }' "$1"
}

group_names() { # rows-file -> 排序后的组名清单
  awk '{ print $1 }' "$1" | sort -u
}

# --- 阶段实现 ----------------------------------------------------------------

phase_precheck() {
  report PASS ssh "连接 ${TUBA_SSH_USER}@${TUBA_SSH_HOST} 成功"

  local cron_out
  cron_out=$(collect_crontab) || true
  if printf '%s\n' "$cron_out" | head -n 1 | grep -qx 'active'; then
    report PASS crond "crond active"
  else
    report FAIL crond "crond 非 active：@reboot 不会触发；处置：systemctl enable --now crond 后再重启"
  fi
  if printf '%s\n' "$cron_out" | grep -q '^@reboot /opt/tuba/bin/tuba-boot'; then
    report PASS boot-entry "root crontab 含 @reboot /opt/tuba/bin/tuba-boot"
  else
    report FAIL boot-entry "root crontab 缺 @reboot tuba-boot；处置：按 RUNBOOK「开机恢复」一节补装后再重启"
  fi

  local label manifest status parsed running total bad
  for label in main mon; do
    if [[ $label == main ]]; then manifest=$MANIFEST_MAIN; else manifest=$MANIFEST_MON; fi
    if status=$(launcher_status "$manifest"); then
      parsed=$(printf '%s\n' "$status" | parse_launcher_status)
      running=$(printf '%s' "$parsed" | sed -n 's/.*running=\([0-9]*\).*/\1/p')
      total=$(printf '%s' "$parsed" | sed -n 's/.*total=\([0-9]*\).*/\1/p')
      bad=$(printf '%s' "$parsed" | sed -n 's/.*bad=\([^ ]*\).*/\1/p')
      if [[ $running == "$total" && $total -gt 0 ]]; then
        report PASS "manifest-${label}" "${manifest} 全部 running（${running}/${total}）"
      else
        report FAIL "manifest-${label}" "${manifest} 仅 ${running}/${total} running（${bad}）；处置：先看 tuba-launcher logs，恢复正常后再采基线"
      fi
    else
      report FAIL "manifest-${label}" "tuba-launcher status ${manifest} 执行失败；处置：核对 Launcher 与清单是否在位"
    fi
  done

  local disk root_pct
  disk=$(collect_disk) || true
  # df 对 /、/var/lib、/opt 可能同属根盘，取第一行挂载点为 / 的结果。
  root_pct=$(printf '%s\n' "$disk" | awk '$NF == "/" { gsub(/%/, "", $(NF-1)); print $(NF-1); exit }')
  if [[ -n $root_pct && $root_pct -lt 75 ]]; then
    report PASS disk "根盘 ${root_pct}%（低于 75% critical）"
  elif [[ -n $root_pct ]]; then
    report FAIL disk "根盘 ${root_pct}% ≥ 75%：ES 将停止分配新分片，重启后索引恢复可能失败；处置：先按「磁盘水位与容量」清理再重启"
  else
    report WARN disk "无法读取根盘水位"
  fi
}

phase_baseline() { # out_dir
  local out=$1
  mkdir -p "$out"
  if [[ -n $(ls -A "$out" 2>/dev/null) && $force -eq 0 ]]; then
    echo "refusing to overwrite non-empty baseline dir: $out (use --force)" >&2
    exit 2
  fi

  collect_meta >"${out}/meta.env"
  collect_boot_log >"${out}/boot_log.txt"
  collect_crontab >"${out}/crontab.txt"
  collect_disk >"${out}/disk.txt"
  launcher_status "$MANIFEST_MAIN" >"${out}/launcher_main.txt"
  launcher_status "$MANIFEST_MON" >"${out}/launcher_monitoring.txt"
  kafka_consumer_groups | kafka_group_rows >"${out}/consumer_groups.txt"
  kafka_dlq_offsets | grep ':' >"${out}/dlq_offsets.txt" || true
  es_counts >"${out}/es_counts.txt"

  echo "baseline written to $out"
  # 落盘后立即给出基线摘要，供执行人确认"从健康状态重启"。
  local parsed lag
  parsed=$(parse_launcher_status <"${out}/launcher_main.txt")
  report PASS baseline-main "数据面清单：${parsed}"
  parsed=$(parse_launcher_status <"${out}/launcher_monitoring.txt")
  report PASS baseline-mon "监控清单：${parsed}"
  lag=$(lag_summary "${out}/consumer_groups.txt")
  report PASS baseline-lag "消费组：$(group_names "${out}/consumer_groups.txt" | wc -l) 个，${lag}"
  report PASS baseline-es "ES alias 计数 $(wc -l <"${out}/es_counts.txt") 项已记录"
}

phase_verify() { # baseline_dir
  local base=$1
  local f
  for f in meta.env boot_log.txt launcher_main.txt launcher_monitoring.txt consumer_groups.txt es_counts.txt dlq_offsets.txt; do
    if [[ ! -s "${base}/${f}" ]]; then
      echo "baseline incomplete: missing ${base}/${f}" >&2
      exit 2
    fi
  done

  local base_boot_id now_meta now_boot_id
  base_boot_id=$(sed -n 's/^boot_id=//p' "${base}/meta.env")
  now_meta=$(collect_meta) || {
    report FAIL ssh "重启后 SSH 仍不可达；处置：需要带外/现场介入（248 无远程带外，见 RUNBOOK）"
    summary
    return 1
  }
  printf '%s\n' "$now_meta" >"${base}/verify_meta.env"
  now_boot_id=$(printf '%s\n' "$now_meta" | sed -n 's/^boot_id=//p')

  if [[ -n $base_boot_id && $now_boot_id != "$base_boot_id" ]]; then
    report PASS rebooted "boot_id 已变化（主机确实重启）"
  else
    report FAIL rebooted "boot_id 与基线相同——主机尚未重启，verify 应在重启后运行"
  fi

  local base_lines new_lines
  base_lines=$(sed -n 's/^lines=//p' "${base}/boot_log.txt" | head -n 1)
  new_lines=$(ssh248 "wc -l < ${BOOT_LOG}" 2>/dev/null || echo 0)
  ssh248 "tail -n +$(( ${base_lines:-0} + 1 )) ${BOOT_LOG}" >"${base}/verify_boot_log_new.txt" 2>/dev/null || true
  if [[ ${base_lines:-0} =~ ^[0-9]+$ && $new_lines -gt $base_lines ]]; then
    report PASS tuba-boot "boot.log 新增 $((new_lines - base_lines)) 行，tuba-boot 已执行"
  else
    report FAIL tuba-boot "boot.log 无新行：@reboot 未触发或执行失败；处置：查 crond 状态后手动执行 /opt/tuba/bin/tuba-boot（幂等），再重跑 verify"
  fi

  local label manifest status parsed running total bad restarts
  for label in main mon; do
    if [[ $label == main ]]; then manifest=$MANIFEST_MAIN; else manifest=$MANIFEST_MON; fi
    if status=$(launcher_status "$manifest"); then
      printf '%s\n' "$status" >"${base}/verify_launcher_${label}.txt"
      parsed=$(printf '%s\n' "$status" | parse_launcher_status)
      running=$(printf '%s' "$parsed" | sed -n 's/.*running=\([0-9]*\).*/\1/p')
      total=$(printf '%s' "$parsed" | sed -n 's/.*total=\([0-9]*\).*/\1/p')
      bad=$(printf '%s' "$parsed" | sed -n 's/.*bad=\([^ ]*\).*/\1/p')
      restarts=$(printf '%s' "$parsed" | sed -n 's/.*restartsum=\([0-9]*\).*/\1/p')
      if [[ $running == "$total" && $total -gt 0 ]]; then
        report PASS "manifest-${label}" "${manifest} 全部 running（${running}/${total}）"
      else
        report FAIL "manifest-${label}" "${manifest} 仅 ${running}/${total} running（${bad}）；处置：tuba-launcher logs --manifest ${manifest} --service <名> 查退出原因；依赖未就绪时 Launcher 会按 1s→30s 退避自动拉起，先确认 PG/Kafka/ES"
      fi
      if [[ $restarts -eq 0 ]]; then
        report PASS "restarts-${label}" "restarts 合计 0"
      else
        report WARN "restarts-${label}" "restarts 合计 ${restarts}：启动顺序竞争可有瞬时重启，查对应服务日志确认非持续崩溃"
      fi
    else
      report FAIL "manifest-${label}" "tuba-launcher status ${manifest} 执行失败；处置：见 RUNBOOK 手动介入节"
    fi
  done

  # 消费组集合对账（偏移连续性的证据：不增不减）。
  kafka_consumer_groups | kafka_group_rows >"${base}/verify_consumer_groups.txt"
  local group_diff
  group_diff=$(diff <(group_names "${base}/consumer_groups.txt") <(group_names "${base}/verify_consumer_groups.txt") || true)
  if [[ -z $group_diff ]]; then
    report PASS group-set "消费组集合与基线一致（$(group_names "${base}/verify_consumer_groups.txt" | wc -l) 个，无消失无新增）"
  else
    report FAIL group-set "消费组集合变化：${group_diff}；处置：消失的组说明有组件没起来，新增的组说明有误配置消费者"
  fi

  # lag 追平（只统计有活跃成员的组；无成员的孤儿组 lag 冻结、永远不动，单列提示）。
  local waited=0 lag active_lag orphan_groups
  while :; do
    kafka_consumer_groups | kafka_group_rows >"${base}/verify_consumer_groups.txt"
    lag=$(lag_summary "${base}/verify_consumer_groups.txt")
    active_lag=$(printf '%s' "$lag" | sed -n 's/.*active_lag=\([0-9]*\).*/\1/p')
    orphan_groups=$(printf '%s' "$lag" | sed -n 's/.*orphan_groups=\([0-9]*\).*/\1/p')
    if [[ ${active_lag:-1} -eq 0 ]]; then break; fi
    if [[ $waited -ge $lag_timeout ]]; then
      report FAIL lag "等待 ${lag_timeout}s 后活跃组 lag 仍 ${active_lag}；处置：按 RUNBOOK「TubaKafkaLag」逐组确认成员与组件状态"
      break
    fi
    sleep "$poll"
    waited=$((waited + poll))
  done
  if [[ ${active_lag:-1} -eq 0 ]]; then
    report PASS lag "活跃消费组 lag 已追平（verify 内等待 ${waited}s；孤儿无成员组 ${orphan_groups} 个，不计入）"
  fi

  # DLQ 零新增（对账"重启不制造新的永久失败"）。
  local dlq_diff
  kafka_dlq_offsets | grep ':' >"${base}/verify_dlq_offsets.txt" || true
  dlq_diff=$(diff "${base}/dlq_offsets.txt" "${base}/verify_dlq_offsets.txt" || true)
  if [[ -z $dlq_diff ]]; then
    report PASS dlq "DLQ topic 末端 offset 与基线一致（零新增）"
  else
    report WARN dlq "DLQ 末端 offset 变化：${dlq_diff}；处置：按「DLQ 重放」一节聚合 failure.code 确认是否为重启引入"
  fi

  # ES 对账：各 alias 计数不低于基线，且 raw alias 恢复增长。
  es_counts >"${base}/verify_es_counts.txt"
  local shrink="" shrink_count=0
  while read -r a c; do
    [[ $c =~ ^[0-9]+$ ]] || continue
    local old
    old=$(awk -v k="$a" '$1 == k { print $2 }' "${base}/es_counts.txt")
    if [[ $old =~ ^[0-9]+$ && $c -lt $old ]]; then
      shrink="${shrink} ${a}(${old}->${c})"
      shrink_count=$((shrink_count + 1))
    fi
  done <"${base}/verify_es_counts.txt"
  if [[ $shrink_count -eq 0 ]]; then
    report PASS es-reconcile "全部 ES alias 计数 ≥ 基线，无丢失"
  else
    report FAIL es-reconcile "以下 alias 计数低于基线：${shrink}；处置：先排除 capacity guard 的到期分区删除（核对其 /metrics deleted_indices_total），再按 ES 恢复流程排查"
  fi

  local grew=0 a c1 c2
  sleep 30
  es_counts >"${base}/verify_es_counts_2.txt"
  while read -r a c1; do
    case $a in
      logs-ueba.raw-*)
        c2=$(awk -v k="$a" '$1 == k { print $2 }' "${base}/verify_es_counts_2.txt")
        if [[ $c1 =~ ^[0-9]+$ && $c2 =~ ^[0-9]+$ && $c2 -gt $c1 ]]; then grew=$((grew + 1)); fi
        ;;
    esac
  done <"${base}/verify_es_counts.txt"
  if [[ $grew -gt 0 ]]; then
    report PASS es-growth "raw alias 计数恢复增长（30s 窗口内 ${grew} 个增长）"
  else
    report WARN es-growth "30s 窗口内 raw alias 计数未增长：可能正值来源静默期；延长观察或核对 adapter lag"
  fi

  local disk root_pct
  disk=$(collect_disk) || true
  printf '%s\n' "$disk" >"${base}/verify_disk.txt"
  root_pct=$(printf '%s\n' "$disk" | awk '$NF == "/" { gsub(/%/, "", $(NF-1)); print $(NF-1); exit }')
  if [[ -n $root_pct && $root_pct -lt 75 ]]; then
    report PASS disk "根盘 ${root_pct}%"
  else
    report WARN disk "根盘 ${root_pct:-未知}%：≥75% 会阻止新分片分配，按「磁盘水位与容量」处理"
  fi
}

main() {
  if [[ $# -lt 1 ]]; then
    usage
    exit 2
  fi
  local phase=$1
  shift
  local out="" baseline_dir="" lag_timeout=1800 poll=15
  force=0
  while [[ $# -gt 0 ]]; do
    case $1 in
      --out) out=$2; shift 2 ;;
      --baseline) baseline_dir=$2; shift 2 ;;
      --lag-timeout) lag_timeout=$2; shift 2 ;;
      --poll) poll=$2; shift 2 ;;
      --force) force=1; shift ;;
      -h | --help) usage; exit 0 ;;
      *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
    esac
  done

  setup_ssh
  # 先证连通，再进阶段逻辑。
  if ! ssh248 true; then
    echo "cannot reach ${TUBA_SSH_USER}@${TUBA_SSH_HOST} over SSH" >&2
    exit 2
  fi

  case $phase in
    precheck)
      phase_precheck
      summary
      ;;
    baseline)
      if [[ -z $out ]]; then
        out="${repo_root}/.runtime/o04-reboot/$(date -u +%Y%m%dT%H%M%SZ)"
      fi
      phase_baseline "$out"
      ;;
    verify)
      if [[ -z $baseline_dir ]]; then
        echo "verify requires --baseline DIR" >&2
        usage
        exit 2
      fi
      phase_verify "$baseline_dir"
      summary
      ;;
    *)
      echo "Unknown phase: $phase" >&2
      usage
      exit 2
      ;;
  esac
}

main "$@"
