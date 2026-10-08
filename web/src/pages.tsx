import { EntityName, ReadableValue, readable, readableDescription } from "./readable";
import { useMemo, useState } from "react";
import {
  Alert,
  App as AntApp,
  Button,
  Descriptions,
  Drawer,
  Form,
  Input,
  Modal,
  Popconfirm,
  Progress,
  Select,
  Space,
  Table,
  Tabs,
  Tag,
  Tooltip,
} from "antd";
import type { TableProps } from "antd";
import {
  useInfiniteQuery,
  useMutation,
  useQueries,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import {
  Activity,
  AlertTriangle,
  Archive,
  ArrowLeft,
  ArrowRight,
  ArrowUpRight,
  Check,
  ChevronRight,
  CircleAlert,
  CircleDot,
  Database,
  FileSearch,
  Filter,
  FolderKanban,
  Gauge,
  GitBranch,
  KeyRound,
  Lock,
  Plus,
  RefreshCw,
  Search,
  Server,
  ShieldCheck,
  UserPlus,
  Users,
} from "lucide-react";
import { Link, useNavigate, useParams, useSearchParams } from "react-router-dom";

import {
  APIError,
  api,
  analysisFeedbackSchema,
  anomalyDetailSchema,
  anomalyPageSchema,
  caseActivityPageSchema,
  caseLinksSchema,
  caseSchema,
  casesSchema,
  caseSnapshotsSchema,
  catalogSchema,
  auditPageSchema,
  type AuditEvent,
  evidencePageSchema,
  collectorsSchema,
  exportsSchema,
  feedbackMetricsSchema,
  formatDuration,
  membersSchema,
  operationsStatusSchema,
  overviewSchema,
  queryString,
  releaseAuditSchema,
  releaseSchema,
  releasesSchema,
  runQuery,
  sourcesSchema,
  statsBuckets,
  statsMetric,
  statsTotal,
  eventProvenance,
  anomalyWindow,
  getEntity,
  getEntityBaseline,
  getEntityRisk,
  groupEntities,
  listEntityAttributions,
  listEntityFeatures,
  listEntityRelations,
  listEntities,
  type AnomalySummary,
  type EntityAggregate,
  type EntityAttribution,
  type EntityRelation,
  type EntityFeatureSample,
  type EntitySummary,
  type Case,
  type CaseLink,
  type CaseSnapshot,
  type CatalogDataset,
  type CollectorSummary,
  type ExportJob,
  type Member,
  type QueryResult,
  type Release,
  type SourceInstance,
} from "./api";
import { PublisherPanel } from "./workbench";
import { QueryBookmarks } from "./query-bookmarks";
import { OverviewInsights } from "./overview-insights";
import { useAuth } from "./auth";
import {
  ActivityTimeline,
  AnomalyStatusTag,
  CaseProgress,
  CaseStatusTag,
  CreateCaseModal,
  EmptyState,
  ErrorState,
  EvidenceTimeline,
  LoadingBlock,
  MetricPanel,
  PageHeader,
  RoleTag,
  SeverityTag,
  TimeValue,
  VerdictTag,
  roleLabels,
  type CasePrefill,
} from "./components";

const anomalyTypeLabels: Record<string, string> = {
  "auth.failure-then-success": "失败后成功登录",
  "auth.failure-burst": "登录失败集中发生",
};

function anomalyTitle(value: AnomalySummary): string {
  return anomalyTypeLabels[value.type] ?? value.type;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "请求失败";
}

function BackLink({ to, children }: { to: string; children: string }) {
  return (
    <Link className="back-link" to={to}>
      <ArrowLeft size={15} />
      {children}
    </Link>
  );
}

export function Overview() {
  const { token, principal } = useAuth();
  const navigate = useNavigate();
  const overview = useQuery({
    queryKey: ["overview"],
    queryFn: ({ signal }) => api("/overview", token, undefined, overviewSchema, signal),
  });
  const recent = useQuery({
    queryKey: ["anomalies", "recent"],
    queryFn: ({ signal }) =>
      api(
        "/anomalies?limit=6",
        token,
        undefined,
        anomalyPageSchema,
        signal,
      ),
  });

  if (overview.isLoading || recent.isLoading) return <LoadingBlock rows={8} />;
  if (overview.error) return <ErrorState message={errorMessage(overview.error)} retry={() => void overview.refetch()} />;
  if (!overview.data) return <EmptyState title="暂无总览数据" description="当前租户尚未产生可展示的统计信息。" />;

  const freshness = overview.data.latest_anomaly
    ? Math.max(0, (Date.now() - new Date(overview.data.latest_anomaly).getTime()) / 1000)
    : null;
  const anomalyColumns: TableProps<AnomalySummary>["columns"] = [
    {
      title: "风险",
      dataIndex: "severity",
      width: 86,
      render: (value: string) => <SeverityTag value={value} />,
    },
    {
      title: "异常",
      key: "title",
      render: (_, value) => (
        <div className="primary-cell">
          <Link to={`/anomalies/${encodeURIComponent(value.id)}`}>{anomalyTitle(value)}</Link>
          <small><EntityName id={value.entity.id}/></small>
        </div>
      ),
    },
    {
      title: "证据",
      dataIndex: "evidence_count",
      width: 76,
      render: (value: number) => `${value} 条`,
    },
    {
      title: "时间",
      dataIndex: "timestamp",
      width: 104,
      render: (value: string) => <TimeValue value={value} />,
    },
  ];

  return (
    <div className="page-stack overview-page">  <PageHeader
        eyebrow={`${principal?.organization_id ?? "租户"} / 24 小时视角`}
        title="安全总览"
        description="聚焦待处理异常、认证事件趋势和案件分派情况。"
        actions={
          <Button type="primary" onClick={() => navigate("/anomalies")} icon={<Search size={16} />}>
            开始调查
          </Button>
        }
      />
      <section className="metric-grid reveal-group">
        <MetricPanel
          label="开放异常"
          value={overview.data.open_anomalies}
          detail={`其中 ${overview.data.high_risk} 条为高危`}
          tone={overview.data.high_risk > 0 ? "danger" : "good"}
          icon={<CircleAlert size={20} />}
        />
        <MetricPanel
          label="处理中案件"
          value={overview.data.cases.active_cases}
          detail={`${overview.data.cases.unassigned_cases} 个尚未分派`}
          tone={overview.data.cases.unassigned_cases > 0 ? "warn" : "good"}
          icon={<FolderKanban size={20} />}
        />
        <MetricPanel
          label="最近异常距今"
          value={formatDuration(freshness)}
          detail="按最近异常发现时间计算"
          tone={freshness !== null && freshness < 300 ? "good" : "warn"}
          icon={<Gauge size={20} />}
        />
        <MetricPanel
          label="今日结案"
          value={overview.data.cases.closed_today}
          detail={`${overview.data.cases.critical_cases} 个紧急案件仍在处理`}
          tone={overview.data.cases.critical_cases > 0 ? "danger" : "neutral"}
          icon={<ShieldCheck size={20} />}
        />
      </section>

      <OverviewInsights mainBelow={(
<article className="panel panel-wide">
          <header className="panel-head">
            <div>
              <span className="panel-index">01</span>
              <h2>优先调查</h2>
              <p>按事件时间排序的最近异常</p>
            </div>
            <Link className="text-link" to="/anomalies">
              查看全部 <ArrowRight size={14} />
            </Link>
          </header>
          {recent.error ? (
            <ErrorState message={errorMessage(recent.error)} retry={() => void recent.refetch()} />
          ) : recent.data?.items.length ? (
            <Table
              className="compact-table"
              rowKey="id"
              columns={anomalyColumns}
              dataSource={recent.data.items}
              pagination={false}
            />
          ) : (
            <EmptyState title="当前没有异常" description="新的检测结果会出现在这里。" />
          )}
        </article>
      )} asideBelow={(
<article className="panel case-balance">
          <header className="panel-head">
            <div>
              <span className="panel-index">02</span>
              <h2>案件负载</h2>
              <p>当前责任分布</p>
            </div>
          </header>
          <div className="balance-figure">
            <strong>{overview.data.cases.active_cases}</strong>
            <span>进行中</span>
          </div>
          <Progress
            percent={Math.min(
              100,
              overview.data.cases.active_cases === 0
                ? 100
                : Math.round(
                    ((overview.data.cases.active_cases - overview.data.cases.unassigned_cases) /
                      overview.data.cases.active_cases) *
                      100,
                  ),
            )}
            showInfo={false}
            strokeColor="#167c72"
            railColor="#e1e8e4"
          />
          <dl className="inline-facts">
            <div>
              <dt>已分派</dt>
              <dd>{overview.data.cases.active_cases - overview.data.cases.unassigned_cases}</dd>
            </div>
            <div>
              <dt>待分派</dt>
              <dd>{overview.data.cases.unassigned_cases}</dd>
            </div>
            <div>
              <dt>紧急</dt>
              <dd>{overview.data.cases.critical_cases}</dd>
            </div>
          </dl>
          <Button onClick={() => navigate("/cases")} block icon={<FolderKanban size={16} />}>
            进入案件中心
          </Button>
        </article>
      )}/>
    </div>
  );
}

type AnomalyFilters = {
  severity: string;
  status: string;
  entity: string;
};

export function FeedbackMetricsPanel() {
  const { token, can } = useAuth();
  const enabled = can("analysis:feedback");
  const query = useQuery({
    queryKey: ["feedback-metrics"],
    enabled,
    queryFn: ({ signal }) => api("/analysis/feedback/metrics", token, undefined, feedbackMetricsSchema, signal),
  });
  if (!enabled) return null;
  if (query.error) {
    return (
      <section className="panel">
        <header className="panel-header">
          <h3>规则反馈质量</h3>
        </header>
        <ErrorState message={errorMessage(query.error)} retry={() => void query.refetch()} />
      </section>
    );
  }
  if (query.isLoading || !query.data || query.data.rules.length === 0) return null;
  return (
    <section className="panel">
      <header className="panel-header">
        <h3>规则反馈质量</h3>
        <span className="muted-text">当前组织的分析师反馈汇总；未绑定规则的反馈单独列出。</span>
      </header>
      <Table
        rowKey="rule_id"
        size="small"
        pagination={false}
        dataSource={query.data.rules}
        columns={[
          { title: "规则", dataIndex: "rule_id", render: (value: string) => value ? <Tooltip title={`规则 ID：${value}`}><span>{anomalyTypeLabels[value] ?? value}</span></Tooltip> : <Tooltip title="反馈记录没有绑定具体规则，可能来自案件判定。"><span>未关联具体规则</span></Tooltip> },
          { title: <Tooltip title="分析师确认检测到的异常确实存在。">确认异常</Tooltip>, dataIndex: "true_positive", width: 100 },
          { title: <Tooltip title="规则产生了告警，但分析师判定并非真实异常。">误报</Tooltip>, dataIndex: "false_positive", width: 90 },
          { title: <Tooltip title="实际存在异常，但规则没有检测出来；由分析师补录。">漏报</Tooltip>, dataIndex: "false_negative", width: 90 },
          { title: <Tooltip title="现有证据不足，暂时无法判定是否为真实异常。">不确定</Tooltip>, dataIndex: "inconclusive", width: 90 },
          {
            title: <Tooltip title="确认异常 ÷（确认异常 + 误报）；漏报和不确定不参与计算。仅反映已反馈记录。">精确率</Tooltip>,
            dataIndex: "precision",
            width: 110,
            render: (value: number, row) => row.true_positive + row.false_positive > 0 ? `${(value * 100).toFixed(1)}%` : <Tooltip title="没有确认异常或误报反馈，暂无法计算。">—</Tooltip>,
          },
        ]}
      />
    </section>
  );
}

function AnomalyEntity({entity}: {entity: AnomalySummary["entity"]}) {
  const {token} = useAuth();
  const isReference = /^ent:[a-f0-9]{64}$/.test(entity.id);
  const profile = useQuery({
    queryKey: ["entities", entity.id, "profile"],
    enabled: isReference,
    staleTime: 300_000,
    retry: false,
    queryFn: ({signal}) => getEntity(token, entity.id, signal),
  });
  const name = profile.data?.canonical_key || readable(entity.id, "实体名称暂不可用");
  const type = entity.type === "account" ? "账户" : entity.type === "device" ? "主机" : entity.type;
  return <div className="entity-cell">
    <Tooltip title={entity.id}><Link to={`/entities/${encodeURIComponent(entity.id)}`}>{name}</Link></Tooltip>
    <small>{type}{profile.data?.authority ? ` · ${profile.data.authority}` : ""}{profile.isLoading ? " · 解析中" : profile.isError ? " · 名称暂不可用" : ""}</small>
  </div>;
}

export function Anomalies() {
  const { token } = useAuth();
  const [hours,setHours] = useState(168);
  const from = useMemo(()=>new Date(Date.now()-hours*3600_000).toISOString(),[hours]);
  const [draft, setDraft] = useState<AnomalyFilters>({ severity: "", status: "", entity: "" });
  const [filters, setFilters] = useState<AnomalyFilters>(draft);
  const query = useInfiniteQuery({
    queryKey: ["anomalies", filters, from],
    initialPageParam: "",
    queryFn: ({ pageParam, signal }) =>
      api(
        `/anomalies${queryString({ limit: 50, cursor: pageParam, from, ...filters })}`,
        token,
        undefined,
        anomalyPageSchema,
        signal,
      ),
    getNextPageParam: (last) => last.next_cursor || undefined,
  });
  const items = query.data?.pages.flatMap((page) => page.items) ?? [];
  const total = query.data?.pages[0]?.total ?? items.length;

  const columns: TableProps<AnomalySummary>["columns"] = [
    {
      title: "风险",
      dataIndex: "severity",
      width: 82,
      render: (value: string) => <SeverityTag value={value} />,
    },
    {
      title: "异常",
      key: "anomaly",
      render: (_, value) => (
        <div className="primary-cell">
          <Link to={`/anomalies/${encodeURIComponent(value.id)}`}>{anomalyTitle(value)}</Link>
          <Tooltip title={<div><div>规则 ID：{value.rule_id || "未提供"}</div><div>异常 ID：{value.id}</div></div>}>
            <small>检测规则：{value.rule_id ? (anomalyTypeLabels[value.rule_id] ?? value.rule_id) : "未提供"} · 版本：{value.rule_version || "未提供"}</small>
          </Tooltip>
        </div>
      ),
    },
    {
      title: "实体",
      key: "entity",
      width: 170,
      render: (_, value) => <AnomalyEntity entity={value.entity} />,
    },
    {
      title: "状态",
      dataIndex: "status",
      width: 96,
      render: (value: string) => <AnomalyStatusTag value={value} />,
    },
    {
      title: "证据",
      dataIndex: "evidence_count",
      width: 72,
      render: (value: number) => `${value} 条`,
    },
    {
      title: "时间",
      dataIndex: "timestamp",
      width: 112,
      render: (value: string) => <TimeValue value={value} />,
    },
    {
      title: "",
      key: "action",
      width: 52,
      render: (_, value) => (
        <Tooltip title="进入调查">
          <Link aria-label={`调查 ${value.id}`} className="icon-link" to={`/anomalies/${encodeURIComponent(value.id)}`}>
            <ChevronRight size={18} />
          </Link>
        </Tooltip>
      ),
    },
  ];

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="调查队列"
        title="异常调查"
        description="以实体、严重度和处置状态缩小调查范围。"
      />
      <FeedbackMetricsPanel />
      <section className="filter-bar">
        <div className="filter-search">
          <Search size={16} />
          <Input
            variant="borderless"
            placeholder="输入异常记录中的账户或主机标识"
            value={draft.entity}
            onChange={(event) => setDraft((value) => ({ ...value, entity: event.target.value }))}
            onPressEnter={() => setFilters(draft)}
          />
        </div>
        <Select aria-label="异常查询时间范围" value={hours} onChange={setHours} options={[{value:24,label:'最近 24 小时'},{value:72,label:'最近 3 天'},{value:168,label:'最近 7 天'},{value:720,label:'最近 30 天'}]}/>
        <Select
          value={draft.severity}
          onChange={(severity) => setDraft((value) => ({ ...value, severity }))}
          options={[
            { value: "", label: "全部严重度" },
            { value: "critical", label: "紧急" },
            { value: "high", label: "高危" },
            { value: "medium", label: "中危" },
            { value: "low", label: "低危" },
          ]}
        />
        <Select
          value={draft.status}
          onChange={(status) => setDraft((value) => ({ ...value, status }))}
          options={[
            { value: "", label: "全部状态" },
            { value: "open", label: "开放" },
            { value: "investigating", label: "调查中" },
            { value: "closed", label: "已关闭" },
            { value: "false_positive", label: "误报" },
          ]}
        />
        <Button type="primary" icon={<Filter size={16} />} onClick={() => setFilters(draft)}>
          应用筛选
        </Button>
        <Button
          onClick={() => {
            const empty = { severity: "", status: "", entity: "" };
            setDraft(empty);
            setFilters(empty);
          }}
        >
          清除
        </Button>
      </section>
      <section className="data-panel">
        <div className="data-summary">
          <span>匹配 {total} 条 · 最近 {hours===24?'24 小时':`${hours/24} 天`}</span>
          {query.isFetching && <span className="fetching"><RefreshCw size={13} /> 更新中</span>}
        </div>
        {query.isLoading ? (
          <LoadingBlock rows={7} />
        ) : query.error ? (
          <ErrorState message={errorMessage(query.error)} retry={() => void query.refetch()} />
        ) : items.length === 0 ? (
          <EmptyState title="所选时间范围内没有匹配的异常" description="可扩大时间范围，或调整风险等级、状态和实体条件。" />
        ) : (
          <>
            <Table
              rowKey="id"
              columns={columns}
              dataSource={items}
              pagination={false}
              scroll={{ x: 900 }}
            />
            {query.hasNextPage && (
              <div className="load-more">
                <Button
                  loading={query.isFetchingNextPage}
                  onClick={() => void query.fetchNextPage()}
                >
                  加载更多
                </Button>
              </div>
            )}
          </>
        )}
      </section>
    </div>
  );
}

export function AnomalyDetail() {
  const { id = "" } = useParams();
  const { token, can } = useAuth();
  const { message } = AntApp.useApp();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [eventSelected, setEventSelected] = useState<EventRow>();
  const [createOpen, setCreateOpen] = useState(false);
  const [feedbackOpen, setFeedbackOpen] = useState(false);
  const detail = useQuery({
    queryKey: ["anomaly", id],
    enabled: Boolean(id),
    queryFn: ({ signal }) =>
      api(`/anomalies/${encodeURIComponent(id)}`, token, undefined, anomalyDetailSchema, signal),
  });
  const evidence = useQuery({
    queryKey: ["anomaly", id, "evidence"],
    enabled: Boolean(id),
    queryFn: ({ signal }) =>
      api(
        `/anomalies/${encodeURIComponent(id)}/evidence?limit=100`,
        token,
        undefined,
        evidencePageSchema,
        signal,
      ),
  });
  const feedback = useMutation({
    mutationFn: (values: { feedback_type: "true_positive" | "false_positive" | "inconclusive"; reason: string }) =>
      api(
        "/analysis/feedback",
        token,
        {
          method: "POST",
          body: JSON.stringify({
            ...values,
            anomaly_id: detail.data?.id ?? id,
            rule_id: detail.data?.rule_id,
            entity_id: detail.data?.entity.id,
            event_ids: detail.data?.evidence_event_ids ?? [],
          }),
        },
        analysisFeedbackSchema,
      ),
    onSuccess: () => {
      void queryClient.invalidateQueries({queryKey:["feedback-evaluation"]});
      void queryClient.invalidateQueries({queryKey:["feedback-metrics"]});
      void message.success("分析反馈已记录");
      setFeedbackOpen(false);
    },
    onError: (error) => void message.error(errorMessage(error)),
  });
  const prefill: CasePrefill | undefined = detail.data
    ? {
        title: `${anomalyTitle(detail.data)} 调查`,
        description: detail.data.summary,
        severity: detail.data.severity,
        anomalyIds: [detail.data.id],
      }
    : undefined;

  if (detail.isLoading) return <LoadingBlock rows={8} />;
  if (detail.error) return <ErrorState message={errorMessage(detail.error)} retry={() => void detail.refetch()} />;
  if (!detail.data) return <EmptyState title="异常不存在" description="该异常可能已被删除或不属于当前租户。" />;
  const anomaly = detail.data;

  return (
    <div className="page-stack anomaly-detail-page">
      <BackLink to="/anomalies">返回异常队列</BackLink>
      <PageHeader
        eyebrow="异常调查 / 详情"
        title={anomalyTitle(anomaly)}
        description={anomaly.summary?.replace(/(\d+) failed logins followed by a successful login within (\d+) minutes/,"$2 分钟内发生 $1 次登录失败，随后出现一次成功登录").replace(/(\d+) failed logins within (\d+) minutes \(threshold (\d+)\)/,"$2 分钟内发生 $1 次登录失败，达到检测阈值 $3 次") || "查看触发原因与事件证据，判断是否需要处置。"}
        actions={
          <Space wrap>
            {can("analysis:feedback") && (
              <Button onClick={() => setFeedbackOpen(true)}>记录反馈</Button>
            )}
            {can("case:write") && (
              <Button type="primary" icon={<Plus size={16} />} onClick={() => setCreateOpen(true)}>
                创建案件
              </Button>
            )}
          </Space>
        }
      />

      <section className="anomaly-hero">
        <div className="anomaly-score-value"><strong>{anomaly.score.toFixed(1)}</strong><span>检测分值</span><Tooltip title="规则返回的原始分值，不代表风险百分比或威胁概率。"><small>评分说明</small></Tooltip></div>
        <div className="anomaly-identity">
          <Space wrap>
            <SeverityTag value={anomaly.severity} />
            <AnomalyStatusTag value={anomaly.status} />
            <Tag className="signal-tag">{readable(anomaly.entity.type)}</Tag>
          </Space>
          <strong><EntityName id={anomaly.entity.id}/></strong>
          <Link to={`/entities/${encodeURIComponent(anomaly.entity.id)}`}>查看账户或主机详情 →</Link>
        </div>
        <dl>
          <div>
            <dt>触发时间</dt>
            <dd><TimeValue value={anomaly.timestamp} /></dd>
          </div>
          <div>
            <dt>证据事件</dt>
            <dd>{anomaly.evidence_count} 条</dd>
          </div>
          <div>
            <dt>检测版本</dt>
            <dd>{anomaly.rule_version}</dd>
          </div>
        </dl>
      </section>

      <section className="detail-layout">
        <article className="panel">
          <header className="panel-head">
            <div>
              <span className="panel-index">01</span>
              <h2>证据时间线</h2>
              <p>按返回顺序展示该异常引用的认证事件，点击可查看详细证据</p>
            </div>
            {evidence.data?.truncated && <Tag color="warning">结果已截断</Tag>}
          </header>
          {evidence.isLoading ? (
            <LoadingBlock rows={6} />
          ) : evidence.error ? (
            <ErrorState message={errorMessage(evidence.error)} retry={() => void evidence.refetch()} />
          ) : evidence.data?.items.length ? (
            <EvidenceTimeline items={evidence.data.items} onSelect={item=>setEventSelected({...item,"@timestamp":item.timestamp,event_id:item.id})} />
          ) : (
            <EmptyState title="没有可用证据" description="异常引用的原始事件尚未索引或已被保留策略清理。" />
          )}
        </article>

        <aside className="context-column">
          <article className="panel">
            <header className="panel-head">
              <div>
                <span className="panel-index">02</span>
                <h2>检测解释</h2>
              </div>
            </header>
            <div className="reason-list">
              {anomaly.reason_codes.length ? (
                anomaly.reason_codes.map((reason) => (
                  <div key={reason}>
                    <Check size={15} />
                    <ReadableValue value={reason}/>
                  </div>
                ))
              ) : (
                <span className="muted">规则尚未提供具体触发原因</span>
              )}
            </div>
          </article>
          <article className="panel">
            <header className="panel-head">
              <div>
                <span className="panel-index">03</span>
                <h2>如何调查与处理</h2>
              </div>
            </header>
            <ol className="anomaly-next-steps"><li>核对失败与成功登录是否来自本人，检查事件时间和来源地址。</li><li>证据不足时继续调查；确认结果后点击“记录反馈”。</li><li>需要持续跟进或协作处置时，点击“创建案件”。</li></ol>
          </article>
        </aside>
      </section>

      {eventSelected&&<EventDetailDrawer item={eventSelected} onClose={()=>setEventSelected(undefined)}/>}
      <CreateCaseModal
        open={createOpen}
        prefill={prefill}
        onClose={() => setCreateOpen(false)}
        onCreated={(created) => navigate(`/cases/${created.id}`)}
      />
      <Modal title="记录分析反馈" open={feedbackOpen} footer={null} onCancel={() => setFeedbackOpen(false)}>
        <Form
          layout="vertical"
          initialValues={{ feedback_type: "false_positive" }}
          onFinish={(values: { feedback_type: "true_positive" | "false_positive" | "inconclusive"; reason: string }) =>
            feedback.mutate(values)
          }
        >
          <Form.Item name="feedback_type" label="反馈类型" rules={[{ required: true }]}>
            <Select
              options={[
                { value: "true_positive", label: "确认威胁" },
                { value: "false_positive", label: "误报" },
                { value: "inconclusive", label: "暂不确定" },
              ]}
            />
          </Form.Item>
          <Form.Item name="reason" label="反馈说明" rules={[{ required: true, max: 2000 }]}>
            <Input.TextArea rows={4} maxLength={2000} showCount />
          </Form.Item>
          <div className="modal-actions">
            <Button onClick={() => setFeedbackOpen(false)}>取消</Button>
            <Button type="primary" htmlType="submit" loading={feedback.isPending}>提交反馈</Button>
          </div>
        </Form>
      </Modal>
    </div>
  );
}

type CaseFilters = {
  q: string;
  status: string;
  severity: string;
};

export function Cases() {
  const { token, can } = useAuth();
  const [view,setView]=useState("list");
  const [createOpen, setCreateOpen] = useState(false);
  const [draft, setDraft] = useState<CaseFilters>({ q: "", status: "", severity: "" });
  const [filters, setFilters] = useState<CaseFilters>(draft);
  const overview = useQuery({
    queryKey: ["overview"],
    queryFn: ({ signal }) => api("/overview", token, undefined, overviewSchema, signal),
  });
  const query = useInfiniteQuery({
    queryKey: ["cases", filters],
    initialPageParam: "",
    queryFn: ({ pageParam, signal }) =>
      api(
        `/cases${queryString({ limit: 50, cursor: pageParam, ...filters })}`,
        token,
        undefined,
        casesSchema,
        signal,
      ),
    getNextPageParam: (last) => last.next_cursor || undefined,
  });
  const items = query.data?.pages.flatMap((page) => page.items) ?? [];

  const columns: TableProps<Case>["columns"] = [
    {
      title: "案件",
      key: "case",
      render: (_, value) => (
        <div className="primary-cell">
          <Link to={`/cases/${value.id}`}>{value.title}</Link>
          <small>{readableDescription(value.description)}</small>
        </div>
      ),
    },
    {
      title: "优先级",
      dataIndex: "severity",
      width: 88,
      render: (value: string) => <SeverityTag value={value} />,
    },
    {
      title: "状态",
      dataIndex: "status",
      width: 140,
      render: (value: string, record) => (
        <Space size={4}>
          <CaseStatusTag value={value} />
          {record.hold && (
            <Tooltip title={`证据保留：${record.hold_reason || "已启用"}`}>
              <Tag className="signal-tag status-open" icon={<Lock size={12} />}>保留</Tag>
            </Tooltip>
          )}
        </Space>
      ),
    },
    {
      title: "负责人",
      dataIndex: "assignee",
      width: 150,
      render: (value: string) => value || <span className="muted">未分派</span>,
    },
    {
      title: "关联异常",
      dataIndex: "anomaly_ids",
      width: 96,
      render: (value: string[]) => `${value.length} 条`,
    },
    {
      title: "最后更新",
      dataIndex: "updated_at",
      width: 112,
      render: (value: string) => <TimeValue value={value} />,
    },
    {
      title: "",
      key: "action",
      width: 50,
      render: (_, value) => (
        <Tooltip title="打开案件">
          <Link aria-label={`案件 ${value.id}`} className="icon-link" to={`/cases/${value.id}`}>
            <ChevronRight size={18} />
          </Link>
        </Tooltip>
      ),
    },
  ];

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="调查责任"
        title="案件中心"
        description="跟踪分派、处置进度、判定结论和审计活动。"
        actions={
          can("case:write") ? (
            <Button type="primary" icon={<Plus size={16} />} onClick={() => setCreateOpen(true)}>
              新建案件
            </Button>
          ) : undefined
        }
      />
      <section className="metric-grid metric-grid-compact">
        <MetricPanel label="进行中" value={overview.data?.cases.active_cases ?? "—"} detail="当前处置负载" icon={<FolderKanban size={19} />} />
        <MetricPanel label="待分派" value={overview.data?.cases.unassigned_cases ?? "—"} detail="需要明确责任人" tone="warn" icon={<Users size={19} />} />
        <MetricPanel label="今日结案" value={overview.data?.cases.closed_today ?? "—"} detail="已完成判定和归档" tone="good" icon={<ShieldCheck size={19} />} />
      </section>
      <Tabs activeKey={view} onChange={setView} items={[{key:"list",label:"案件列表"},{key:"board",label:"处置看板"}]}/>
      <section className="filter-bar">
        <div className="filter-search">
          <Search size={16} />
          <Input
            variant="borderless"
            placeholder="搜索案件标题或说明"
            value={draft.q}
            onChange={(event) => setDraft((value) => ({ ...value, q: event.target.value }))}
            onPressEnter={() => setFilters(draft)}
          />
        </div>
        <Select
          value={draft.status}
          onChange={(status) => setDraft((value) => ({ ...value, status }))}
          options={[
            { value: "", label: "全部状态" },
            { value: "open", label: "待分派" },
            { value: "in_progress", label: "调查中" },
            { value: "closed", label: "已结案" },
          ]}
        />
        <Select
          value={draft.severity}
          onChange={(severity) => setDraft((value) => ({ ...value, severity }))}
          options={[
            { value: "", label: "全部优先级" },
            { value: "critical", label: "紧急" },
            { value: "high", label: "高危" },
            { value: "medium", label: "中危" },
            { value: "low", label: "低危" },
          ]}
        />
        <Button type="primary" icon={<Filter size={16} />} onClick={() => setFilters(draft)}>
          应用筛选
        </Button>
      </section>
      <section className="data-panel">
        <div className="data-summary">
          <span>当前显示 {items.length} 个案件</span>
          {query.isFetching && <span className="fetching"><RefreshCw size={13} /> 更新中</span>}
        </div>
        {query.isLoading ? (
          <LoadingBlock rows={7} />
        ) : query.error ? (
          <ErrorState message={errorMessage(query.error)} retry={() => void query.refetch()} />
        ) : items.length === 0 ? (
          <EmptyState title="没有匹配的案件" description="调整筛选条件，或从异常详情创建新案件。" />
        ) : (
          <>
            {view==='list'?<Table rowKey="id" columns={columns} dataSource={items} pagination={false} scroll={{ x: 900 }} />:<div className="wb-kanban">{[['open','待分派'],['in_progress','调查中'],['closed','已结案']].map(([status,label])=><section className="wb-lane" key={status}><h3>{label}<small>{items.filter(x=>x.status===status).length}</small></h3>{items.filter(x=>x.status===status).map(x=><Link className="wb-case-card" to={'/cases/'+x.id} key={x.id}><SeverityTag value={x.severity}/><h3>{x.title}</h3><small>{x.assignee||'未分派'} · 异常 {x.anomaly_ids.length}</small></Link>)}</section>)}</div>}
            {query.hasNextPage && (
              <div className="load-more">
                <Button loading={query.isFetchingNextPage} onClick={() => void query.fetchNextPage()}>
                  加载更多
                </Button>
              </div>
            )}
          </>
        )}
      </section>
      <CreateCaseModal open={createOpen} onClose={() => setCreateOpen(false)} />
    </div>
  );
}

type CaseMutationInput = {
  status?: "open" | "in_progress" | "closed";
  assignee?: string;
  verdict?: "true_positive" | "benign_positive" | "false_positive" | "inconclusive";
  reason?: string;
};

export function CaseDetail() {
  const { id = "" } = useParams();
  const { token, can } = useAuth();
  const { message } = AntApp.useApp();
  const queryClient = useQueryClient();
  const [tab,setTab]=useState("workspace");
  const [closeOpen, setCloseOpen] = useState(false);
  const [assignOpen, setAssignOpen] = useState(false);
  const [holdOpen, setHoldOpen] = useState(false);
  const [linkOpen, setLinkOpen] = useState(false);
  const [snapshotOpen, setSnapshotOpen] = useState(false);
  const detail = useQuery({
    queryKey: ["case", id],
    enabled: Boolean(id),
    queryFn: ({ signal }) => api(`/cases/${encodeURIComponent(id)}`, token, undefined, caseSchema, signal),
  });
  const activity = useQuery({
    queryKey: ["case", id, "activity"],
    enabled: Boolean(id),
    queryFn: ({ signal }) =>
      api(
        `/cases/${encodeURIComponent(id)}/activity?limit=100`,
        token,
        undefined,
        caseActivityPageSchema,
        signal,
      ),
  });
  const links = useQuery({
    queryKey: ["case", id, "links"],
    enabled: Boolean(id),
    queryFn: ({ signal }) =>
      api(`/cases/${encodeURIComponent(id)}/links`, token, undefined, caseLinksSchema, signal),
  });
  const snapshots = useQuery({
    queryKey: ["case", id, "snapshots"],
    enabled: Boolean(id),
    queryFn: ({ signal }) =>
      api(`/cases/${encodeURIComponent(id)}/snapshots`, token, undefined, caseSnapshotsSchema, signal),
  });
  const refreshCase = () => {
    void queryClient.invalidateQueries({ queryKey: ["case", id] });
    void queryClient.invalidateQueries({ queryKey: ["cases"] });
    void queryClient.invalidateQueries({ queryKey: ["overview"] });
  };
  const conflictMessage = (error: unknown) => {
    const text = errorMessage(error);
    return text.includes("conflict") ? "案件已被其他用户更新，请刷新后重试" : text;
  };
  const update = useMutation({
    mutationFn: (input: CaseMutationInput) =>
      api<Case>(
        `/cases/${encodeURIComponent(id)}/actions`,
        token,
        {
          method: "POST",
          body: JSON.stringify({ ...input, expected_version: detail.data?.version }),
        },
        caseSchema,
      ),
    onSuccess: () => {
      void message.success("案件处置已记录");
      setCloseOpen(false);
      setAssignOpen(false);
      refreshCase();
    },
    onError: (error) => void message.error(conflictMessage(error)),
  });
  const holdMutation = useMutation({
    mutationFn: (input: { hold: boolean; reason: string }) =>
      api<{ case: Case; changed: boolean }>(
        `/cases/${encodeURIComponent(id)}/hold`,
        token,
        {
          method: "POST",
          body: JSON.stringify({ ...input, expected_version: detail.data?.version }),
        },
      ),
    onSuccess: (result) => {
      void message.success(result.changed ? "保留标识已更新" : "保留标识已是该状态（幂等重放，未变更）");
      setHoldOpen(false);
      refreshCase();
    },
    onError: (error) => void message.error(conflictMessage(error)),
  });
  const linkMutation = useMutation({
    mutationFn: (input: { link_type: string; ref_kind: string; target_id: string }) =>
      api<{ case: Case; added: boolean }>(
        `/cases/${encodeURIComponent(id)}/links`,
        token,
        {
          method: "POST",
          body: JSON.stringify({ ...input, expected_version: detail.data?.version }),
        },
      ),
    onSuccess: (result) => {
      void message.success(result.added ? "关联已添加" : "该关联已存在（幂等重放，未重复添加）");
      setLinkOpen(false);
      refreshCase();
      void queryClient.invalidateQueries({ queryKey: ["case", id, "links"] });
    },
    onError: (error) => void message.error(conflictMessage(error)),
  });
  const snapshotMutation = useMutation({
    mutationFn: (input: { label: string }) =>
      api<CaseSnapshot>(
        `/cases/${encodeURIComponent(id)}/snapshots`,
        token,
        {
          method: "POST",
          body: JSON.stringify({ ...input, expected_version: detail.data?.version }),
        },
      ),
    onSuccess: () => {
      void message.success("取证快照已冻结");
      setSnapshotOpen(false);
      refreshCase();
      void queryClient.invalidateQueries({ queryKey: ["case", id, "snapshots"] });
    },
    onError: (error) => void message.error(conflictMessage(error)),
  });

  if (detail.isLoading) return <LoadingBlock rows={8} />;
  if (detail.error) return <ErrorState message={errorMessage(detail.error)} retry={() => void detail.refetch()} />;
  if (!detail.data) return <EmptyState title="案件不存在" description="该案件可能不存在或不属于当前租户。" />;
  const current = detail.data;
  const writable = can("case:write");

  return (
    <div className="page-stack">
      <BackLink to="/cases">返回案件中心</BackLink>
      <PageHeader
        eyebrow={`调查案件 / 第 ${current.version} 次更新`}
        title={current.title}
        description={readableDescription(current.description)}
        actions={
          writable ? (
            <Space wrap>
              {current.status === "open" && (
                <Button
                  loading={update.isPending}
                  onClick={() => update.mutate({ status: "in_progress", reason: "开始调查" })}
                >
                  开始调查
                </Button>
              )}
              {current.status !== "closed" && (
                <>
                  <Button onClick={() => setAssignOpen(true)}>分派</Button>
                  <Button type="primary" onClick={() => setCloseOpen(true)}>记录结论并结案</Button>
                </>
              )}
              {current.status === "closed" && (
                <Button onClick={() => update.mutate({ status: "open", reason: "重新打开案件" })}>
                  重新打开
                </Button>
              )}
              <Button
                icon={<Lock size={15} />}
                danger={Boolean(current.hold)}
                onClick={() => setHoldOpen(true)}
              >
                {current.hold ? "解除保留" : "设置保留"}
              </Button>
            </Space>
          ) : undefined
        }
      />
      {current.hold && (
        <Alert
          type="warning"
          showIcon
          icon={<Lock size={16} />}
          title={`证据保留已启用：${current.hold_reason || "未填写原因"}`}
          description="保留期间，引用该案件处理作业证据的清理任务不会清理相关数据；解除保留后恢复可清理。"
        />
      )}
      {update.error instanceof APIError&&update.error.status===409&&<Alert showIcon type="warning" title="案件版本已发生变化" description="编辑内容保留。刷新当前版本，核对状态与结论差异后再提交。" action={<Button onClick={()=>void detail.refetch()}>刷新当前版本</Button>}/>}
      <CaseProgress status={current.status} />
      <section className="case-facts">
        <div><span>优先级</span><SeverityTag value={current.severity} /></div>
        <div><span>状态</span><CaseStatusTag value={current.status} /></div>
        <div><span>负责人</span><strong>{current.assignee || "未分派"}</strong></div>
        <div><span>判定结论</span><VerdictTag value={current.verdict} /></div>
        <div><span>保留标识</span><strong>{current.hold ? "已保留" : "未保留"}</strong></div>
        <div><span>关联异常</span><strong>{current.anomaly_ids.length} 条</strong></div>
        <div><span>最后更新</span><TimeValue value={current.updated_at} /></div>
      </section>
      <Tabs activeKey={tab} onChange={setTab} items={[{key:'workspace',label:'处置工作区'},{key:'snapshots',label:'证据与快照'},{key:'verdict',label:'反馈判定'}]}/>
      {tab==='verdict'&&<section className="panel"><header className="panel-head"><h2>调查判定</h2></header><Descriptions column={1} items={[{key:'result',label:'当前判定',children:<VerdictTag value={current.verdict}/>},{key:'reason',label:'依据',children:current.verdict_reason||'尚未记录'}]}/>{writable&&<Form layout="vertical" className="wb-panel-body" onFinish={(values:{verdict:CaseMutationInput['verdict'];reason:string})=>update.mutate(values)}><Form.Item name="verdict" label="判定结果" rules={[{required:true}]}><Select options={[{value:'true_positive',label:'真实异常'},{value:'benign_positive',label:'有效但无害'},{value:'false_positive',label:'误报'},{value:'inconclusive',label:'证据不足'}]}/></Form.Item><Form.Item name="reason" label="证据与判定依据" rules={[{required:true,max:2000}]}><Input.TextArea rows={4}/></Form.Item><Button type="primary" htmlType="submit" loading={update.isPending}>提交判定</Button></Form>}</section>}
      <section className="detail-layout" hidden={tab!=='workspace'}>
        <article className="panel">
          <header className="panel-head">
            <div>
              <span className="panel-index">01</span>
              <h2>处置记录</h2>
              <p>追加式审计时间线，不提供普通修改或删除</p>
            </div>
          </header>
          {activity.isLoading ? (
            <LoadingBlock rows={5} />
          ) : activity.error ? (
            <ErrorState message={errorMessage(activity.error)} retry={() => void activity.refetch()} />
          ) : activity.data?.items.length ? (
            <ActivityTimeline items={activity.data.items} />
          ) : (
            <EmptyState title="暂无处置记录" description="案件状态、指派和判定变更会写入这里。" />
          )}
        </article>
        <aside className="context-column">
          <article className="panel">
            <header className="panel-head">
              <div>
                <span className="panel-index">02</span>
                <h2>关联证据</h2>
              </div>
            </header>
            {current.anomaly_ids.length ? (
              <div className="linked-list">
                {current.anomaly_ids.map((anomalyID,index) => (
                  <Link key={anomalyID} to={`/anomalies/${encodeURIComponent(anomalyID)}`}>
                    <FileSearch size={16} />
                    <Tooltip title={`异常标识：${anomalyID}`}><span>关联异常 {index+1} · 查看详情</span></Tooltip>
                    <ChevronRight size={16} />
                  </Link>
                ))}
              </div>
            ) : (
              <EmptyState title="未关联异常" description="可以从异常详情创建案件并关联证据。" />
            )}
          </article>
          <article className="panel">
            <header className="panel-head">
              <div>
                <span className="panel-index">03</span>
                <h2>结论摘要</h2>
              </div>
            </header>
            <Descriptions column={1} size="small" items={[
              { key: "verdict", label: "判定", children: <VerdictTag value={current.verdict} /> },
              { key: "reason", label: "说明", children: current.verdict_reason || "尚未记录" },
              { key: "closed", label: "结案时间", children: current.closed_at ? <TimeValue value={current.closed_at} /> : "尚未结案" },
            ]} />
          </article>
        </aside>
      </section>
      <section className="operations-grid" hidden={tab!=="snapshots"}>
        <article className="panel">
          <header className="panel-head">
            <div>
              <span className="panel-index">04</span>
              <h2>实体/风险/证据关联</h2>
              <p>重放同一关联为幂等空操作；事件本体在 ES，引用按形状合同校验</p>
            </div>
            {writable && (
              <Button size="small" icon={<Plus size={15} />} onClick={() => setLinkOpen(true)}>
                添加关联
              </Button>
            )}
          </header>
          {links.isLoading ? (
            <LoadingBlock rows={3} />
          ) : links.error ? (
            <ErrorState message={errorMessage(links.error)} retry={() => void links.refetch()} />
          ) : links.data?.items.length ? (
            <Table
              rowKey={(item) => `${item.link_type}-${item.ref_kind}-${item.target_id}`}
              size="small"
              pagination={false}
              dataSource={links.data.items}
              columns={[
                {
                  title: "类型",
                  dataIndex: "link_type",
                  width: 130,
                  render: (value: CaseLink["link_type"]) => (
                    <Tag>
                      {value === "entity" ? "实体" : value === "risk_contribution" ? "风险贡献" : "证据"}
                    </Tag>
                  ),
                },
                { title: "引用类别", dataIndex: "ref_kind", width: 130 },
                {
                  title: "目标",
                  dataIndex: "target_id",
                  render: (value: string) => <ReadableValue value={value}/>,
                },
                {
                  title: "关联时间",
                  dataIndex: "linked_at",
                  width: 130,
                  render: (value: string) => <TimeValue value={value} />,
                },
              ]}
            />
          ) : (
            <EmptyState title="暂无关联" description="可关联实体、风险贡献或事件证据引用。" />
          )}
        </article>
        <article className="panel">
          <header className="panel-head">
            <div>
              <span className="panel-index">05</span>
              <h2>取证快照</h2>
              <p>插入即冻结，不可修改或删除</p>
            </div>
            {writable && (
              <Button size="small" icon={<Archive size={15} />} onClick={() => setSnapshotOpen(true)}>
                冻结快照
              </Button>
            )}
          </header>
          {snapshots.isLoading ? (
            <LoadingBlock rows={3} />
          ) : snapshots.error ? (
            <ErrorState message={errorMessage(snapshots.error)} retry={() => void snapshots.refetch()} />
          ) : snapshots.data?.items.length ? (
            <div className="linked-list">
              {snapshots.data.items.map((snapshot) => (
                <div key={snapshot.id} style={{ display: "flex", gap: 8, alignItems: "center" }}>
                  <Archive size={15} />
                  <div>
                    <strong>{snapshot.label || "调查快照"}</strong>
                    <small>
                      {snapshot.created_by ? `${snapshot.created_by} · ` : ""}
                      <TimeValue value={snapshot.created_at} />
                    </small>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <EmptyState title="暂无快照" description="冻结当前案件头、异常集合与完整链接集。" />
          )}
        </article>
      </section>

      <Modal title={current.hold ? "解除证据保留" : "设置证据保留"} open={holdOpen} footer={null} onCancel={() => setHoldOpen(false)}>
        <Form
          layout="vertical"
          onFinish={(values: { reason: string }) =>
            holdMutation.mutate({ hold: !current.hold, reason: values.reason })
          }
        >
          <Form.Item name="reason" label="保留原因" rules={[{ required: true, max: 2000 }]}>
            <Input.TextArea autoFocus rows={3} maxLength={2000} placeholder={current.hold ? "解除保留也需记录原因" : "例如：案件调查期间需要保留证据"} />
          </Form.Item>
          <div className="modal-actions">
            <Button onClick={() => setHoldOpen(false)}>取消</Button>
            <Button type="primary" danger={Boolean(current.hold)} htmlType="submit" loading={holdMutation.isPending}>
              {current.hold ? "确认解除" : "确认保留"}
            </Button>
          </div>
        </Form>
      </Modal>

      <Modal title="添加案件关联" open={linkOpen} footer={null} onCancel={() => setLinkOpen(false)}>
        <Form
          layout="vertical"
          onFinish={(values: { link_type: string; ref_kind: string; target_id: string }) =>
            linkMutation.mutate(values)
          }
        >
          <Form.Item name="link_type" label="关联类型" rules={[{ required: true }]}>
            <Select
              placeholder="选择关联类型"
              options={[
                { value: "entity", label: "实体（entity_id，需在身份空间可解析）" },
                { value: "risk_contribution", label: "风险贡献（rc: 引用）" },
                { value: "evidence", label: "证据引用" },
              ]}
            />
          </Form.Item>
          <Form.Item noStyle shouldUpdate>
            {({ getFieldValue }) => {
              const linkType = getFieldValue("link_type") as string | undefined;
              const kinds =
                linkType === "entity"
                  ? [{ value: "entity_id", label: "entity_id" }]
                  : linkType === "risk_contribution"
                    ? [{ value: "contribution_id", label: "contribution_id" }]
                    : [
                        { value: "event_id", label: "event_id" },
                        { value: "raw_event_id", label: "raw_event_id" },
                        { value: "attribution", label: "attribution" },
                        { value: "job_id", label: "job_id" },
                      ];
              return (
                <Form.Item name="ref_kind" label="引用类别" rules={[{ required: true }]}>
                  <Select placeholder="选择引用类别" options={kinds} disabled={!linkType} />
                </Form.Item>
              );
            }}
          </Form.Item>
          <Form.Item name="target_id" label="目标引用" rules={[{ required: true, max: 512 }]}>
            <Input placeholder="例如 ent:… / rc:… / evt:…" />
          </Form.Item>
          <div className="modal-actions">
            <Button onClick={() => setLinkOpen(false)}>取消</Button>
            <Button type="primary" htmlType="submit" loading={linkMutation.isPending}>添加关联</Button>
          </div>
        </Form>
      </Modal>

      <Modal title="冻结取证快照" open={snapshotOpen} footer={null} onCancel={() => setSnapshotOpen(false)}>
        <Form
          layout="vertical"
          onFinish={(values: { label: string }) => snapshotMutation.mutate(values)}
        >
          <Form.Item name="label" label="快照标签" rules={[{ required: true, max: 200 }]}>
            <Input autoFocus placeholder="例如：初检证据固定" maxLength={200} />
          </Form.Item>
          <div className="modal-actions">
            <Button onClick={() => setSnapshotOpen(false)}>取消</Button>
            <Button type="primary" htmlType="submit" loading={snapshotMutation.isPending}>冻结快照</Button>
          </div>
        </Form>
      </Modal>

      <Modal title="分派案件" open={assignOpen} footer={null} onCancel={() => setAssignOpen(false)}>
        {update.error instanceof APIError&&update.error.status===409&&<Alert showIcon type="warning" title={"当前案件版本 "+current.version} description={"负责人 "+(current.assignee||'未分派')+" / 状态 "+current.status} action={<Button onClick={()=>void detail.refetch()}>刷新当前版本</Button>}/>}
        <Form
          layout="vertical"
          onFinish={(values: { assignee: string; reason?: string }) =>
            update.mutate({ assignee: values.assignee, reason: values.reason })
          }
        >
          <Form.Item name="assignee" label="负责人身份" rules={[{ required: true, max: 128 }]}>
            <Input autoFocus placeholder="输入系统用户标识" />
          </Form.Item>
          <Form.Item name="reason" label="分派说明">
            <Input.TextArea rows={3} maxLength={2000} />
          </Form.Item>
          <div className="modal-actions">
            <Button onClick={() => setAssignOpen(false)}>取消</Button>
            <Button type="primary" htmlType="submit" loading={update.isPending}>确认分派</Button>
          </div>
        </Form>
      </Modal>

      <Modal title="记录判定并结案" open={closeOpen} footer={null} onCancel={() => setCloseOpen(false)}>
        {update.error instanceof APIError&&update.error.status===409&&<Alert showIcon type="warning" title={"当前案件版本 "+current.version} description={"判定 "+(current.verdict||'尚未记录')+" / 状态 "+current.status} action={<Button onClick={()=>void detail.refetch()}>刷新当前版本</Button>}/>}
        <Form
          layout="vertical"
          onFinish={(values: { verdict: CaseMutationInput["verdict"]; reason: string }) =>
            update.mutate({ status: "closed", verdict: values.verdict, reason: values.reason })
          }
        >
          <Form.Item name="verdict" label="判定结论" rules={[{ required: true }]}>
            <Select
              options={[
                { value: "true_positive", label: "确认威胁" },
                { value: "benign_positive", label: "有效但无害" },
                { value: "false_positive", label: "误报" },
                { value: "inconclusive", label: "暂不确定" },
              ]}
            />
          </Form.Item>
          <Form.Item name="reason" label="结论说明" rules={[{ required: true, max: 2000 }]}>
            <Input.TextArea autoFocus rows={5} maxLength={2000} showCount />
          </Form.Item>
          <div className="modal-actions">
            <Button onClick={() => setCloseOpen(false)}>取消</Button>
            <Button type="primary" htmlType="submit" loading={update.isPending}>结案</Button>
          </div>
        </Form>
      </Modal>
    </div>
  );
}

type MemberGroup = {
  subject: string;
  email: string;
  displayName: string;
  active: Member[];
  revoked: Member[];
};

export function Access() {
  const { token } = useAuth();
  const [tab,setTab]=useState("members");
  const { message } = AntApp.useApp();
  const queryClient = useQueryClient();
  const [addOpen, setAddOpen] = useState(false);
  const [selected, setSelected] = useState<string>();
  const query = useQuery({
    queryKey: ["members"],
    queryFn: ({ signal }) => api("/members", token, undefined, membersSchema, signal),
  });
  const createUser = useMutation({
    mutationFn: (values: {username: string;password: string;role: Member["role"]}) => api("/users", token, {method: "POST", body: JSON.stringify(values)}),
    onSuccess: () => {setAddOpen(false);void queryClient.invalidateQueries({queryKey:["members"]});},
  });
  const changeRole = useMutation({
    mutationFn: ({ subject, role, grant }: { subject: string; role: string; grant: boolean }) =>
      api<void>(
        `/members/${encodeURIComponent(subject)}/roles/${encodeURIComponent(role)}`,
        token,
        { method: grant ? "PUT" : "DELETE" },
      ),
    onSuccess: () => {
      void message.success("成员角色已更新");
      void queryClient.invalidateQueries({ queryKey: ["members"] });
    },
    onError: (error) => void message.error(errorMessage(error)),
  });
  const groups = useMemo(() => {
    const values = new Map<string, MemberGroup>();
    for (const member of query.data?.items ?? []) {
      const group = values.get(member.subject) ?? {
        subject: member.subject,
        email: member.email ?? "",
        displayName: member.display_name ?? "",
        active: [],
        revoked: [],
      };
      if (member.revoked_at) group.revoked.push(member);
      else group.active.push(member);
      values.set(member.subject, group);
    }
    return [...values.values()];
  }, [query.data]);
  const selectedGroup = groups.find((group) => group.subject === selected);

  const columns: TableProps<MemberGroup>["columns"] = [
    {
      title: "成员",
      key: "member",
      render: (_, value) => (
        <div className="primary-cell">
          <strong>{value.displayName || value.subject}</strong>
          <small>{value.email || value.subject}</small>
        </div>
      ),
    },
    {
      title: "当前角色",
      key: "roles",
      render: (_, value) =>
        value.active.length ? (
          <Space wrap>{value.active.map((member) => <RoleTag key={member.role} value={member.role} />)}</Space>
        ) : (
          <Tag>已撤销</Tag>
        ),
    },
    {
      title: "状态",
      key: "status",
      width: 100,
      render: (_, value) =>
        value.active.length ? <Tag className="signal-tag status-open">启用</Tag> : <Tag>停用</Tag>,
    },
    {
      title: "",
      key: "action",
      width: 90,
      render: (_, value) => (
        <Button type="link" onClick={() => setSelected(value.subject)}>编辑角色</Button>
      ),
    },
  ];

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="租户控制面"
        title="访问控制"
        description="在当前组织内分配用户角色与访问权限。"
        actions={
          <Button type="primary" icon={<UserPlus size={16} />} onClick={() => setAddOpen(true)}>
            添加成员
          </Button>
        }
      />
      <Tabs activeKey={tab} onChange={setTab} items={[{key:'members',label:'成员'},{key:'roles',label:'角色与权限'},{key:'service',label:'服务身份'},{key:'publishers',label:'独立发布授权'}]}/>
      {tab==='roles'&&<section className="panel"><header className="panel-head"><h2>角色权限边界</h2></header><Table rowKey="capability" pagination={false} dataSource={[
      {capability:'事件、异常、案件读取',viewer:'✓',analyst:'✓',admin:'✓',publisher:'—'},
      {capability:'案件处置与分析反馈',viewer:'—',analyst:'✓',admin:'✓',publisher:'—'},
      {capability:'敏感字段',viewer:'—',analyst:'✓',admin:'✓',publisher:'—'},
      {capability:'原文',viewer:'—',analyst:'—',admin:'✓',publisher:'—'},
      {capability:'来源、成员与运行管理',viewer:'—',analyst:'—',admin:'✓',publisher:'—'},
      {capability:'发布读取与推进',viewer:'—',analyst:'—',admin:'—',publisher:'✓'}
      ]} columns={[{title:'能力',dataIndex:'capability'},{title:'审计员',dataIndex:'viewer'},{title:'分析师',dataIndex:'analyst'},{title:'租户管理员',dataIndex:'admin'},{title:'独立发布者',dataIndex:'publisher'}]}/><Alert type="info" title="角色权限说明" description="实际权限以服务端会话为准；平台管理员与发布者是独立角色。"/></section>}
      {tab==='service'&&<section className="panel"><header className="panel-head"><h2>服务身份</h2></header><EmptyState title="独立服务身份目录尚未接入" description="来源凭据和 Agent 身份在各自管理页注册与轮换，管理身份与数据身份分别授权。"/><div className="wb-panel-body"><Link to="/sources">来源凭据管理 →</Link><br/><Link to="/agents">Agent 身份管理 →</Link></div></section>}
      {tab==='publishers'&&<><PublisherPanel/><Alert type="info" title="独立平台授权" description="发布授权不由租户管理员自动取得；若当前账号没有发布管理权限，请由获授权的平台操作者管理。"/></>}
      <section className="access-layout" hidden={tab!=='members'}>
        <article className="data-panel">
          <div className="data-summary"><span>{groups.length} 位成员</span></div>
          {query.isLoading ? (
            <LoadingBlock rows={7} />
          ) : query.error ? (
            <ErrorState message={errorMessage(query.error)} retry={() => void query.refetch()} />
          ) : groups.length ? (
            <Table rowKey="subject" columns={columns} dataSource={groups} pagination={false} />
          ) : (
            <EmptyState title="暂无成员" description="添加首位租户成员并授予最小必要角色。" />
          )}
        </article>
        <article className="panel role-guide">
          <header className="panel-head">
            <div>
              <span className="panel-index">RBAC</span>
              <h2>角色边界</h2>
            </div>
          </header>
          <dl>
            <div>
              <dt><KeyRound size={15} /> 安全分析师</dt>
              <dd>读取异常、创建案件、推进处置并记录判定。</dd>
            </div>
            <div>
              <dt><Users size={15} /> 租户管理员</dt>
              <dd>拥有调查权限，并管理成员、角色和运行状态。</dd>
            </div>
            <div>
              <dt><ShieldCheck size={15} /> 只读审计员</dt>
              <dd>仅查看授权异常与案件，不产生处置副作用。</dd>
            </div>
          </dl>
        </article>
      </section>

      <Modal title="添加系统用户" open={addOpen} footer={null} destroyOnHidden onCancel={() => setAddOpen(false)}>
        {createUser.error && <Alert type="error" title={createUser.error.message}/>}
        <Form preserve={false} layout="vertical" onFinish={(values: {username: string;password: string;role: Member["role"]}) => createUser.mutate(values)}>
          <Form.Item name="username" label="用户名" rules={[{required:true,min:3,max:64,pattern:/^[a-zA-Z0-9_.-]+$/}]}>
            <Input autoFocus autoComplete="off" placeholder="例如 wang.min"/>
          </Form.Item>
          <Form.Item name="password" label="初始密码" rules={[{required:true,min:12,max:256}]}>
            <Input.Password autoComplete="new-password"/>
          </Form.Item>
          <Form.Item name="role" label="初始角色" rules={[{ required: true }]}>
            <Select
              options={Object.entries(roleLabels).map(([value, label]) => ({ value, label }))}
            />
          </Form.Item>
          <div className="modal-actions">
            <Button onClick={() => setAddOpen(false)}>取消</Button>
            <Button type="primary" htmlType="submit" loading={createUser.isPending}>添加</Button>
          </div>
        </Form>
      </Modal>

      <Drawer
        title={selectedGroup?.displayName || selectedGroup?.subject || "成员角色"}
        open={Boolean(selectedGroup)}
        onClose={() => setSelected(undefined)}
        size={420}
      >
        {selectedGroup && (
          <div className="role-editor">
            <p>{selectedGroup.email || selectedGroup.subject}</p>
            {(Object.keys(roleLabels) as Member["role"][]).map((role) => {
              const active = selectedGroup.active.some((member) => member.role === role);
              const revoked = selectedGroup.revoked.some((member) => member.role === role);
              return (
                <div className="role-row" key={role}>
                  <div>
                    <RoleTag value={role} />
                    {revoked && !active && <small>该角色曾被撤销</small>}
                  </div>
                  {active ? (
                    <Popconfirm
                      title="撤销该角色？"
                      description="撤销会立即阻止后续请求，并写入审计记录。"
                      onConfirm={() => changeRole.mutate({ subject: selectedGroup.subject, role, grant: false })}
                    >
                      <Button danger loading={changeRole.isPending}>撤销</Button>
                    </Popconfirm>
                  ) : (
                    <Button
                      type="primary"
                      loading={changeRole.isPending}
                      onClick={() => changeRole.mutate({ subject: selectedGroup.subject, role, grant: true })}
                    >
                      授予
                    </Button>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </Drawer>
    </div>
  );
}

export function Operations() {
  const { token } = useAuth();
  const [tab,setTab]=useState("runtime");
  const query = useQuery({
    queryKey: ["operations"],
    queryFn: ({ signal }) =>
      api("/operations/status", token, undefined, operationsStatusSchema, signal),
    refetchInterval: 30_000,
  });

  if (query.isLoading) return <LoadingBlock rows={8} />;
  if (query.error) return <ErrorState message={errorMessage(query.error)} retry={() => void query.refetch()} />;
  if (!query.data) return <EmptyState title="暂无运行数据" description="运行状态接口未返回数据。" />;
  const status = query.data;
  const healthy = status.dependencies.filter((item) => item.status === "ok").length;
  const runtime = status.analysis.runtime;

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="平台遥测"
        title="运行与容量"
        description="检查控制面、检索依赖和分析新鲜度。"
        actions={
          <Tooltip title="刷新状态">
            <Button
              aria-label="刷新状态"
              icon={<RefreshCw size={16} />}
              loading={query.isFetching}
              onClick={() => void query.refetch()}
            />
          </Tooltip>
        }
      />
      <section className="metric-grid metric-grid-compact">
        <MetricPanel
          label="总体状态"
          value={status.status === "ok" ? "正常" : status.status === "degraded" ? "降级" : "不可用"}
          detail={`最近检查 ${new Date(status.checked_at).toLocaleTimeString("zh-CN", { hour12: false })}`}
          tone={status.status === "ok" ? "good" : status.status === "degraded" ? "warn" : "danger"}
          icon={<Activity size={19} />}
        />
        <MetricPanel
          label="健康依赖"
          value={`${healthy}/${status.dependencies.length}`}
          detail="PostgreSQL、Elasticsearch 与 API"
          icon={<Server size={19} />}
        />
        <MetricPanel
          label="分析新鲜度"
          value={formatDuration(status.analysis.age_seconds)}
          detail={status.analysis.latest_at ? "最近异常写入延迟" : "尚无分析结果"}
          tone={status.analysis.status === "ok" ? "good" : "warn"}
          icon={<Gauge size={19} />}
        />
        <MetricPanel
          label="服务运行时长"
          value={formatDuration(status.uptime_seconds)}
          detail="当前 API 实例"
          icon={<Database size={19} />}
        />
      </section>
      <Tabs activeKey={tab} onChange={setTab} items={[{key:"runtime",label:"服务与积压"},{key:"capacity",label:"容量与保留"},{key:"alerts",label:"告警与恢复"}]}/>
      <section className="operations-grid" hidden={tab!=="runtime"}>
        <article className="panel">
          <header className="panel-head">
            <div>
              <span className="panel-index">01</span>
              <h2>依赖状态</h2>
              <p>每项检查均为实时请求</p>
            </div>
          </header>
          <div className="dependency-list">
            {status.dependencies.map((item) => (
              <div key={item.name}>
                <span className={`health-indicator status-${item.status}`} />
                <div>
                  <strong>{item.name}</strong>
                  <small>{item.detail}</small>
                </div>
                <code>{item.latency_ms === undefined ? "n/a" : `${item.latency_ms} ms`}</code>
              </div>
            ))}
          </div>
        </article>
        <article className="panel">
          <header className="panel-head">
            <div>
              <span className="panel-index">02</span>
              <h2>链路覆盖</h2>
              <p>当前 API 可直接观测的范围</p>
            </div>
          </header>
          <div className="coverage-list">
            <div className="covered"><Check size={16} /><span>控制面连接</span><strong>已接入</strong></div>
            <div className="covered"><Check size={16} /><span>检索集群健康</span><strong>已接入</strong></div>
            <div className="covered"><Check size={16} /><span>分析结果新鲜度</span><strong>已接入</strong></div>
            <div className="pending"><AlertTriangle size={16} /><span>Kafka 消费组 Lag</span><strong>{status.kafka_configured ? "已配置" : "待 M5"}</strong></div>
          </div>
        </article>
      </section>
      <section className="panel runtime-panel" hidden={tab!=="runtime"}>
        <header className="panel-head">
          <div>
            <span className="panel-index">03</span>
            <h2>分析运行与 checkpoint</h2>
            <p>运行状态、事件时间水位和 Kafka 提交位置</p>
          </div>
          {runtime?.run && (
            <Tag className={`signal-tag status-${runtime.run.status === "running" ? "open" : "closed"}`}>
              {runtime.run.status === "running" ? "运行中" : runtime.run.status === "failed" ? "失败" : "已停止"}
            </Tag>
          )}
        </header>
        {runtime?.run ? (
          <>
            <div className="runtime-summary">
              <div><span>Run ID</span><code>{runtime.run.run_id}</code></div>
              <div><span>Registry</span><strong>{runtime.run.registry_version}</strong></div>
              <div><span>已处理事件</span><strong>{runtime.run.processed_events}</strong></div>
              <div><span>已发布结果</span><strong>{runtime.run.emitted_results}</strong></div>
              <div><span>最后心跳</span><TimeValue value={runtime.run.last_heartbeat_at} /></div>
            </div>
            {runtime.checkpoints.length ? (
              <Table
                rowKey={(item) => `${item.topic}-${item.partition}`}
                pagination={false}
                dataSource={runtime.checkpoints}
                columns={[
                  { title: "Topic", dataIndex: "topic" },
                  { title: "Partition", dataIndex: "partition", width: 100 },
                  { title: "Offset", dataIndex: "offset", width: 110 },
                  {
                    title: "Watermark",
                    dataIndex: "watermark",
                    width: 160,
                    render: (value: string | null | undefined) => value ? <TimeValue value={value} /> : "未知",
                  },
                  {
                    title: "最后更新",
                    dataIndex: "updated_at",
                    width: 130,
                    render: (value: string) => <TimeValue value={value} />,
                  },
                ]}
              />
            ) : (
              <EmptyState title="暂无 checkpoint" description="Worker 处理后会自动写入提交位置和事件时间水位。" />
            )}
          </>
        ) : (
          <EmptyState title="暂无分析运行" description="启动 Python analysis worker 后会显示运行元数据。" />
        )}
      </section>

      <section className="panel" hidden={tab!=="capacity"}><header className="panel-head"><h2>容量与保留</h2></header><EmptyState title="容量遥测尚未接入" description="数据盘使用率、Kafka 保留和样本独立预算尚无在线接口，不将原型中的阈值当作当前环境实测结果。"/></section>
      <section className="panel" hidden={tab!=="alerts"}><header className="panel-head"><h2>当前依赖提醒</h2></header>{status.dependencies.filter(x=>x.status!=='ok').length?status.dependencies.filter(x=>x.status!=='ok').map(x=><Alert key={x.name} showIcon type="warning" title={x.name+' / '+x.status} description={x.detail}/>):<EmptyState title="本次检查未发现依赖异常" description="在线检查范围之外的容量、缺口和通知送达不能从此结果推断。"/>}<Alert type="info" title="通知配置尚无管理接口" description="当前仅呈现接口返回的依赖检查结果。"/></section>
      <Link to="/backups">备份与恢复 →</Link>
    </div>
  );
}

export function Audit() {
  const { token } = useAuth();
  const [pages, setPages] = useState<AuditEvent[][]>([]);
  const [cursor, setCursor] = useState("");
  const query = useQuery({
    queryKey: ["audit", cursor],
    queryFn: ({ signal }) =>
      api(`/audit?limit=100${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`, token, undefined, auditPageSchema, signal),
  });
  const items = useMemo(() => [...pages.flat(), ...(query.data?.items ?? [])], [pages, query.data]);
  const nextCursor = query.data?.next_cursor ?? "";

  const columns: TableProps<AuditEvent>["columns"] = [
    {
      title: "时间",
      dataIndex: "occurred_at",
      key: "occurred_at",
      width: 190,
      render: (value: string) => new Date(value).toLocaleString("zh-CN", { hour12: false }),
    },
    {
      title: "动作",
      dataIndex: "action",
      key: "action",
      render: (value: string) => <ReadableValue value={value}/>,
    },
    {
      title: "资源",
      key: "resource",
      render: (_, value) =>
        value.resource_type ? (
          <span>
            <Tag>{value.resource_type}</Tag>
            <code className="muted-text">{value.resource_id}</code>
          </span>
        ) : (
          <span className="muted-text">—</span>
        ),
    },
    {
      title: "请求",
      dataIndex: "request_id",
      key: "request_id",
      render: (value: string) => <code className="muted-text">{value || "—"}</code>,
    },
  ];

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="审计追踪"
        title="审计日志"
        description="平台全量审计记录（append-only），按时间倒序。"
        actions={
          <Tooltip title="刷新">
            <Button
              aria-label="刷新"
              icon={<RefreshCw size={16} />}
              loading={query.isFetching}
              onClick={() => {
                setPages([]);
                setCursor("");
                void query.refetch();
              }}
            />
          </Tooltip>
        }
      />
      {query.isLoading && pages.length === 0 ? (
        <LoadingBlock rows={8} />
      ) : query.error ? (
        <ErrorState message={errorMessage(query.error)} retry={() => void query.refetch()} />
      ) : items.length === 0 ? (
        <EmptyState title="暂无审计事件" description="平台操作产生后会写入审计。" />
      ) : (
        <>
          <Table<AuditEvent>
            rowKey="id"
            columns={columns}
            dataSource={items}
            pagination={false}
            size="small"
            expandable={{
              rowExpandable: (record) => !!record.metadata && Object.keys(record.metadata).length > 0,
              expandedRowRender: (record) => (
                <pre className="json-block">{JSON.stringify(record.metadata, null, 2)}</pre>
              ),
            }}
          />
          {nextCursor && (
            <Button
              loading={query.isFetching}
              onClick={() => {
                setPages((previous) => [...previous, query.data?.items ?? []]);
                setCursor(nextCursor);
              }}
            >
              加载更多
            </Button>
          )}
        </>
      )}
    </div>
  );
}

const collectorStateMeta: Record<string, { label: string; className: string }> = {
  enrolled: { label: "已注册", className: "progress" },
  running: { label: "运行中", className: "open" },
  buffering: { label: "缓冲中", className: "progress" },
  backpressured: { label: "背压", className: "progress" },
  paused: { label: "已暂停", className: "muted" },
  error: { label: "错误", className: "danger" },
  disabled: { label: "已禁用", className: "danger" },
};

const sourceStateMeta: Record<string, { label: string; className: string }> = {
  active: { label: "启用", className: "open" },
  paused: { label: "暂停", className: "progress" },
  revoked: { label: "已撤销", className: "danger" },
};

function StateTag({ meta, value }: { meta: Record<string, { label: string; className: string }>; value: string }) {
  const current = meta[value] ?? { label: value || "未知", className: "muted" };
  return <Tag className={`signal-tag status-${current.className}`}>{current.label}</Tag>;
}

export function Sources() {
  const { token } = useAuth();
  const [selected, setSelected] = useState<string>();
  const sources = useQuery({
    queryKey: ["sources"],
    queryFn: ({ signal }) => api("/sources", token, undefined, sourcesSchema, signal),
    refetchInterval: 30_000,
  });
  const collectors = useQuery({
    queryKey: ["collectors"],
    queryFn: ({ signal }) => api("/collectors", token, undefined, collectorsSchema, signal),
    refetchInterval: 30_000,
  });

  const collectionBySource = useMemo(() => {
    const map = new Map<string, { collector: CollectorSummary; status: NonNullable<CollectorSummary["heartbeat"]>["sources"][number] }>();
    for (const collector of collectors.data?.items ?? []) {
      for (const status of collector.heartbeat?.sources ?? []) {
        map.set(status.source_id, { collector, status });
      }
    }
    return map;
  }, [collectors.data]);

  const collectorColumns: TableProps<CollectorSummary>["columns"] = [
    {
      title: "采集器",
      key: "collector",
      render: (_, value) => (
        <div className="primary-cell">
          <strong>{value.hostname || "主机名称暂不可用"}</strong>
          <small>{value.os}/{value.architecture} · 采集客户端 {value.installed_version || "版本未知"}</small>
        </div>
      ),
    },
    {
      title: "管理面状态",
      key: "management",
      children: [
        {
          title: "管理状态",
          dataIndex: "state",
          width: 110,
          render: (value: string, row) => (
            <Space size={4} wrap>
              <StateTag meta={collectorStateMeta} value={value} />
              {row.online ? (
                <Tag className="signal-tag status-open">在线</Tag>
              ) : (
                <Tag className="signal-tag status-muted">离线</Tag>
              )}
            </Space>
          ),
        },
        {
          title: "最后心跳",
          dataIndex: "last_heartbeat_at",
          width: 112,
          render: (value: string | null | undefined) =>
            value ? <TimeValue value={value} /> : <span className="muted">从未上报</span>,
        },
        {
          title: "配置版本",
          key: "config",
          width: 130,
          render: (_, value) => (
            <Tooltip title={value.config_version === value.desired_config_version ? "期望配置已生效" : "期望配置尚未生效"}>
              <span>
                生效 v{value.config_version} / 期望 v{value.desired_config_version}
                {value.config_version !== value.desired_config_version && (
                  <Tag className="signal-tag status-progress">待生效</Tag>
                )}
              </span>
            </Tooltip>
          ),
        },
      ],
    },
    {
      title: "采集面状态",
      key: "collection",
      children: [
        {
          title: "队列积压",
          key: "queue",
          width: 130,
          render: (_, value) => {
            const heartbeat = value.heartbeat;
            if (!heartbeat) return <span className="muted">无采集信号</span>;
            return (
              <Tooltip title={heartbeat.oldest_queued_at ? `最早排队 ${heartbeat.oldest_queued_at}` : "队列为空"}>
                <span>{heartbeat.queue_depth} 条</span>
              </Tooltip>
            );
          },
        },
        {
          title: "已发送事件",
          key: "sent",
          width: 110,
          render: (_, value) => {
            const heartbeat = value.heartbeat;
            if (!heartbeat) return <span className="muted">无采集信号</span>;
            const sent = heartbeat.sources.reduce((total, item) => total + item.events_sent, 0);
            return `${sent} 条`;
          },
        },
        {
          title: "采集健康",
          key: "health",
          width: 120,
          render: (_, value) => {
            const heartbeat = value.heartbeat;
            if (!heartbeat) return <span className="muted">无采集信号</span>;
            const broken = heartbeat.sources.filter((item) => item.state === "error" || item.last_error);
            if (heartbeat.state === "error" || broken.length > 0) {
              return (
                <Tooltip title={heartbeat.diagnostic || broken[0]?.last_error || "采集异常"}>
                  <Tag className="signal-tag status-danger">异常</Tag>
                </Tooltip>
              );
            }
            return <Tag className="signal-tag status-open">数据在流动</Tag>;
          },
        },
      ],
    },
  ];

  const sourceColumns: TableProps<SourceInstance>["columns"] = [
    {
      title: "来源",
      key: "source",
      render: (_, value) => (
        <div className="primary-cell">
          <strong>{value.vendor_product} / {value.vendor_dataset}</strong>
          <small>接入批次：{value.source_epoch}</small>
        </div>
      ),
    },
    {
      title: "管理面状态",
      key: "management",
      children: [
        {
          title: "管理状态",
          dataIndex: "state",
          width: 100,
          render: (value: string) => <StateTag meta={sourceStateMeta} value={value} />,
        },
        {
          title: "限速",
          dataIndex: "rate_limit",
          width: 100,
          render: (value: number) => `${value} 条/秒`,
        },
        {
          title: "Release",
          dataIndex: "release_id",
          width: 150,
          render: (value: string) => value || <span className="muted">未绑定</span>,
        },
      ],
    },
    {
      title: "采集面状态",
      key: "collection",
      children: [
        {
          title: "数据流动",
          key: "flow",
          width: 150,
          render: (_, value) => {
            const signal = collectionBySource.get(value.id);
            if (!signal) return <span className="muted">无采集信号</span>;
            const { collector, status } = signal;
            return (
              <Tooltip title={`由 ${collector.hostname || collector.id} 上报 · 读取 ${status.events_read} / 发送 ${status.events_sent} / 丢弃 ${status.events_drop}`}>
                <Space size={4} wrap>
                  {collector.online && status.state === "running" ? (
                    <Tag className="signal-tag status-open">流动中</Tag>
                  ) : (
                    <Tag className="signal-tag status-progress">{status.state === "paused" ? "采集暂停" : collector.online ? status.state : "采集器离线"}</Tag>
                  )}
                  <span>{status.events_sent} 条</span>
                </Space>
              </Tooltip>
            );
          },
        },
        {
          title: "最近错误",
          key: "error",
          render: (_, value) => {
            const signal = collectionBySource.get(value.id);
            if (!signal?.status.last_error) return <span className="muted">无</span>;
            return (
              <Tooltip title={signal.status.last_error}>
                <Tag className="signal-tag status-danger">有错误</Tag>
              </Tooltip>
            );
          },
        },
      ],
    },
    {
      title: "最近更新",
      dataIndex: "updated_at",
      width: 112,
      render: (value: string) => <TimeValue value={value} />,
    },
    {
      title: "",
      key: "action",
      width: 80,
      render: (_, value) => (
        <Button type="link" onClick={() => setSelected(value.id)}>详情</Button>
      ),
    },
  ];

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="接入治理"
        title="来源与采集器"
        description="管理面（启用/禁用、心跳、配置版本）与采集面（数据流动、队列积压）分列呈现，互不混用。"
      />
      <section className="panel">
        <header className="panel-head">
          <div>
            <span className="panel-index">01</span>
            <h2>采集器</h2>
            <p>Management Agent 注册、心跳与配置下发状态（管理面），与队列/流量（采集面）分列</p>
          </div>
          {(collectors.isFetching || sources.isFetching) && (
            <span className="fetching"><RefreshCw size={13} /> 更新中</span>
          )}
        </header>
        {collectors.isLoading ? (
          <LoadingBlock rows={4} />
        ) : collectors.error ? (
          <ErrorState message={errorMessage(collectors.error)} retry={() => void collectors.refetch()} />
        ) : collectors.data?.items.length ? (
          <Table
            rowKey="id"
            columns={collectorColumns}
            dataSource={collectors.data.items}
            pagination={false}
            scroll={{ x: 980 }}
          />
        ) : (
          <EmptyState title="暂无已注册采集器" description="Management Agent 完成 enroll 后会出现在这里。" />
        )}
      </section>
      <section className="panel">
        <header className="panel-head">
          <div>
            <span className="panel-index">02</span>
            <h2>来源</h2>
            <p>来源生命周期为管理面状态；数据是否流动来自采集器心跳上报，单独成列</p>
          </div>
        </header>
        {sources.isLoading ? (
          <LoadingBlock rows={4} />
        ) : sources.error ? (
          <ErrorState message={errorMessage(sources.error)} retry={() => void sources.refetch()} />
        ) : sources.data?.items.length ? (
          <Table
            rowKey="id"
            columns={sourceColumns}
            dataSource={sources.data.items}
            pagination={false}
            scroll={{ x: 980 }}
          />
        ) : (
          <EmptyState title="暂无来源" description="通过来源注册 API 建立来源后会出现在这里。" />
        )}
      </section>
      <SourceDetailDrawer
        source={sources.data?.items.find((item) => item.id === selected)}
        signal={selected ? collectionBySource.get(selected) : undefined}
        onClose={() => setSelected(undefined)}
      />
    </div>
  );
}

function SourceDetailDrawer({
  source,
  signal,
  onClose,
}: {
  source?: SourceInstance;
  signal?: { collector: CollectorSummary; status: NonNullable<CollectorSummary["heartbeat"]>["sources"][number] };
  onClose: () => void;
}) {
  const { token } = useAuth();
  const from = useMemo(() => new Date(Date.now() - 24 * 3600_000).toISOString(), []);
  const contextId = source?.source_context_id ?? "";
  const dip = useQuery({
    queryKey: ["source", "dip", contextId],
    enabled: Boolean(source && contextId),
    queryFn: ({ signal: abort }) =>
      runQuery(
        token,
        `search raw WHERE ueba.provenance.source_context_id=${contextId} | stats count`,
        from,
        abort,
      ),
  });

  return (
    <Drawer
      title={source ? `${source.vendor_product} / ${source.vendor_dataset}` : "来源详情"}
      open={Boolean(source)}
      onClose={onClose}
      size={480}
    >
      {source && (
        <div className="page-stack">
          <Descriptions
            column={1}
            size="small"
            items={[
              { key: "id", label: "来源 ID", children: <ReadableValue value={source.id} label="查看完整来源标识"/> },
              { key: "vendor", label: "厂商", children: source.vendor_name },
              { key: "epoch", label: "Source epoch", children: source.source_epoch },
              { key: "state", label: "管理状态", children: <StateTag meta={sourceStateMeta} value={source.state} /> },
              { key: "rate", label: "限速", children: `${source.rate_limit} 条/秒` },
              { key: "release", label: "关联发布包", children: source.release_id || "未绑定" },
              { key: "ctx", label: "Source context", children: contextId ? <code>{contextId}</code> : "未登记" },
              { key: "created", label: "创建时间", children: <TimeValue value={source.created_at} /> },
              { key: "updated", label: "最近更新", children: <TimeValue value={source.updated_at} /> },
            ]}
          />
          <article className="panel">
            <header className="panel-head">
              <div>
                <span className="panel-index">DIP</span>
                <h2>解析/规范化链路信号</h2>
                <p>采集面上报与原始事件入库均为实时查询，不做推断</p>
              </div>
            </header>
            <div className="dependency-list">
              <div>
                <span className={`health-indicator status-${signal ? (signal.collector.online && signal.status.state === "running" ? "ok" : "degraded") : "unknown"}`} />
                <div>
                  <strong>采集面</strong>
                  <small>
                    {signal
                      ? `${signal.collector.hostname || signal.collector.id} · 读取 ${signal.status.events_read} / 发送 ${signal.status.events_sent} / 丢弃 ${signal.status.events_drop}`
                      : "采集器未上报该来源"}
                  </small>
                </div>
                <code>{signal?.status.last_error ? "有错误" : "—"}</code>
              </div>
              <div>
                <span className={`health-indicator status-${dip.data && statsTotal(dip.data) > 0 ? "ok" : dip.data ? "degraded" : "unknown"}`} />
                <div>
                  <strong>原始事件入库（最近 24 小时）</strong>
                  <small>
                    {dip.isLoading
                      ? "查询中"
                      : dip.error
                        ? errorMessage(dip.error)
                        : contextId
                          ? dip.data
                            ? `原始数据域内 ${statsTotal(dip.data)} 条该上下文事件`
                            : "尚无查询结果"
                          : "无 source context，无法关联原始事件"}
                  </small>
                </div>
                <code>{dip.data ? `${statsTotal(dip.data)} 条` : "—"}</code>
              </div>
            </div>
          </article>
        </div>
      )}
    </Drawer>
  );
}

const qualityRanges = [
  { value: "24h", label: "最近 24 小时", hours: 24 },
  { value: "7d", label: "最近 7 天", hours: 24 * 7 },
  { value: "30d", label: "最近 30 天", hours: 24 * 30 },
];

const releaseStateMeta: Record<string, { label: string; className: string }> = {
  draft: { label: "草稿", className: "progress" },
  validated: { label: "已验证", className: "progress" },
  staged: { label: "已预置", className: "progress" },
  active: { label: "已激活", className: "open" },
};

const jobStateMeta: Record<string, { label: string; className: string }> = {
  queued: { label: "排队中", className: "progress" },
  running: { label: "运行中", className: "progress" },
  succeeded: { label: "已完成", className: "open" },
  completed: { label: "已完成", className: "open" },
  failed: { label: "失败", className: "danger" },
  cancelled: { label: "已取消", className: "muted" },
  expired: { label: "已过期", className: "muted" },
};

export function Quality() {
  const { token, can } = useAuth();
  const [range, setRange] = useState("24h");
  const hours = qualityRanges.find((item) => item.value === range)?.hours ?? 24;
  const from = useMemo(() => new Date(Date.now() - hours * 3600_000).toISOString(), [hours]);

  const catalog = useQuery({
    queryKey: ["catalog"],
    queryFn: ({ signal }) => api("/catalog", token, undefined, catalogSchema, signal),
  });
  const domains = useMemo(
    () => (catalog.data?.datasets ?? []).filter((dataset) => dataset.kind === "uim-domain"),
    [catalog.data],
  );
  const sources = useQuery({
    queryKey: ["sources"],
    enabled: can("source:manage"),
    queryFn: ({ signal }) => api("/sources", token, undefined, sourcesSchema, signal),
  });
  const sourceByContext = useMemo(() => {
    const map = new Map<string, SourceInstance>();
    for (const source of sources.data?.items ?? []) {
      if (source.source_context_id) map.set(source.source_context_id, source);
    }
    return map;
  }, [sources.data]);

  const qualityQueries = useQueries({
    queries: domains.map((domain) => ({
      queryKey: ["quality", "status", domain.name, range],
      queryFn: ({ signal }: { signal: AbortSignal }) =>
        runQuery(token, `search ${domain.name} | stats count BY ueba.quality.status`, from, signal),
    })),
  });
  const reasonQueries = useQueries({
    queries: domains.map((domain) => ({
      queryKey: ["quality", "reasons", domain.name, range],
      queryFn: ({ signal }: { signal: AbortSignal }) =>
        runQuery(
          token,
          `search ${domain.name} WHERE ueba.quality.status=partial | stats count BY ueba.quality.reasons`,
          from,
          signal,
        ),
    })),
  });
  const dip = useQuery({
    queryKey: ["quality", "dip", range],
    queryFn: ({ signal }) =>
      runQuery(token, "search raw | stats count BY ueba.provenance.source_context_id", from, signal),
  });
  const quarantineAgg = useQuery({
    queryKey: ["quarantine", "agg", range],
    retry: false,
    queryFn: ({ signal }) =>
      runQuery(token, "search quarantine | stats count BY quarantine.stage,quarantine.reason", from, signal),
  });
  const quarantineEvents = useQuery({
    queryKey: ["quarantine", "events", range],
    retry: false,
    queryFn: ({ signal }) => runQuery(token, "search quarantine | head 50", from, signal),
  });

  const qualityRows = domains.map((domain, index) => {
    const result = qualityQueries[index]?.data;
    const buckets = result ? statsBuckets(result) : [];
    const qualified = buckets.find((bucket) => bucket.key["ueba.quality.status"] === "qualified")?.count ?? 0;
    const partial = buckets.find((bucket) => bucket.key["ueba.quality.status"] === "partial")?.count ?? 0;
    return { domain: domain.name, qualified, partial, total: qualified + partial };
  });
  const reasonRows = useMemo(() => {
    const totals = new Map<string, number>();
    for (const query of reasonQueries) {
      if (!query.data) continue;
      for (const bucket of statsBuckets(query.data)) {
        const reason = bucket.key["ueba.quality.reasons"] ?? "unknown";
        totals.set(reason, (totals.get(reason) ?? 0) + bucket.count);
      }
    }
    return [...totals.entries()]
      .map(([reason, count]) => ({ reason, count }))
      .sort((a, b) => b.count - a.count);
  }, [reasonQueries]);
  const dipRows = dip.data ? statsBuckets(dip.data) : [];
  const quarantineRows = quarantineAgg.data ? statsBuckets(quarantineAgg.data) : [];
  const quarantineUnavailable =
    quarantineAgg.error instanceof APIError && quarantineAgg.error.status === 503;
  const qualityError = qualityQueries.find((query) => query.error)?.error;
  const reasonsError = reasonQueries.find((query) => query.error)?.error;
  const loadingQuality = catalog.isLoading || qualityQueries.some((query) => query.isLoading);

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="数据治理"
        title="数据质量与隔离"
        description="查看各类事件的完整程度、字段缺失原因和未能正常处理的数据。"
        actions={
          <Select
            value={range}
            onChange={setRange}
            options={qualityRanges.map((item) => ({ value: item.value, label: item.label }))}
          />
        }
      />

      <section className="panel">
        <header className="panel-head">
          <div>
            <span className="panel-index">01</span>
            <h2>事件质量分布</h2>
            <p>按数据类型统计完整事件与字段不完整事件</p>
          </div>
          {(qualityQueries.some((query) => query.isFetching) || dip.isFetching) && (
            <span className="fetching"><RefreshCw size={13} /> 更新中</span>
          )}
        </header>
        {catalog.error ? (
          <ErrorState message={errorMessage(catalog.error)} retry={() => void catalog.refetch()} />
        ) : loadingQuality ? (
          <LoadingBlock rows={6} />
        ) : qualityError ? (
          <ErrorState
            message={errorMessage(qualityError)}
            retry={() => void qualityQueries.forEach((query) => void query.refetch())}
          />
        ) : (
          <Table
            rowKey="domain"
            pagination={false}
            dataSource={qualityRows}
            columns={[
              { title: "数据类型", dataIndex: "domain" },
              {
                title: "字段完整",
                dataIndex: "qualified",
                width: 120,
                render: (value: number) => value.toLocaleString("zh-CN"),
              },
              {
                title: "字段不完整",
                dataIndex: "partial",
                width: 120,
                render: (value: number) => value.toLocaleString("zh-CN"),
              },
              {
                title: "合计",
                dataIndex: "total",
                width: 120,
                render: (value: number) => value.toLocaleString("zh-CN"),
              },
              {
                title: "质量",
                key: "tone",
                width: 110,
                render: (_, row) =>
                  row.total === 0 ? (
                    <Tag className="signal-tag status-muted">无数据</Tag>
                  ) : row.partial > 0 ? (
                    <Tag className="signal-tag status-progress">字段不完整</Tag>
                  ) : (
                    <Tag className="signal-tag status-open">全部合格</Tag>
                  ),
              },
            ]}
          />
        )}
      </section>

      <section className="dashboard-grid">
        <article className="panel">
          <header className="panel-head">
            <div>
              <span className="panel-index">02</span>
              <h2>质量原因</h2>
              <p>汇总事件字段不完整的原因</p>
            </div>
          </header>
          {reasonQueries.some((query) => query.isLoading) ? (
            <LoadingBlock rows={4} />
          ) : reasonsError ? (
            <ErrorState
              message={errorMessage(reasonsError)}
              retry={() => void reasonQueries.forEach((query) => void query.refetch())}
            />
          ) : reasonRows.length ? (
            <Table
              rowKey="reason"
              pagination={false}
              dataSource={reasonRows}
              columns={[
                { title: "原因码", dataIndex: "reason", render: (value: string) => <ReadableValue value={value}/> },
                {
                  title: "事件数",
                  dataIndex: "count",
                  width: 110,
                  render: (value: number) => value.toLocaleString("zh-CN"),
                },
              ]}
            />
          ) : (
            <EmptyState title="无 partial 事件" description="当前时间范围内没有质量降级事件。" />
          )}
        </article>

        <article className="panel">
          <header className="panel-head">
            <div>
              <span className="panel-index">03</span>
              <h2>原始数据接入情况</h2>
              <p>按数据来源统计原始事件，辅助确认是否有数据进入平台</p>
            </div>
          </header>
          {dip.isLoading ? (
            <LoadingBlock rows={4} />
          ) : dip.error ? (
            <ErrorState message={errorMessage(dip.error)} retry={() => void dip.refetch()} />
          ) : dipRows.length ? (
            <Table
              rowKey={(row) => row.key["ueba.provenance.source_context_id"] ?? "unknown"}
              pagination={false}
              dataSource={dipRows}
              columns={[
                {
                  title: "Source context",
                  key: "ctx",
                  render: (_, row) => {
                    const contextId = row.key["ueba.provenance.source_context_id"] ?? "";
                    const bound = sourceByContext.get(contextId);
                    return (
                      <div className="primary-cell">
                        <code>{contextId}</code>
                        {bound && <small>{bound.vendor_product} / {bound.vendor_dataset}</small>}
                      </div>
                    );
                  },
                },
                {
                  title: "原始事件数",
                  dataIndex: "count",
                  width: 120,
                  render: (value: number) => value.toLocaleString("zh-CN"),
                },
                {
                  title: "链路",
                  key: "health",
                  width: 100,
                  render: (_, row) =>
                    row.count > 0 ? (
                      <Tag className="signal-tag status-open">有数据流入</Tag>
                    ) : (
                      <Tag className="signal-tag status-progress">无数据</Tag>
                    ),
                },
              ]}
            />
          ) : (
            <EmptyState
              title="范围内无原始事件"
              description="原始数据域在当前时间范围没有事件，或已过保留期被清理。"
            />
          )}
        </article>
      </section>

      <section className="panel">
        <header className="panel-head">
          <div>
            <span className="panel-index">04</span>
            <h2>待处理数据</h2>
            <p>查看未能正常处理的数据及失败原因；此处不展示原始内容</p>
          </div>
          {(quarantineAgg.isFetching || quarantineEvents.isFetching) && (
            <span className="fetching"><RefreshCw size={13} /> 更新中</span>
          )}
        </header>
        {quarantineUnavailable ? (
          <EmptyState
            title="隔离索引暂不可用"
            description="隔离数据域的索引尚未创建或事件存储不可用；出现隔离事件后会自动可查。"
          />
        ) : quarantineAgg.error ? (
          <ErrorState message={errorMessage(quarantineAgg.error)} retry={() => void quarantineAgg.refetch()} />
        ) : (
          <>
            {quarantineRows.length ? (
              <Table
                rowKey={(row) => `${row.key["quarantine.stage"]}-${row.key["quarantine.reason"]}`}
                pagination={false}
                dataSource={quarantineRows}
                columns={[
                  { title: "阶段", key: "stage", width: 110, render: (_, row) => <Tag className="signal-tag">{row.key["quarantine.stage"]}</Tag> },
                  { title: "原因", key: "reason", render: (_, row) => <code>{row.key["quarantine.reason"]}</code> },
                  { title: "事件数", dataIndex: "count", width: 110, render: (value: number) => value.toLocaleString("zh-CN") },
                ]}
              />
            ) : (
              <EmptyState title="范围内无隔离事件" description="当前时间范围内没有进入隔离区的事件。" />
            )}
            {quarantineEvents.data && quarantineEvents.data.items.length > 0 && (
              <Table
                className="compact-table"
                rowKey={(_, index) => String(index)}
                pagination={false}
                dataSource={quarantineEvents.data.items}
                columns={[
                  {
                    title: "时间",
                    key: "ts",
                    width: 150,
                    render: (_, item) => {
                      const value = item["@timestamp"];
                      return typeof value === "string" ? <TimeValue value={value} /> : "—";
                    },
                  },
                  {
                    title: "阶段",
                    key: "stage",
                    width: 100,
                    render: (_, item) => String((item["quarantine"] as Record<string, unknown> | undefined)?.["stage"] ?? "—"),
                  },
                  {
                    title: "原因",
                    key: "reason",
                    render: (_, item) => (
                      <code>{String((item["quarantine"] as Record<string, unknown> | undefined)?.["reason"] ?? "—")}</code>
                    ),
                  },
                ]}
              />
            )}
          </>
        )}
      </section>
    </div>
  );
}

export function Releases() {
  const { token, can } = useAuth();
  const { message } = AntApp.useApp();
  const queryClient = useQueryClient();
  const [selected, setSelected] = useState<string>();
  const releases = useQuery({
    queryKey: ["releases"],
    queryFn: ({ signal }) => api("/releases", token, undefined, releasesSchema, signal),
  });
  const current = releases.data?.items.find((item) => item.id === selected);
  const audit = useQuery({
    queryKey: ["release", selected, "audit"],
    enabled: Boolean(selected),
    queryFn: ({ signal }) =>
      api(`/releases/${encodeURIComponent(selected ?? "")}/audit`, token, undefined, releaseAuditSchema, signal),
  });
  const transition = useMutation({
    mutationFn: ({ id, action }: { id: string; action: "validate" | "stage" | "activate" }) =>
      api<Release>(`/releases/${encodeURIComponent(id)}/${action}`, token, { method: "POST" }, releaseSchema),
    onSuccess: (value) => {
      void message.success(`Release 已推进到 ${value.state}`);
      void queryClient.invalidateQueries({ queryKey: ["releases"] });
      void queryClient.invalidateQueries({ queryKey: ["release", value.id, "audit"] });
    },
    onError: (error) => void message.error(errorMessage(error)),
  });
  const manageable = can("release:manage");
  const nextAction: Record<string, { action: "validate" | "stage" | "activate"; label: string }> = {
    draft: { action: "validate", label: "校验" },
    validated: { action: "stage", label: "预置" },
    staged: { action: "activate", label: "激活" },
  };

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="发布治理"
        title="版本发布"
        description="Release bundle 的登记状态与 draft→validated→staged→active 推进链，全部操作写审计。"
      />
      <section className="data-panel">
        <div className="data-summary">
          <span>{releases.data?.items.length ?? 0} 个 release</span>
          {releases.isFetching && <span className="fetching"><RefreshCw size={13} /> 更新中</span>}
        </div>
        {releases.isLoading ? (
          <LoadingBlock rows={5} />
        ) : releases.error ? (
          <ErrorState message={errorMessage(releases.error)} retry={() => void releases.refetch()} />
        ) : releases.data?.items.length ? (
          <Table
            rowKey="id"
            pagination={false}
            dataSource={releases.data.items}
            columns={[
              {
                title: "Release",
                key: "release",
                render: (_, value) => (
                  <div className="primary-cell">
                    <strong><ReadableValue value={value.id}/></strong>
                    <small>版本 {value.version} · sha256 {value.sha256.slice(0, 16)}…</small>
                  </div>
                ),
              },
              {
                title: "状态",
                dataIndex: "state",
                width: 110,
                render: (value: string) => <StateTag meta={releaseStateMeta} value={value} />,
              },
              {
                title: "资产",
                key: "assets",
                width: 90,
                render: (_, value) => `${value.manifest.assets.length} 项`,
              },
              {
                title: "创建时间",
                dataIndex: "created_at",
                width: 120,
                render: (value: string) => <TimeValue value={value} />,
              },
              {
                title: "激活时间",
                dataIndex: "activated_at",
                width: 120,
                render: (value: string | null | undefined) =>
                  value ? <TimeValue value={value} /> : <span className="muted">未激活</span>,
              },
              {
                title: "",
                key: "action",
                width: 170,
                render: (_, value) => {
                  const next = nextAction[value.state];
                  return (
                    <Space size={4}>
                      <Button type="link" onClick={() => setSelected(value.id)}>详情</Button>
                      {manageable && next && (
                        <Button
                          type="link"
                          loading={transition.isPending}
                          onClick={() => transition.mutate({ id: value.id, action: next.action })}
                        >
                          {next.label}
                        </Button>
                      )}
                    </Space>
                  );
                },
              },
            ]}
          />
        ) : (
          <EmptyState title="暂无 release" description="Release 登记并推进后会出现在这里。" />
        )}
      </section>

      <Drawer
        title={current ? `Release ${current.id}` : "Release 详情"}
        open={Boolean(current)}
        onClose={() => setSelected(undefined)}
        size={560}
      >
        {current && (
          <div className="page-stack">
            <Descriptions
              column={1}
              size="small"
              items={[
                { key: "id", label: "发布包标识", children: <code>{current.id}</code> },
                { key: "version", label: "版本", children: current.version },
                { key: "state", label: "状态", children: <StateTag meta={releaseStateMeta} value={current.state} /> },
                { key: "sha", label: "Canonical sha256", children: <code>{current.sha256}</code> },
                { key: "created", label: "创建时间", children: <TimeValue value={current.created_at} /> },
                {
                  key: "activated",
                  label: "激活时间",
                  children: current.activated_at ? <TimeValue value={current.activated_at} /> : "未激活",
                },
              ]}
            />
            <article className="panel">
              <header className="panel-head">
                <div>
                  <span className="panel-index">资产</span>
                  <h2>Manifest 资产清单</h2>
                </div>
              </header>
              <Table
                rowKey="asset_id"
                pagination={false}
                dataSource={current.manifest.assets}
                columns={[
                  { title: "类型", dataIndex: "kind", width: 90, render: (value: string) => <Tag className="signal-tag">{value}</Tag> },
                  {
                    title: "资产",
                    key: "asset",
                    render: (_, asset) => (
                      <div className="primary-cell">
                        <strong>{asset.asset_id}</strong>
                        <small>{asset.path} · v{asset.version}</small>
                      </div>
                    ),
                  },
                  {
                    title: "依赖",
                    dataIndex: "dependencies",
                    width: 90,
                    render: (value: string[]) => (value.length ? `${value.length} 项` : "无"),
                  },
                ]}
              />
            </article>
            <article className="panel">
              <header className="panel-head">
                <div>
                  <span className="panel-index">审计</span>
                  <h2>发布审计链</h2>
                </div>
              </header>
              {audit.isLoading ? (
                <LoadingBlock rows={3} />
              ) : audit.error ? (
                <ErrorState message={errorMessage(audit.error)} retry={() => void audit.refetch()} />
              ) : audit.data?.items.length ? (
                <Table
                  rowKey="id"
                  pagination={false}
                  dataSource={audit.data.items}
                  columns={[
                    { title: "动作", dataIndex: "action", render: (value: string) => <ReadableValue value={value}/> },
                    { title: "操作者", dataIndex: "actor_subject", width: 150, render: (value: string) => value || "—" },
                    { title: "时间", dataIndex: "occurred_at", width: 120, render: (value: string) => <TimeValue value={value} /> },
                  ]}
                />
              ) : (
                <EmptyState title="暂无审计记录" description="状态推进与登记操作会写入审计。" />
              )}
            </article>
          </div>
        )}
      </Drawer>
    </div>
  );
}

export function Jobs() {
  const { token } = useAuth();
  const exportsQuery = useQuery({
    queryKey: ["exports"],
    retry: false,
    queryFn: ({ signal }) => api("/exports", token, undefined, exportsSchema, signal),
  });

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="处理治理"
        title="任务中心"
        description="处理作业、回放/回填与导出任务的状态视图；不展示任何模拟数据。"
      />
      <section className="panel">
        <header className="panel-head">
          <div>
            <span className="panel-index">01</span>
            <h2>处理与回放任务</h2>
            <p>回放、回填、基线训练等 processing job 的列表与控制</p>
          </div>
        </header>
        <EmptyState
          title="任务 API 待后端"
          description="/api/v1/jobs 在 API 合同中仍为 planned 状态，后端尚未提供任务列表与回放创建端点；本区块待后端落地后接入真实数据。"
        />
      </section>
      <section className="panel">
        <header className="panel-head">
          <div>
            <span className="panel-index">02</span>
            <h2>导出任务</h2>
            <p>Q03 异步导出的任务记录（创建入口在事件查询页，属 W02 范围）</p>
          </div>
          {exportsQuery.isFetching && <span className="fetching"><RefreshCw size={13} /> 更新中</span>}
        </header>
        {exportsQuery.isLoading ? (
          <LoadingBlock rows={4} />
        ) : exportsQuery.error ? (
          exportsQuery.error instanceof APIError && exportsQuery.error.status === 503 ? (
            <EmptyState
              title="导出功能未启用"
              description="后端按设计 fail-closed：未配置导出存储时返回 503，待 D4 部署启用后可查。"
            />
          ) : (
            <ErrorState message={errorMessage(exportsQuery.error)} retry={() => void exportsQuery.refetch()} />
          )
        ) : exportsQuery.data?.items.length ? (
          <Table
            rowKey="id"
            pagination={false}
            dataSource={exportsQuery.data.items}
            columns={[
              {
                title: "导出",
                key: "export",
                render: (_, value: ExportJob) => (
                  <div className="primary-cell">
                    <strong><Link to={"/exports/"+encodeURIComponent(value.id)}>{value.dataset} · {value.format.toUpperCase()}</Link></strong>
                    <small>{value.query}</small>
                  </div>
                ),
              },
              {
                title: "状态",
                dataIndex: "state",
                width: 100,
                render: (value: string) => <StateTag meta={jobStateMeta} value={value} />,
              },
              {
                title: "行数",
                dataIndex: "row_count",
                width: 100,
                render: (value: number | null | undefined) =>
                  value === null || value === undefined ? "—" : value.toLocaleString("zh-CN"),
              },
              {
                title: "创建时间",
                dataIndex: "created_at",
                width: 120,
                render: (value: string) => <TimeValue value={value} />,
              },
              {
                title: "完成时间",
                dataIndex: "completed_at",
                width: 120,
                render: (value: string | null | undefined) =>
                  value ? <TimeValue value={value} /> : <span className="muted">—</span>,
              },
              {
                title: "错误",
                dataIndex: "error",
                render: (value: string) =>
                  value ? (
                    <Tooltip title={value}><Tag className="signal-tag status-danger">有错误</Tag></Tooltip>
                  ) : (
                    <span className="muted">无</span>
                  ),
              },
            ]}
          />
        ) : (
          <EmptyState title="暂无导出任务" description="创建导出后任务会出现在这里。" />
        )}
      </section>
    </div>
  );
}

type EventRow = Record<string, unknown>;

function eventField(item: EventRow, ...path: string[]): string {
  let value: unknown = item;
  for (const key of path) {
    if (!value || typeof value !== "object" || Array.isArray(value)) return "";
    value = (value as Record<string, unknown>)[key];
  }
  return typeof value === "string" || typeof value === "number" ? String(value) : "";
}

const splPresets: Record<string, { label: string; query: string }[]> = {
  authentication: [
    { label: "失败登录", query: "search authentication WHERE event.outcome=failure | head 100" },
    { label: "按结果统计", query: "search authentication | stats count BY event.outcome" },
    { label: "失败最多账号", query: "search authentication WHERE event.outcome=failure | top 10 user.name" },
    { label: "每小时认证量", query: "search authentication | timechart span=1h count" },
  ],
  network: [
    { label: "按协议统计", query: "search network | stats count BY network.transport" },
    { label: "流量最大目的", query: "search network | top 10 destination.ip" },
  ],
  dns: [
    { label: "热点域名", query: "search dns | top 10 dns.question.name" },
  ],
  raw: [
    { label: "按来源统计", query: "search raw | stats count BY ueba.provenance.source_context_id" },
    { label: "最近原始事件", query: "search raw | head 50" },
  ],
  quarantine: [
    { label: "按原因统计", query: "search quarantine | stats count BY quarantine.reason" },
    { label: "最近隔离记录", query: "search quarantine | head 50" },
  ],
};

function defaultQuery(dataset: string): string {
  return `search ${dataset} | head 100`;
}

function presetsFor(dataset: string, kind: string): { label: string; query: string }[] {
  if (splPresets[dataset]) return splPresets[dataset];
  if (kind === "uim-domain") {
    return [
      { label: "按动作统计", query: `search ${dataset} | stats count BY event.action` },
      { label: "质量分布", query: `search ${dataset} | stats count BY ueba.quality.status` },
      { label: "每小时事件量", query: `search ${dataset} | timechart span=1h count` },
    ];
  }
  return [];
}

function TimechartTable({ result }: { result: QueryResult }) {
  const agg = result.aggregations?.["timechart"] as
    | { buckets?: Array<Record<string, unknown>> }
    | undefined;
  const buckets = agg?.buckets ?? [];
  if (!buckets.length) {
    return <EmptyState title="时间桶为空" description="当前时间范围内没有可聚合的事件。" />;
  }
  const rows = buckets.map((bucket, index) => ({
    key: index,
    time: String(bucket["key_as_string"] ?? bucket["key"] ?? ""),
    count: (bucket["count"] as { value?: number } | undefined)?.value ?? null,
  }));
  return (
    <Table
      rowKey="key"
      pagination={false}
      dataSource={rows}
      columns={[
        { title: "时间桶", dataIndex: "time", render: (value: string) => <TimeValue value={value} /> },
        {
          title: "事件数",
          dataIndex: "count",
          width: 120,
          render: (value: number | null) => (value === null ? "—" : value.toLocaleString("zh-CN")),
        },
      ]}
    />
  );
}

function AggregationResult({ result }: { result: QueryResult }) {
  if (result.mode === "timechart") return <TimechartTable result={result} />;
  const buckets = statsBuckets(result);
  if (buckets.length) {
    const byFields = Object.keys(buckets[0]!.key);
    return (
      <Table
        rowKey={(row) => byFields.map((field) => row.key[field]).join("|")}
        pagination={false}
        dataSource={buckets}
        columns={[
          ...byFields.map((field) => ({
            title: field,
            key: field,
            render: (_: unknown, row: { key: Record<string, string> }) => <code>{row.key[field]}</code>,
          })),
          {
            title: "事件数",
            dataIndex: "count",
            width: 110,
            render: (value: number) => value.toLocaleString("zh-CN"),
          },
        ]}
      />
    );
  }
  // top/rare modes: terms aggregation under the mode name.
  const terms = result.aggregations?.[result.mode] as
    | { buckets?: Array<{ key?: unknown; doc_count?: number }> }
    | undefined;
  if (terms?.buckets?.length) {
    return (
      <Table
        rowKey={(row) => String(row.key)}
        pagination={false}
        dataSource={terms.buckets}
        columns={[
          { title: "值", dataIndex: "key", render: (value: unknown) => <code>{String(value)}</code> },
          {
            title: "事件数",
            dataIndex: "doc_count",
            width: 110,
            render: (value: number | undefined) => (value ?? 0).toLocaleString("zh-CN"),
          },
        ]}
      />
    );
  }
  const metrics = Object.entries(result.aggregations ?? {}).filter(
    (entry): entry is [string, { value?: number | null; value_as_string?: string }] =>
      Boolean(entry[1]) && typeof entry[1] === "object" && "value" in (entry[1] as object),
  );
  if (!metrics.length) {
    return <EmptyState title="聚合为空" description="当前时间范围内没有事件命中。" />;
  }
  return (
    <div className="metric-grid-compact">
      {metrics.map(([name, agg]) => (
        <article className="metric-panel tone-neutral" key={name}>
          <span>{name}</span>
          <strong>
            {agg.value === null || agg.value === undefined
              ? "—"
              : (agg.value_as_string ?? agg.value.toLocaleString("zh-CN"))}
          </strong>
        </article>
      ))}
    </div>
  );
}

function RawReference({ rawEventId }: { rawEventId: string }) {
  const { token, can } = useAuth();
  const [requested, setRequested] = useState(false);
  const rawLookup = useQuery({
    queryKey: ["events", "raw-ref", rawEventId],
    enabled: requested,
    retry: false,
    queryFn: ({ signal }) =>
      runQuery(
        token,
        `search raw WHERE event.id=${rawEventId} | head 5`,
        new Date(Date.now() - 30 * 24 * 3600_000).toISOString(),
        signal,
      ),
  });

  return (
    <div className="page-stack">
      <Descriptions size="small" column={1} items={[{ key: "raw", label: "raw_event_id", children: <code>{rawEventId}</code> }]} />
      {!can("raw:read") && (
        <Alert
          type="info"
          showIcon
          icon={<Lock size={15} />}
          title="原文内容受 raw:read 权限控制"
          description="当前角色不持有 raw:read，原始报文不可查看。"
        />
      )}
      <Alert
        type="warning"
        showIcon
        title="原文在线查看属后端缺口"
        description="查询 API 对任何角色都不返回 event.original（服务端统一剥离）；目前唯一携带原文的通道是 Q03 异步导出（需 raw:read 且后端已配置导出存储，248 尚未启用）。此处仅呈现引用与元数据。"
      />
      {!requested ? (
        <Button size="small" onClick={() => setRequested(true)} icon={<Search size={14} />}>
          查询 raw 域引用记录（元数据）
        </Button>
      ) : rawLookup.isLoading ? (
        <LoadingBlock rows={2} />
      ) : rawLookup.error ? (
        <ErrorState message={errorMessage(rawLookup.error)} retry={() => void rawLookup.refetch()} />
      ) : rawLookup.data && rawLookup.data.items.length > 0 ? (
        <Table
          className="compact-table"
          rowKey={(_, index) => String(index)}
          pagination={false}
          dataSource={rawLookup.data.items}
          columns={[
            {
              title: "时间",
              key: "ts",
              width: 140,
              render: (_, item) => <TimeValue value={eventField(item, "@timestamp")} />,
            },
            {
              title: "event.id",
              key: "id",
              render: (_, item) => <code>{eventField(item, "event", "id")}</code>,
            },
            {
              title: "source context",
              key: "ctx",
              render: (_, item) => (
                <code>{eventField(item, "ueba", "provenance", "source_context_id") || "—"}</code>
              ),
            },
          ]}
        />
      ) : (
        <EmptyState
          title="raw 域未查到该引用"
          description="原始事件可能已过保留期被清理，或从未写入 raw 域。"
        />
      )}
    </div>
  );
}

function EventDetailDrawer({ item, onClose }: { item: EventRow; onClose: () => void }) {
  const provenance = eventProvenance(item);
  const chain = [
    { label: "原始事件", value: provenance.rawEventId, hint: "raw 域 event.id" },
    { label: "DIP 解析", value: provenance.releaseId, hint: "解析规则 release" },
    { label: "UIM 事件", value: provenance.eventId, hint: `${provenance.domain || "uim"} · ${provenance.generation || "—"}` },
  ];
  return (
    <Drawer title="事件详情与血缘" width={520} open onClose={onClose}>
      <div className="page-stack">
        <Descriptions
          size="small"
          column={1}
          bordered
          items={[
            { key: "ts", label: "时间", children: <TimeValue value={eventField(item, "@timestamp")} /> },
            { key: "id", label: "event.id", children: <code>{provenance.eventId}</code> },
            { key: "action", label: "动作", children: eventField(item, "event", "action") || "—" },
            { key: "outcome", label: "结果", children: eventField(item, "event", "outcome") || "—" },
            { key: "user", label: "用户", children: eventField(item, "user", "name") || eventField(item, "user", "id") || "—" },
            { key: "src", label: "来源地址", children: eventField(item, "source", "ip") || "—" },
            { key: "quality", label: "质量", children: provenance.qualityStatus || "—" },
            { key: "schema", label: "UIM schema", children: provenance.schemaVersion || "—" },
          ]}
        />
        <section className="panel">
          <header className="panel-head">
            <div>
              <span className="panel-index"><GitBranch size={14} /></span>
              <h2>血缘（provenance）</h2>
              <p>由事件自带引用字段构成，不做推断</p>
            </div>
          </header>
          <div className="activity-timeline">
            {chain.map((node) => (
              <article key={node.label}>
                <div className="activity-dot"><CircleDot size={14} /></div>
                <div>
                  <strong>{node.label}</strong>
                  <p>{node.hint}</p>
                  {node.value ? <code>{node.value}</code> : <span className="muted">无引用字段</span>}
                </div>
              </article>
            ))}
          </div>
        </section>
        <section className="panel">
          <header className="panel-head">
            <div>
              <span className="panel-index"><Lock size={14} /></span>
              <h2>原始引用</h2>
              <p>raw_event_id 引用与 raw 域元数据；原文内容按权限与后端能力呈现</p>
            </div>
          </header>
          {provenance.rawEventId ? (
            <RawReference rawEventId={provenance.rawEventId} />
          ) : (
            <EmptyState
              title="无 raw 引用"
              description="该事件没有 ueba.provenance.raw_event_id 字段，无法回链原始事件。"
            />
          )}
        </section>
      </div>
    </Drawer>
  );
}

function DatasetStatus({ datasets }: { datasets: CatalogDataset[] }) {
  const { token } = useAuth();
  const from = useMemo(() => new Date(Date.now() - 30 * 24 * 3600_000).toISOString(), []);
  const freshness = useQueries({
    queries: datasets.map((dataset) => ({
      queryKey: ["events", "freshness", dataset.name],
      retry: false,
      queryFn: ({ signal }: { signal: AbortSignal }) =>
        runQuery(token, `search ${dataset.name} | stats count,max(@timestamp) as latest`, from, signal),
    })),
  });

  const rows = datasets.map((dataset, index) => {
    const query = freshness[index];
    if (query?.error) {
      const unavailable = query.error instanceof APIError && query.error.status === 503;
      return {
        dataset,
        state: unavailable ? ("unavailable" as const) : ("error" as const),
        count: null as number | null,
        latest: null as string | null,
        message: unavailable ? "索引暂不可用（尚未创建或事件存储不可用）" : errorMessage(query.error),
      };
    }
    if (!query?.data) {
      return { dataset, state: "loading" as const, count: null, latest: null, message: "" };
    }
    const count = statsMetric(query.data, "count")?.value ?? 0;
    const latestMetric = statsMetric(query.data, "latest");
    const latest =
      latestMetric?.valueAsString ??
      (latestMetric?.value != null ? new Date(latestMetric.value).toISOString() : null);
    if (count === 0 || !latest) {
      return { dataset, state: "empty" as const, count, latest, message: "30 天窗口内无事件——可能从未写入，或已过保留期；后端尚无权威归档/过期信号" };
    }
    const lagSeconds = Math.max(0, (Date.now() - new Date(latest).getTime()) / 1000);
    return {
      dataset,
      state: lagSeconds > 24 * 3600 ? ("lagging" as const) : ("fresh" as const),
      count,
      latest,
      message: "",
    };
  });

  const stateMeta: Record<string, { label: string; className: string }> = {
    fresh: { label: "新鲜", className: "open" },
    lagging: { label: "索引滞后", className: "progress" },
    empty: { label: "窗口内无数据", className: "muted" },
    unavailable: { label: "索引不可用", className: "muted" },
    error: { label: "查询失败", className: "danger" },
  };

  return (
    <Table
      rowKey={(row) => row.dataset.name}
      pagination={false}
      loading={freshness.some((query) => query.isLoading)}
      dataSource={rows}
      columns={[
        {
          title: "数据集",
          key: "name",
          render: (_, row) => (
            <div className="primary-cell">
              <strong>{row.dataset.name}</strong>
              <small>{row.dataset.kind} · 代次 {row.dataset.active_generation}</small>
            </div>
          ),
        },
        {
          title: "30 天事件数",
          key: "count",
          width: 130,
          render: (_, row) => (row.count === null ? "—" : row.count.toLocaleString("zh-CN")),
        },
        {
          title: "最新事件",
          key: "latest",
          width: 130,
          render: (_, row) => (row.latest ? <TimeValue value={row.latest} /> : <span className="muted">—</span>),
        },
        {
          title: "状态",
          key: "state",
          width: 130,
          render: (_, row) =>
            row.state === "loading" ? (
              <span className="muted">查询中</span>
            ) : (
              <StateTag meta={stateMeta} value={row.state} />
            ),
        },
        {
          title: "说明",
          key: "message",
          render: (_, row) => (row.message ? <span className="muted">{row.message}</span> : <span className="muted">—</span>),
        },
      ]}
    />
  );
}

export function Events() {
  const { token } = useAuth();
  const { message } = AntApp.useApp();
  const queryClient = useQueryClient();
  const [params]=useSearchParams();
  const initialDataset=params.get("dataset")??"authentication";
  const [dataset, setDataset] = useState(initialDataset);
  const [spl, setSpl] = useState(defaultQuery(initialDataset));
  const [range, setRange] = useState("24h");
  const [result, setResult] = useState<QueryResult | null>(null);
  const [items, setItems] = useState<EventRow[]>([]);
  const [cursor, setCursor] = useState("");
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string>();
  const [selected, setSelected] = useState<EventRow>();
  const [exportFormat, setExportFormat] = useState<"ndjson" | "csv">("ndjson");
  const [exportOpen, setExportOpen] = useState(false);
  const exportMutation = useMutation({
    mutationFn: (values: { query: string; format: string; from: string }) =>
      api("/exports", token, { method: "POST", body: JSON.stringify(values) }),
    onSuccess: () => {
      void message.success("导出任务已创建，可在“任务中心”页查看进度");
      void queryClient.invalidateQueries({ queryKey: ["exports"] });
      setExportOpen(false);
    },
    onError: (reason) => void message.error(errorMessage(reason)),
  });

  const catalog = useQuery({
    queryKey: ["catalog"],
    queryFn: ({ signal }) => api("/catalog", token, undefined, catalogSchema, signal),
  });
  const datasets = useMemo(() => catalog.data?.datasets ?? [], [catalog.data]);
  const datasetDecl = datasets.find((item) => item.name === dataset);
  const presets = presetsFor(dataset, datasetDecl?.kind ?? "");

  async function execute(nextCursor?: string) {
    setRunning(true);
    setError(undefined);
    const hours = qualityRanges.find((item) => item.value === range)?.hours ?? 24;
    const from = new Date(Date.now() - hours * 3600_000).toISOString();
    try {
      const value = await runQuery(token, spl, from, undefined, { limit: 100, cursor: nextCursor });
      setResult(value);
      setCursor(value.next_cursor);
      setItems((previous) => (nextCursor ? [...previous, ...value.items] : value.items));
    } catch (reason) {
      if (!nextCursor) {
        setResult(null);
        setItems([]);
        setCursor("");
      }
      setError(errorMessage(reason));
    } finally {
      setRunning(false);
    }
  }

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="调查"
        title="事件检索"
        description="SPL 子集查询：数据集、字段与时间范围由服务端按 Catalog 白名单与租户/代次强制约束。"
        actions={
          <Select
            value={range}
            onChange={setRange}
            options={qualityRanges.map((item) => ({ value: item.value, label: item.label }))}
          />
        }
      />

      <section className="panel">
        <header className="panel-head">
          <div>
            <span className="panel-index">01</span>
            <h2>SPL 查询</h2>
            <p>默认 24 小时、单页 100 条；31 日为 API 请求上限而非数据保留承诺</p>
          </div>
          {running && <span className="fetching"><RefreshCw size={13} /> 查询中</span>}
        </header>
        <Space direction="vertical" size="middle" style={{ width: "100%" }}>
          <Space wrap size="small">
            <Select
              value={dataset}
              style={{ minWidth: 220 }}
              loading={catalog.isLoading}
              onChange={(value) => {
                setDataset(value);
                setSpl(defaultQuery(value));
                setResult(null);
                setItems([]);
                setCursor("");
                setError(undefined);
              }}
              options={datasets.map((item) => ({
                value: item.name,
                label: `${item.name}（${item.kind}）`,
              }))}
            />
            {presets.map((preset) => (
              <Button key={preset.label} size="small" onClick={() => setSpl(preset.query)}>
                {preset.label}
              </Button>
            ))}
          </Space>
          <Input.TextArea
            value={spl}
            rows={3}
            spellCheck={false}
            onChange={(event) => setSpl(event.target.value)}
            placeholder="search <dataset> WHERE … | stats count BY …"
          />
          <Space size="small">
            <Button
              type="primary"
              icon={<Search size={16} />}
              loading={running}
              disabled={!spl.trim()}
              onClick={() => void execute()}
            >
              运行查询
            </Button>
            <Button
              icon={<ArrowUpRight size={15} />}
              disabled={!spl.trim()}
              onClick={() => setExportOpen(true)}
            >
              导出当前查询
            </Button>
            <QueryBookmarks dataset={dataset} spl={spl} range={range} onLoad={value=>{setDataset(value.dataset);setSpl(value.spl);setRange(value.range);setResult(null);setItems([]);setCursor("");setError(undefined);}}/>
            <span className="muted">
              禁止子查询、join、eval、正则、通配符与任意 ES DSL；字段必须在 Catalog 白名单内
            </span>
          </Space>
        </Space>
      </section>

      <section className="panel">
        <header className="panel-head">
          <div>
            <span className="panel-index">02</span>
            <h2>查询结果</h2>
            <p>
              {result
                ? result.mode === "events"
                  ? `共 ${result.total.toLocaleString("zh-CN")} 条命中，已加载 ${items.length} 条`
                  : `聚合模式：${result.mode}`
                : "运行查询后在此呈现真实结果"}
            </p>
          </div>
        </header>
        {error ? (
          <ErrorState message={error} retry={() => void execute()} />
        ) : !result ? (
          <EmptyState title="尚未执行查询" description="选择数据集与常用示例，或直接编写 SPL 后运行。" />
        ) : result.mode === "events" ? (
          <>
            {items.length ? (
              <Table
                rowKey={(_, index) => String(index)}
                pagination={false}
                dataSource={items}
                columns={[
                  {
                    title: "时间",
                    key: "ts",
                    width: 130,
                    render: (_, item) => <TimeValue value={eventField(item, "@timestamp")} />,
                  },
                  {
                    title: "事件",
                    key: "event",
                    render: (_, item) => (
                      <div className="primary-cell">
                        <strong>{eventField(item, "event", "action") || eventField(item, "event", "kind") || "—"}</strong>
                        <small><code>{eventField(item, "event", "id") || eventField(item, "id")}</code></small>
                      </div>
                    ),
                  },
                  {
                    title: "结果",
                    key: "outcome",
                    width: 90,
                    render: (_, item) => eventField(item, "event", "outcome") || "—",
                  },
                  {
                    title: "用户",
                    key: "user",
                    width: 140,
                    render: (_, item) => eventField(item, "user", "name") || eventField(item, "user", "id") || "—",
                  },
                  {
                    title: "来源地址",
                    key: "src",
                    width: 120,
                    render: (_, item) => eventField(item, "source", "ip") || "—",
                  },
                  {
                    title: "质量",
                    key: "quality",
                    width: 90,
                    render: (_, item) => {
                      const status = eventField(item, "ueba", "quality", "status");
                      return status ? (
                        <Tag className={`signal-tag ${status === "partial" ? "status-progress" : "status-open"}`}>{status}</Tag>
                      ) : (
                        "—"
                      );
                    },
                  },
                  {
                    title: "操作",
                    key: "ops",
                    width: 90,
                    render: (_, item) => (
                      <Button size="small" type="link" icon={<FileSearch size={14} />} onClick={() => setSelected(item)}>
                        详情
                      </Button>
                    ),
                  },
                ]}
              />
            ) : (
              <EmptyState title="范围内无事件" description="当前时间范围没有命中事件；不要把空结果解释为无事件——超出保留期的范围不会静默截短。" />
            )}
            {cursor && (
              <div style={{ marginTop: 12 }}>
                <Button size="small" loading={running} onClick={() => void execute(cursor)}>
                  加载更多（游标分页）
                </Button>
              </div>
            )}
          </>
        ) : (
          <AggregationResult result={result} />
        )}
      </section>

      <section className="panel">
        <header className="panel-head">
          <div>
            <span className="panel-index">03</span>
            <h2>数据集状态（索引新鲜度 / 可查询边界）</h2>
            <p>
              查看最近事件时间与查询范围。当前未提供每数据集的实际保留起点和归档状态，不能据此判断证据是否已经过期。
            </p>
          </div>
        </header>
        {catalog.error ? (
          <ErrorState message={errorMessage(catalog.error)} retry={() => void catalog.refetch()} />
        ) : catalog.isLoading ? (
          <LoadingBlock rows={6} />
        ) : (
          <DatasetStatus datasets={datasets} />
        )}
      </section>

      {selected && <EventDetailDrawer item={selected} onClose={() => setSelected(undefined)} />}
      <Modal
        title="创建异步导出"
        open={exportOpen}
        onCancel={() => setExportOpen(false)}
        okText="创建导出"
        confirmLoading={exportMutation.isPending}
        onOk={() => {
          const hours = qualityRanges.find((item) => item.value === range)?.hours ?? 24;
          exportMutation.mutate({
            query: spl,
            format: exportFormat,
            from: new Date(Date.now() - hours * 3600_000).toISOString(),
          });
        }}
      >
        <div className="page-stack">
          <Alert
            type="info"
            showIcon
            title="导出为异步任务，下载时重新授权"
            description="敏感字段按创建时权限快照脱敏；原文需 raw:read。未配置导出存储时后端 fail-closed 拒绝（503），不会生成任何任务。"
          />
          <Form layout="vertical">
            <Form.Item label="格式">
              <Select
                value={exportFormat}
                onChange={setExportFormat}
                options={[
                  { value: "ndjson", label: "NDJSON（事件或聚合）" },
                  { value: "csv", label: "CSV（仅事件模式）" },
                ]}
              />
            </Form.Item>
            <Form.Item label="查询快照（创建后冻结）">
              <Input.TextArea value={spl} rows={3} readOnly />
            </Form.Item>
          </Form>
        </div>
      </Modal>
    </div>
  );
}

const entityWindowOptions = [
  { value: 24, label: "最近 24 小时" },
  { value: 24 * 7, label: "最近 7 天" },
  { value: 24 * 31, label: "最近 31 天（API 请求上限）" },
];

export function Entities() {
  const { token } = useAuth();
  const [hours, setHours] = useState(24 * 7);
  const [directorySearch, setDirectorySearch] = useState("");
  const [directoryType, setDirectoryType] = useState("");
  const [window, setWindow] = useState(() => anomalyWindow(24 * 7));
  const directory = useInfiniteQuery({
    queryKey: ["entities", "directory", directorySearch, directoryType],
    initialPageParam: "",
    queryFn: ({ pageParam, signal }) =>
      listEntities(token, { query: directorySearch || undefined, type: directoryType || undefined, cursor: pageParam, limit: 50 }, signal),
    getNextPageParam: (last) => last.next_cursor || undefined,
  });
  const directoryItems = directory.data?.pages.flatMap((page) => page.items) ?? [];
  const anomalies = useInfiniteQuery({
    queryKey: ["entities", "anomaly-entities", window.from, window.to],
    initialPageParam: "",
    queryFn: ({ pageParam, signal }) =>
      api(
        `/anomalies${queryString({ limit: 100, cursor: pageParam, from: window.from, to: window.to })}`,
        token,
        undefined,
        anomalyPageSchema,
        signal,
      ),
    getNextPageParam: (last) => last.next_cursor || undefined,
  });
  const items = anomalies.data?.pages.flatMap((page) => page.items) ?? [];
  const total = anomalies.data?.pages[0]?.total ?? items.length;
  const entities = useMemo(() => groupEntities(items), [items]);

  const directoryColumns: TableProps<EntitySummary>["columns"] = [
    {
      title: "实体",
      dataIndex: "entity_id",
      render: (value: string, row) => (
        <div className="primary-cell">
          <Link to={`/entities/${encodeURIComponent(value)}`}>
            <strong>{row.canonical_key}</strong>
          </Link>
          <small>{value.slice(0, 24)}…</small>
        </div>
      ),
    },
    {
      title: "类型",
      dataIndex: "entity_type",
      width: 90,
      render: (value: string) => <Tag className="signal-tag">{value}</Tag>,
    },
    {
      title: "标识强度",
      dataIndex: "identity_strength",
      width: 96,
      render: (value: string) => (
        <Tag className={`signal-tag ${value === "strong" ? "status-ok" : "status-warn"}`}>{value}</Tag>
      ),
    },
    { title: "身份空间", dataIndex: "authority", width: 150 },
    {
      title: "生效自",
      dataIndex: "valid_from",
      width: 130,
      render: (value: string) => <TimeValue value={value} />,
    },
    {
      title: "",
      key: "action",
      width: 52,
      render: (_, row) => (
        <Tooltip title="实体详情">
          <Link aria-label={`实体 ${row.entity_id}`} className="icon-link" to={`/entities/${encodeURIComponent(row.entity_id)}`}>
            <ChevronRight size={18} />
          </Link>
        </Tooltip>
      ),
    },
  ];

  const columns: TableProps<EntityAggregate>["columns"] = [
    {
      title: "实体",
      key: "entity",
      render: (_, value) => (
        <div className="entity-cell">
          <Link to={`/entities/${encodeURIComponent(value.id)}`}>
            <strong><ReadableValue value={value.id}/></strong>
          </Link>
          <small>{value.type}</small>
        </div>
      ),
    },
    {
      title: "窗口内异常",
      dataIndex: "anomalyCount",
      width: 110,
      render: (value: number) => `${value} 条`,
    },
    {
      title: "开放中",
      dataIndex: "openCount",
      width: 90,
      render: (value: number) => (value > 0 ? <Tag className="signal-tag status-danger">{value}</Tag> : "0"),
    },
    {
      title: "最高严重度",
      dataIndex: "highestSeverity",
      width: 110,
      render: (value: string) => <SeverityTag value={value} />,
    },
    {
      title: "最近异常时间",
      dataIndex: "latest",
      width: 130,
      render: (value: string) => <TimeValue value={value} />,
    },
    {
      title: "",
      key: "action",
      width: 52,
      render: (_, value) => (
        <Tooltip title="实体详情">
          <Link aria-label={`实体 ${value.id}`} className="icon-link" to={`/entities/${encodeURIComponent(value.id)}`}>
            <ChevronRight size={18} />
          </Link>
        </Tooltip>
      ),
    },
  ];

  return (
    <div className="page-stack">
      <PageHeader
        eyebrow="实体画像"
        title="账户与主机"
        description="查看账户与主机的信息、关联关系、行为基线和风险来源。"
      />

      <section className="panel">
        <header className="panel-head">
          <div>
            <span className="panel-index">02</span>
            <h2>实体目录</h2>
            <p>按账户名、主机名或实体类型查找</p>
          </div>
          <Space>
            <Input.Search
              allowClear
              placeholder="搜索账户名或主机名，例如 win-139"
              style={{ width: 260 }}
              onSearch={(value) => setDirectorySearch(value.trim())}
            />
            <Select
              value={directoryType}
              onChange={setDirectoryType}
              options={[
                { value: "", label: "全部类型" },
                { value: "account", label: "账户" },
                { value: "device", label: "主机" },
              ]}
              style={{ width: 130 }}
            />
            {directory.isFetching && <span className="fetching"><RefreshCw size={13} /> 更新中</span>}
          </Space>
        </header>
        {directory.isLoading ? (
          <LoadingBlock rows={6} />
        ) : directory.error ? (
          <ErrorState message={errorMessage(directory.error)} retry={() => void directory.refetch()} />
        ) : directoryItems.length === 0 ? (
          <EmptyState title="目录为空" description="没有匹配的实体；系统处理事件后会自动登记相关账户和主机。" />
        ) : (
          <>
            <div className="data-summary">
              <span>已加载 {directoryItems.length} 个实体</span>
            </div>
            <Table
              rowKey="entity_id"
              columns={directoryColumns}
              dataSource={directoryItems}
              pagination={false}
              scroll={{ x: 860 }}
            />
            {directory.hasNextPage && (
              <div className="load-more">
                <Button loading={directory.isFetchingNextPage} onClick={() => void directory.fetchNextPage()}>
                  加载更多实体
                </Button>
              </div>
            )}
          </>
        )}
      </section>
      <section className="panel">
        <header className="panel-head">
          <div>
            <span className="panel-index">03</span>
            <h2>窗口内出现异常的实体</h2>
            <p>来自所选窗口内异常（finding）中的实体引用聚合，不是租户全量实体目录</p>
          </div>
          <Space>
            <Select
              value={hours}
              onChange={(value: number) => {
                setHours(value);
                setWindow(anomalyWindow(value));
              }}
              options={entityWindowOptions}
              style={{ width: 220 }}
            />
            {anomalies.isFetching && <span className="fetching"><RefreshCw size={13} /> 更新中</span>}
          </Space>
        </header>
        {anomalies.isLoading ? (
          <LoadingBlock rows={6} />
        ) : anomalies.error ? (
          <ErrorState message={errorMessage(anomalies.error)} retry={() => void anomalies.refetch()} />
        ) : entities.length === 0 ? (
          <EmptyState
            title="窗口内没有带异常的实体"
            description="扩大时间窗口，或改用上方实体目录/实体 ID 直达；无异常的实体出现在实体目录中。"
          />
        ) : (
          <>
            <div className="data-summary">
              <span>窗口内共 {total} 条异常，聚合出 {entities.length} 个实体</span>
            </div>
            <Table
              rowKey="id"
              columns={columns}
              dataSource={entities}
              pagination={false}
              scroll={{ x: 760 }}
            />
            {anomalies.hasNextPage && (
              <div className="load-more">
                <Button loading={anomalies.isFetchingNextPage} onClick={() => void anomalies.fetchNextPage()}>
                  加载更多异常以聚合更多实体
                </Button>
              </div>
            )}
          </>
        )}
      </section>
    </div>
  );
}

export function EntityDetail() {
  const [attributionSelected,setAttributionSelected] = useState<EntityAttribution>();

  const { id = "" } = useParams();
  const entityId = decodeURIComponent(id);
  const { token } = useAuth();
  const [params] = useSearchParams();
  const [tab, setTab] = useState(params.get("tab") ?? "profile");
  const [hours, setHours] = useState(24 * 7);
  const [window, setWindow] = useState(() => anomalyWindow(24 * 7));
  const anomalies = useInfiniteQuery({
    queryKey: ["entities", entityId, "anomalies", window.from, window.to],
    enabled: Boolean(entityId),
    initialPageParam: "",
    queryFn: ({ pageParam, signal }) =>
      api(
        `/anomalies${queryString({ limit: 50, cursor: pageParam, entity: entityId, from: window.from, to: window.to })}`,
        token,
        undefined,
        anomalyPageSchema,
        signal,
      ),
    getNextPageParam: (last) => last.next_cursor || undefined,
  });
  const items = anomalies.data?.pages.flatMap((page) => page.items) ?? [];
  const total = anomalies.data?.pages[0]?.total ?? items.length;
  const entityType = items.find((item) => item.entity.type)?.entity.type ?? "";

  // The entity registry API keys on the strong entity reference
  // (ent:<sha256>); a free-form account/hostname lookup cannot resolve the
  // registry panels, which then say so instead of guessing.
  const isEntityRef = /^ent:[a-f0-9]{64}$/.test(entityId);
  const profile = useQuery({
    queryKey: ["entities", entityId, "profile"],
    enabled: isEntityRef,
    retry: false,
    queryFn: ({ signal }) => getEntity(token, entityId, signal),
  });
  const attributions = useInfiniteQuery({
    queryKey: ["entities", entityId, "attributions"],
    enabled: isEntityRef,
    initialPageParam: "",
    queryFn: ({ pageParam, signal }) => listEntityAttributions(token, entityId, { cursor: pageParam, limit: 50 }, signal),
    getNextPageParam: (last) => last.next_cursor || undefined,
  });
  const relations = useQuery({
    queryKey: ["entities", entityId, "relations"],
    enabled: isEntityRef,
    queryFn: ({ signal }) => listEntityRelations(token, entityId, true, signal),
  });
  const features = useQuery({
    queryKey: ["entities", entityId, "features"],
    enabled: isEntityRef,
    queryFn: ({ signal }) => listEntityFeatures(token, entityId, 20, signal),
  });
  const baseline = useQuery({
    queryKey: ["entities", entityId, "baseline"],
    enabled: isEntityRef,
    queryFn: ({ signal }) => getEntityBaseline(token, entityId, signal),
  });
  const risk = useQuery({
    queryKey: ["entities", entityId, "risk"],
    enabled: isEntityRef,
    queryFn: ({ signal }) => getEntityRisk(token, entityId, signal),
  });
  const attributionItems = attributions.data?.pages.flatMap((page) => page.items) ?? [];
  const profileData = profile.data;
  const effectiveEntityType = profileData?.entity_type ?? entityType;

  const attributionColumns: TableProps<EntityAttribution>["columns"] = [
    {
      title: "事件时间",
      dataIndex: "event_time",
      width: 150,
      render: (value: string) => <TimeValue value={value} />,
    },
    { title: "角色", dataIndex: "role", width: 150, render: (value: string) => <ReadableValue value={value}/> },
    {
      title: "状态",
      dataIndex: "state",
      width: 96,
      render: (value: string) => (
        <Tag className={`signal-tag ${value === "resolved" ? "status-ok" : "status-warn"}`}>{readable(value)}</Tag>
      ),
    },
    { title: "规则版本", dataIndex: "rule_version", width: 100 },
    {
      title: "证据/原因",
      dataIndex: "reason",
      ellipsis: true,
      render: (_: string, row) => <div className="primary-cell"><span>{row.state === "resolved" ? "事件身份信息已关联到此实体" : row.state === "ambiguous" ? "存在多个匹配对象，无法唯一确定身份" : "尚未找到匹配的实体"}</span><Button type="link" size="small" onClick={()=>setAttributionSelected(row)}>查看关联依据</Button></div>,
    },
  ];

  const relationColumns: TableProps<EntityRelation>["columns"] = [
    { title: "关系", dataIndex: "relation_type", width: 140, render: (value: string) => <ReadableValue value={value}/> },
    {
      title: "对端实体",
      key: "peer",
      render: (_, row) => {
        const peer = row.from_entity_id === entityId ? row.to_entity_id : row.from_entity_id;
        return (
          <Link to={`/entities/${encodeURIComponent(peer)}`}>
            <code>{peer.slice(0, 24)}…</code>
          </Link>
        );
      },
    },
    {
      title: "置信度",
      dataIndex: "confidence",
      width: 90,
      render: (value: number) => value.toFixed(2),
    },
    {
      title: "生效区间",
      key: "valid",
      width: 250,
      render: (_, row) => (
        <span>
          <TimeValue value={row.valid_from} /> → {row.valid_to ? <TimeValue value={row.valid_to} /> : "当前有效"}
        </span>
      ),
    },
  ];

  const featureColumns: TableProps<EntityFeatureSample>["columns"] = [
    { title: "特征", dataIndex: "feature_id", width: 210, render: (value: string) => <ReadableValue value={value}/> },
    {
      title: "窗口",
      dataIndex: "window_start",
      width: 150,
      render: (value: string) => <TimeValue value={value} />,
    },
    { title: "rev", dataIndex: "revision", width: 60 },
    {
      title: "质量",
      dataIndex: "quality",
      width: 90,
      render: (value: string) => (
        <Tag className={`signal-tag ${value === "qualified" ? "status-ok" : "status-warn"}`}>{value}</Tag>
      ),
    },
    {
      title: "特征值",
      dataIndex: "values",
      ellipsis: true,
      render: (value: Record<string, unknown>) => (
        <code title={JSON.stringify(value)}>
          {Object.entries(value)
            .slice(0, 4)
            .map(([k, v]) => `${k}=${typeof v === "number" ? v.toFixed?.(2) ?? v : String(v)}`)
            .join("  ") || "—"}
        </code>
      ),
    },
  ];

  const findingColumns: TableProps<AnomalySummary>["columns"] = [
    {
      title: "风险",
      dataIndex: "severity",
      width: 82,
      render: (value: string) => <SeverityTag value={value} />,
    },
    {
      title: "Finding",
      key: "finding",
      render: (_, value) => (
        <div className="primary-cell">
          <Link to={`/anomalies/${encodeURIComponent(value.id)}`}>{anomalyTitle(value)}</Link>
          <small>检测规则：{readable(value.rule_id)} · 版本：{value.rule_version || "未提供"}</small>
        </div>
      ),
    },
    {
      title: "状态",
      dataIndex: "status",
      width: 96,
      render: (value: string) => <AnomalyStatusTag value={value} />,
    },
    {
      title: "分值",
      dataIndex: "score",
      width: 76,
      render: (value: number) => value.toFixed(2),
    },
    {
      title: "证据",
      dataIndex: "evidence_count",
      width: 72,
      render: (value: number) => `${value} 条`,
    },
    {
      title: "时间",
      dataIndex: "timestamp",
      width: 112,
      render: (value: string) => <TimeValue value={value} />,
    },
    {
      title: "",
      key: "action",
      width: 52,
      render: (_, value) => (
        <Tooltip title="五要素解释与证据">
          <Link aria-label={`解释 ${value.id}`} className="icon-link" to={`/anomalies/${encodeURIComponent(value.id)}`}>
            <ChevronRight size={18} />
          </Link>
        </Tooltip>
      ),
    },
  ];

  return (
    <div className="page-stack">
      <Drawer open={!!attributionSelected} title="事件身份关联依据" onClose={()=>setAttributionSelected(undefined)} size={600}>{attributionSelected&&<><Descriptions column={1} items={[{key:"time",label:"事件时间",children:<TimeValue value={attributionSelected.event_time}/>},{key:"role",label:"在事件中的角色",children:readable(attributionSelected.role)},{key:"state",label:"关联结果",children:readable(attributionSelected.state)},{key:"version",label:"身份匹配规则版本",children:attributionSelected.rule_version||"未提供"}]}/><h3>判断依据</h3><p>{attributionSelected.reason ? readable(attributionSelected.reason) : "记录未提供文字说明；关联结果以身份匹配规则的输出为准。"}</p>{Array.isArray(attributionSelected.evidence?.adjudication)&&<ul>{attributionSelected.evidence.adjudication.map((reason:unknown,index:number)=><li key={index}>{readable(String(reason))}</li>)}</ul>}<Alert type="info" showIcon title="身份关联不等于安全判定" description="这些记录说明账户或主机如何与事件建立联系，不代表该事件一定异常。是否存在风险请结合关联异常和事件内容判断。"/><details style={{marginTop:16}}><summary>技术追溯信息</summary><p>事件标识：{attributionSelected.event_id}</p><pre className="wb-json">{JSON.stringify(attributionSelected.evidence??{},null,2)}</pre></details></>}</Drawer>
      <BackLink to="/entities">返回实体列表</BackLink>
      <PageHeader
        eyebrow="实体详情"
        title={profileData?.canonical_key || "实体详情"}
        description="实体主档、角色关系、特征基线、异常与风险解释的统一视图。"
        actions={effectiveEntityType ? <Tag className="signal-tag">{effectiveEntityType}</Tag> : undefined}
      />

      <Tabs activeKey={tab} onChange={setTab} items={[{key:'profile',label:'主体画像'},{key:'relations',label:'身份与关系'},{key:'features',label:'特征与基线'},{key:'findings',label:'关联异常'},{key:'risk',label:'风险解释'}]}/>
      <section className="panel" hidden={tab!=="profile"}>
        <header className="panel-head">
          <div>
            <span className="panel-index">01</span>
            <h2>实体主档</h2>
            <p>标识、类型、身份空间与来源</p>
          </div>
        </header>
        {isEntityRef ? (
          profile.isLoading ? (
            <LoadingBlock rows={3} />
          ) : profile.error || !profileData ? (
            <ErrorState message={errorMessage(profile.error ?? new Error("实体不存在"))} retry={() => void profile.refetch()} />
          ) : (
            <Descriptions column={2} size="small" items={[
              { key: "id", label: "实体标识", children: <ReadableValue value={profileData.entity_id} label="查看完整实体标识"/> },
              { key: "key", label: "规范化键", children: <code>{profileData.canonical_key}</code> },
              { key: "type", label: "实体类型", children: <Tag className="signal-tag">{profileData.entity_type}</Tag> },
              { key: "strength", label: "标识强度", children: readable(profileData.identity_strength) },
              { key: "space", label: "身份空间 / 来源", children: profileData.authority },
              {
                key: "valid",
                label: "生效区间",
                children: (
                  <span>
                    <TimeValue value={profileData.valid_from} /> →{" "}
                    {profileData.valid_to ? <TimeValue value={profileData.valid_to} /> : "当前有效"}
                  </span>
                ),
              },
              { key: "rev", label: "主档版本", children: `rev ${profileData.revision}` },
              {
                key: "recent",
                label: "最近归因",
                children: `${profileData.recent_attributions.length} 条（详见 02 区块）`,
              },
            ]} />
          )
        ) : (
          <EmptyState
            title="主档查询需要强实体标识"
            description="实体注册表以 ent:…（强标识哈希）为主键。从实体列表、异常或风险页面点入可自动携带该标识；直接输入账户/主机名时本区块无法解析。"
          />
        )}
      </section>

      <section className="panel" hidden={tab!=="relations"}>
        <header className="panel-head">
          <div>
            <span className="panel-index">02</span>
            <h2>多角色与关系</h2>
            <p>归因角色与时态关系区间</p>
          </div>
        </header>
        {!isEntityRef ? (
          <EmptyState
            title="归因/关系查询需要强实体标识"
            description="归因与关系以 ent:… 强标识为键。从实体列表或异常点入可自动携带。"
          />
        ) : attributions.isLoading || relations.isLoading ? (
          <LoadingBlock rows={5} />
        ) : attributions.error || relations.error ? (
          <ErrorState
            message={errorMessage(attributions.error ?? relations.error)}
            retry={() => {
              void attributions.refetch();
              void relations.refetch();
            }}
          />
        ) : (
          <>
            <div className="data-summary">
              <span>归因 {attributions.data?.pages[0] ? attributionItems.length : 0} 条（含分页）</span>
              <span>时态关系 {relations.data?.items.length ?? 0} 条（含历史区间）</span>
            </div>
            {attributionItems.length > 0 && (
              <Table
                rowKey="attribution_id"
                columns={attributionColumns}
                dataSource={attributionItems}
                pagination={false}
                size="small"
                scroll={{ x: 720 }}
              />
            )}
            {attributions.hasNextPage && (
              <div className="load-more">
                <Button loading={attributions.isFetchingNextPage} onClick={() => void attributions.fetchNextPage()}>
                  加载更多归因
                </Button>
              </div>
            )}
            {(relations.data?.items.length ?? 0) > 0 ? (
              <Table
                rowKey="relation_id"
                columns={relationColumns}
                dataSource={relations.data?.items}
                pagination={false}
                size="small"
                scroll={{ x: 720 }}
              />
            ) : (
              attributionItems.length === 0 && <EmptyState title="无归因与关系记录" description="该实体尚未产生归因观察或时态关系。" />
            )}
          </>
        )}
      </section>

      <section className="panel" hidden={tab!=="features"}>
        <header className="panel-head">
          <div>
            <span className="panel-index">03</span>
            <h2>特征与基线</h2>
            <p>窗口特征值与基线模型状态（cold_start/ready、训练信息）</p>
          </div>
        </header>
        {!isEntityRef ? (
          <EmptyState
            title="特征/基线查询需要强实体标识"
            description="特征样本与基线状态以 ent:… 强标识为键。从实体列表或异常点入可自动携带。"
          />
        ) : features.isLoading || baseline.isLoading ? (
          <LoadingBlock rows={5} />
        ) : features.error || baseline.error ? (
          <ErrorState
            message={errorMessage(features.error ?? baseline.error)}
            retry={() => {
              void features.refetch();
              void baseline.refetch();
            }}
          />
        ) : (
          <>
            <div className="data-summary">
              <span>最近 {features.data?.items.length ?? 0} 个窗口特征样本</span>
              <span>
                基线模型{" "}
                {baseline.data?.items.map((model) => `${model.model_id} ${model.status}${model.covers_entity ? "（覆盖本实体）" : ""}`).join("、") ||
                  "无"}
              </span>
            </div>
            {(features.data?.items.length ?? 0) > 0 ? (
              <Table
                rowKey={(row) => `${row.feature_id}|${row.window_start}|${row.revision}`}
                columns={featureColumns}
                dataSource={features.data?.items}
                pagination={false}
                size="small"
                scroll={{ x: 760 }}
              />
            ) : (
              <EmptyState
                title="无窗口特征样本"
                description="该实体尚未有已关闭窗口的特征样本（基线处于 cold_start 或窗口未关闭）。"
              />
            )}
            {(baseline.data?.items.length ?? 0) > 0 && (
              <Descriptions
                column={2}
                size="small"
                items={(baseline.data?.items ?? []).map((model) => ({
                  key: model.model_id,
                  label: `${model.model_id}（${model.generation}）`,
                  children: (
                    <span>
                      <Tag className={`signal-tag ${model.status === "ready" ? "status-ok" : "status-warn"}`}>{model.status}</Tag>
                      {" "}v{model.model_version} · 样本 {model.sample_count} · 完整日 {model.complete_days}
                      {model.trained_at ? (
                        <>
                          {" "}· 训练于 <TimeValue value={model.trained_at} />
                        </>
                      ) : null}
                    </span>
                  ),
                }))}
              />
            )}
          </>
        )}
      </section>

      <section className="panel" hidden={tab!=="findings"}>
        <header className="panel-head">
          <div>
            <span className="panel-index">04</span>
            <h2>异常（Finding）列表</h2>
            <p>该实体窗口内的检测 finding；进入详情查看五要素解释（规则版本、摘要、原因码、证据、分值）</p>
          </div>
          <Space>
            <Select
              value={hours}
              onChange={(value: number) => {
                setHours(value);
                setWindow(anomalyWindow(value));
              }}
              options={entityWindowOptions}
              style={{ width: 220 }}
            />
            {anomalies.isFetching && <span className="fetching"><RefreshCw size={13} /> 更新中</span>}
          </Space>
        </header>
        {anomalies.isLoading ? (
          <LoadingBlock rows={6} />
        ) : anomalies.error ? (
          <ErrorState message={errorMessage(anomalies.error)} retry={() => void anomalies.refetch()} />
        ) : items.length === 0 ? (
          <EmptyState
            title="窗口内该实体没有异常"
            description="扩大时间窗口重试；该实体可能尚未触发检测，或实体 ID 与异常引用中的标识不一致。"
          />
        ) : (
          <>
            <div className="data-summary">
              <span>窗口内共 {total} 条 finding</span>
            </div>
            <Table
              rowKey="id"
              columns={findingColumns}
              dataSource={items}
              pagination={false}
              scroll={{ x: 860 }}
            />
            {anomalies.hasNextPage && (
              <div className="load-more">
                <Button loading={anomalies.isFetchingNextPage} onClick={() => void anomalies.fetchNextPage()}>
                  加载更多
                </Button>
              </div>
            )}
          </>
        )}
      </section>

      <section className="panel" hidden={tab!=="risk"}>
        <header className="panel-head">
          <div>
            <span className="panel-index">05</span>
            <h2>风险解释</h2>
            <p>当前风险分、贡献构成与衰减参数</p>
          </div>
        </header>
        {!isEntityRef ? (
          <EmptyState
            title="风险投影查询需要强实体标识"
            description="风险投影以 ent:… 强标识为键。从实体列表或异常点入可自动携带。"
          />
        ) : risk.isLoading ? (
          <LoadingBlock rows={3} />
        ) : risk.error ? (
          <ErrorState message={errorMessage(risk.error)} retry={() => void risk.refetch()} />
        ) : !risk.data?.projection ? (
          <EmptyState
            title="无风险投影"
            description="该实体尚无 finding 贡献（无投影属正常状态，不代表查询失败）。"
          />
        ) : (
          (() => {
            const projection = risk.data.projection;
            const score = typeof projection.risk_score === "number" ? projection.risk_score : null;
            const contributions = Array.isArray(projection.contributions) ? projection.contributions : [];
            return (
              <>
                <Descriptions column={2} size="small" items={[
                  {
                    key: "score",
                    label: "当前风险分",
                    children: (
                      <strong className={score !== null && score >= 3 ? "status-danger" : undefined}>
                        {score !== null ? score.toFixed(3) : "—"}
                      </strong>
                    ),
                  },
                  { key: "rev", label: "投影版本", children: risk.data.revision ? `rev ${risk.data.revision}` : "—" },
                  {
                    key: "decay",
                    label: "衰减",
                    children: (
                      <span>
                        半衰期 {String(projection.half_life ?? "7d")} · 归零 {String(projection.zero_after ?? "30d")}
                      </span>
                    ),
                  },
                  {
                    key: "updated",
                    label: "更新时间",
                    children: typeof projection.updated_at === "string" ? <TimeValue value={projection.updated_at} /> : "—",
                  },
                ]} />
                {contributions.length > 0 && (
                  <div className="data-summary">
                    <span>不可变贡献 {contributions.length} 条：</span>
                    {contributions.slice(0, 12).map((c, i) => {
                      const entry = c as Record<string, unknown>;
                      return (
                        <Tag key={i} className="signal-tag">
                          {String(entry.contribution_id ?? entry.id ?? `rc#${i + 1}`).slice(0, 18)}…{" "}
                          {typeof entry.score_delta === "number" ? entry.score_delta.toFixed(3) : ""}
                        </Tag>
                      );
                    })}
                  </div>
                )}
              </>
            );
          })()
        )}
      </section>
    </div>
  );
}
