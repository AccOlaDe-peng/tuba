import { useMemo, useState } from "react";
import {
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
  Tag,
  Tooltip,
} from "antd";
import type { TableProps } from "antd";
import {
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import {
  Activity,
  AlertTriangle,
  ArrowLeft,
  ArrowRight,
  Check,
  ChevronRight,
  CircleAlert,
  Database,
  FileSearch,
  Filter,
  FolderKanban,
  Gauge,
  KeyRound,
  Plus,
  RefreshCw,
  Search,
  Server,
  ShieldCheck,
  UserPlus,
  Users,
} from "lucide-react";
import { Link, useNavigate, useParams } from "react-router-dom";

import {
  api,
  analysisFeedbackSchema,
  anomalyDetailSchema,
  anomalyPageSchema,
  caseActivityPageSchema,
  caseSchema,
  casesSchema,
  evidencePageSchema,
  collectorsSchema,
  formatDuration,
  membersSchema,
  operationsStatusSchema,
  overviewSchema,
  queryString,
  sourcesSchema,
  type AnomalySummary,
  type Case,
  type CollectorSummary,
  type Member,
  type SourceInstance,
} from "./api";
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
  ScoreGauge,
  SeverityTag,
  TimeValue,
  VerdictTag,
  roleLabels,
  type CasePrefill,
} from "./components";

const anomalyTypeLabels: Record<string, string> = {
  "auth.failure-then-success": "失败后成功登录",
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
          <small>{value.entity.id}</small>
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
    <div className="page-stack">
      <PageHeader
        eyebrow={`${principal?.organization_id ?? "租户"} / 24 小时视角`}
        title="安全总览"
        description="聚焦开放风险、调查责任和分析链路新鲜度。"
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
          label="检测新鲜度"
          value={formatDuration(freshness)}
          detail={freshness !== null && freshness < 300 ? "目标窗口内" : "关注分析消费状态"}
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

      <section className="dashboard-grid">
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
      </section>
    </div>
  );
}

type AnomalyFilters = {
  severity: string;
  status: string;
  entity: string;
};

export function Anomalies() {
  const { token } = useAuth();
  const [draft, setDraft] = useState<AnomalyFilters>({ severity: "", status: "", entity: "" });
  const [filters, setFilters] = useState<AnomalyFilters>(draft);
  const query = useInfiniteQuery({
    queryKey: ["anomalies", filters],
    initialPageParam: "",
    queryFn: ({ pageParam, signal }) =>
      api(
        `/anomalies${queryString({ limit: 50, cursor: pageParam, ...filters })}`,
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
          <small>{value.rule_id}@{value.rule_version} · {value.id.slice(0, 22)}</small>
        </div>
      ),
    },
    {
      title: "实体",
      key: "entity",
      width: 170,
      render: (_, value) => (
        <div className="entity-cell">
          <strong>{value.entity.id}</strong>
          <small>{value.entity.type}</small>
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
      <section className="filter-bar">
        <div className="filter-search">
          <Search size={16} />
          <Input
            variant="borderless"
            placeholder="输入账户、主机或其他实体 ID"
            value={draft.entity}
            onChange={(event) => setDraft((value) => ({ ...value, entity: event.target.value }))}
            onPressEnter={() => setFilters(draft)}
          />
        </div>
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
          <span>匹配 {total} 条</span>
          {query.isFetching && <span className="fetching"><RefreshCw size={13} /> 更新中</span>}
        </div>
        {query.isLoading ? (
          <LoadingBlock rows={7} />
        ) : query.error ? (
          <ErrorState message={errorMessage(query.error)} retry={() => void query.refetch()} />
        ) : items.length === 0 ? (
          <EmptyState title="没有匹配的异常" description="调整风险等级、状态或实体条件后重试。" />
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
      void message.success("分析反馈已记录");
      setFeedbackOpen(false);
    },
    onError: (error) => void message.error(errorMessage(error)),
  });
  const prefill: CasePrefill | undefined = detail.data
    ? {
        title: `${anomalyTitle(detail.data)} · ${detail.data.entity.id}`,
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
    <div className="page-stack">
      <BackLink to="/anomalies">返回异常队列</BackLink>
      <PageHeader
        eyebrow={`${anomaly.rule_id}@${anomaly.rule_version}`}
        title={anomalyTitle(anomaly)}
        description={anomaly.summary || "检测规则未提供摘要。"}
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
        <ScoreGauge score={anomaly.score} />
        <div className="anomaly-identity">
          <Space wrap>
            <SeverityTag value={anomaly.severity} />
            <AnomalyStatusTag value={anomaly.status} />
            <Tag className="signal-tag">{anomaly.entity.type}</Tag>
          </Space>
          <strong>{anomaly.entity.id}</strong>
          <code>{anomaly.id}</code>
        </div>
        <dl>
          <div>
            <dt>触发时间</dt>
            <dd><TimeValue value={anomaly.timestamp} /></dd>
          </div>
          <div>
            <dt>证据事件</dt>
            <dd>{anomaly.evidence_count}</dd>
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
              <p>仅展示异常引用的认证事件，重复事件已折叠</p>
            </div>
            {evidence.data?.truncated && <Tag color="warning">结果已截断</Tag>}
          </header>
          {evidence.isLoading ? (
            <LoadingBlock rows={6} />
          ) : evidence.error ? (
            <ErrorState message={errorMessage(evidence.error)} retry={() => void evidence.refetch()} />
          ) : evidence.data?.items.length ? (
            <EvidenceTimeline items={evidence.data.items} />
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
                    <code>{reason}</code>
                  </div>
                ))
              ) : (
                <span className="muted">规则未返回原因码</span>
              )}
            </div>
          </article>
          <article className="panel">
            <header className="panel-head">
              <div>
                <span className="panel-index">03</span>
                <h2>调查边界</h2>
              </div>
            </header>
            <Descriptions column={1} size="small" items={[
              { key: "tenant", label: "租户", children: "由服务端从授权会话确定" },
              { key: "namespace", label: "数据域", children: "当前租户隔离命名空间" },
              { key: "raw", label: "原始数据", children: "浏览器无 Elasticsearch 凭据" },
            ]} />
          </article>
        </aside>
      </section>

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
          <small>{value.description || value.id}</small>
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
      width: 100,
      render: (value: string) => <CaseStatusTag value={value} />,
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
        <MetricPanel label="进行中" value={overview.data?.cases.active_cases ?? 0} detail="当前处置负载" icon={<FolderKanban size={19} />} />
        <MetricPanel label="待分派" value={overview.data?.cases.unassigned_cases ?? 0} detail="需要明确责任人" tone="warn" icon={<Users size={19} />} />
        <MetricPanel label="今日结案" value={overview.data?.cases.closed_today ?? 0} detail="已完成判定和归档" tone="good" icon={<ShieldCheck size={19} />} />
      </section>
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
            <Table rowKey="id" columns={columns} dataSource={items} pagination={false} scroll={{ x: 900 }} />
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
  const [closeOpen, setCloseOpen] = useState(false);
  const [assignOpen, setAssignOpen] = useState(false);
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
      void queryClient.invalidateQueries({ queryKey: ["case", id] });
      void queryClient.invalidateQueries({ queryKey: ["cases"] });
      void queryClient.invalidateQueries({ queryKey: ["overview"] });
    },
    onError: (error) => {
      const text = errorMessage(error);
      void message.error(text.includes("conflict") ? "案件已被其他用户更新，请刷新后重试" : text);
    },
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
        eyebrow={`案件 ${current.id.slice(0, 12)} / 版本 ${current.version}`}
        title={current.title}
        description={current.description || "未填写调查背景。"}
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
            </Space>
          ) : undefined
        }
      />
      <CaseProgress status={current.status} />
      <section className="case-facts">
        <div><span>优先级</span><SeverityTag value={current.severity} /></div>
        <div><span>状态</span><CaseStatusTag value={current.status} /></div>
        <div><span>负责人</span><strong>{current.assignee || "未分派"}</strong></div>
        <div><span>判定结论</span><VerdictTag value={current.verdict} /></div>
        <div><span>关联异常</span><strong>{current.anomaly_ids.length} 条</strong></div>
        <div><span>最后更新</span><TimeValue value={current.updated_at} /></div>
      </section>
      <section className="detail-layout">
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
                {current.anomaly_ids.map((anomalyID) => (
                  <Link key={anomalyID} to={`/anomalies/${encodeURIComponent(anomalyID)}`}>
                    <FileSearch size={16} />
                    <span>{anomalyID}</span>
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

      <Modal title="分派案件" open={assignOpen} footer={null} onCancel={() => setAssignOpen(false)}>
        <Form
          layout="vertical"
          onFinish={(values: { assignee: string; reason?: string }) =>
            update.mutate({ assignee: values.assignee, reason: values.reason })
          }
        >
          <Form.Item name="assignee" label="负责人身份" rules={[{ required: true, max: 128 }]}>
            <Input autoFocus placeholder="输入 IAM subject" />
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
  const { message } = AntApp.useApp();
  const queryClient = useQueryClient();
  const [addOpen, setAddOpen] = useState(false);
  const [selected, setSelected] = useState<string>();
  const query = useQuery({
    queryKey: ["members"],
    queryFn: ({ signal }) => api("/members", token, undefined, membersSchema, signal),
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
        title="用户与权限"
        description="成员关系以 PostgreSQL 为权威，身份来自企业 IAM。"
        actions={
          <Button type="primary" icon={<UserPlus size={16} />} onClick={() => setAddOpen(true)}>
            添加成员
          </Button>
        }
      />
      <section className="access-layout">
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

      <Modal title="添加成员" open={addOpen} footer={null} onCancel={() => setAddOpen(false)}>
        <Form
          layout="vertical"
          onFinish={(values: { subject: string; role: Member["role"] }) => {
            changeRole.mutate({ ...values, grant: true }, { onSuccess: () => setAddOpen(false) });
          }}
        >
          <Form.Item name="subject" label="IAM Subject" rules={[{ required: true, max: 256 }]}>
            <Input autoFocus placeholder="例如 wang.min" />
          </Form.Item>
          <Form.Item name="role" label="初始角色" rules={[{ required: true }]}>
            <Select
              options={Object.entries(roleLabels).map(([value, label]) => ({ value, label }))}
            />
          </Form.Item>
          <div className="modal-actions">
            <Button onClick={() => setAddOpen(false)}>取消</Button>
            <Button type="primary" htmlType="submit" loading={changeRole.isPending}>添加</Button>
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
        title="系统运行"
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
      <section className="operations-grid">
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
      <section className="panel runtime-panel">
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
          <strong>{value.hostname || value.id}</strong>
          <small>{value.id} · {value.os}/{value.architecture} · Agent {value.installed_version || "未知"}</small>
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
          <small>{value.id} · epoch {value.source_epoch}</small>
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
    </div>
  );
}
