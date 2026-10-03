import {
  Alert,
  App as AntApp,
  Button,
  Empty,
  Form,
  Input,
  Modal,
  Progress,
  Select,
  Skeleton,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import {
  AlertTriangle,
  ArrowUpRight,
  CheckCircle2,
  CircleDashed,
  CircleDot,
  Clock3,
  RotateCcw,
  ShieldAlert,
  UserRound,
  XCircle,
} from "lucide-react";
import type { ReactNode } from "react";

import {
  api,
  caseSchema,
  type Case,
  type CaseActivity,
  type EvidenceEvent,
} from "./api";
import { useAuth } from "./auth";

const severityMeta = {
  critical: { label: "紧急", className: "critical" },
  high: { label: "高危", className: "high" },
  medium: { label: "中危", className: "medium" },
  low: { label: "低危", className: "low" },
} as const;

const anomalyStatusMeta = {
  open: { label: "开放", className: "open" },
  investigating: { label: "调查中", className: "progress" },
  closed: { label: "已关闭", className: "closed" },
  false_positive: { label: "误报", className: "muted" },
} as const;

const caseStatusMeta = {
  open: { label: "待分派", className: "open" },
  in_progress: { label: "调查中", className: "progress" },
  closed: { label: "已结案", className: "closed" },
} as const;

export const roleLabels = {
  viewer: "只读审计员",
  analyst: "安全分析师",
  tenant_admin: "租户管理员",
} as const;

const verdictLabels = {
  true_positive: "确认威胁",
  benign_positive: "有效但无害",
  false_positive: "误报",
  inconclusive: "暂不确定",
} as const;

export function SeverityTag({ value }: { value: string }) {
  const meta = severityMeta[value as keyof typeof severityMeta];
  if (!meta) return <Tag className="signal-tag status-muted">{value || "未知"}</Tag>;
  return <Tag className={`signal-tag severity-${meta.className}`}>{meta.label}</Tag>;
}

export function AnomalyStatusTag({ value }: { value: string }) {
  const meta = anomalyStatusMeta[value as keyof typeof anomalyStatusMeta];
  if (!meta) return <Tag className="signal-tag status-muted">{value || "未知"}</Tag>;
  return <Tag className={`signal-tag status-${meta.className}`}>{meta.label}</Tag>;
}

export function CaseStatusTag({ value }: { value: string }) {
  const meta = caseStatusMeta[value as keyof typeof caseStatusMeta];
  if (!meta) return <Tag className="signal-tag status-muted">{value || "未知"}</Tag>;
  return <Tag className={`signal-tag status-${meta.className}`}>{meta.label}</Tag>;
}

export function RoleTag({ value }: { value: string }) {
  const label = roleLabels[value as keyof typeof roleLabels] ?? value;
  return <Tag className={`signal-tag role-${value}`}>{label}</Tag>;
}

export function VerdictTag({ value }: { value?: string }) {
  if (!value) return <span className="muted">未判定</span>;
  return <Tag className="signal-tag verdict">{verdictLabels[value as keyof typeof verdictLabels] ?? value}</Tag>;
}

export function PageHeader({
  eyebrow,
  title,
  description,
  actions,
}: {
  eyebrow: string;
  title: string;
  description: string;
  actions?: ReactNode;
}) {
  return (
    <header className="page-header">
      <div>
        <span className="eyebrow">{eyebrow}</span>
        <Typography.Title level={1}>{title}</Typography.Title>
        <Typography.Paragraph>{description}</Typography.Paragraph>
      </div>
      {actions && <div className="page-actions">{actions}</div>}
    </header>
  );
}

export function MetricPanel({
  label,
  value,
  detail,
  tone = "neutral",
  icon,
}: {
  label: string;
  value: ReactNode;
  detail: string;
  tone?: "neutral" | "good" | "warn" | "danger";
  icon: ReactNode;
}) {
  return (
    <article className={`metric-panel tone-${tone}`}>
      <div className="metric-icon">{icon}</div>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </article>
  );
}

export function LoadingBlock({ rows = 4 }: { rows?: number }) {
  return (
    <div className="loading-block" aria-label="正在加载">
      <Skeleton active paragraph={{ rows }} />
    </div>
  );
}

export function ErrorState({ message, retry }: { message: string; retry?: () => void }) {
  return (
    <Alert
      className="state-alert"
      type="error"
      showIcon
      title="数据加载失败"
      description={message}
      action={
        retry ? (
          <Tooltip title="重新请求">
            <Button aria-label="重新请求" icon={<RotateCcw size={16} />} onClick={retry} />
          </Tooltip>
        ) : undefined
      }
    />
  );
}

export function EmptyState({ title, description }: { title: string; description: string }) {
  return (
    <div className="empty-state">
      <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={<><strong>{title}</strong><span>{description}</span></>} />
    </div>
  );
}

export function TimeValue({ value }: { value?: string }) {
  if (!value) return <span className="muted">未知</span>;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return <span>{value}</span>;
  const relative = new Intl.RelativeTimeFormat("zh-CN", { numeric: "auto" });
  const seconds = Math.round((date.getTime() - Date.now()) / 1000);
  const label =
    Math.abs(seconds) < 60
      ? relative.format(seconds, "second")
      : Math.abs(seconds) < 3600
        ? relative.format(Math.round(seconds / 60), "minute")
        : Math.abs(seconds) < 86400
          ? relative.format(Math.round(seconds / 3600), "hour")
          : relative.format(Math.round(seconds / 86400), "day");
  return (
    <Tooltip title={date.toLocaleString("zh-CN", { hour12: false })}>
      <span className="time-value">{label}</span>
    </Tooltip>
  );
}

export function ScoreGauge({ score }: { score: number }) {
  const percent = Math.max(0, Math.min(100, Math.round(score * 100)));
  const stroke = percent >= 80 ? "#bd3f3f" : percent >= 60 ? "#b47620" : "#167c72";
  return <Progress type="circle" size={68} percent={percent} strokeColor={stroke} railColor="#e6eceb" format={(value) => <strong>{value}</strong>} />;
}

export function EvidenceTimeline({ items, onSelect }: { items: EvidenceEvent[]; onSelect?: (item: EvidenceEvent) => void }) {
  return (
    <div className="evidence-timeline">
      {items.map((item, index) => {
        const outcome = String(item.event.outcome ?? "unknown");
        const action = String(item.event.action ?? "认证活动");
        const dataset = String(item.event.dataset ?? "authentication");
        return (
          <article className={`evidence-item outcome-${outcome}`} key={item.id}>
            <div className="evidence-rail">
              {outcome === "failure" ? <XCircle size={16} /> : outcome === "success" ? <CheckCircle2 size={16} /> : <CircleDot size={16} />}
              {index < items.length - 1 && <span />}
            </div>
            <div className="evidence-copy">
              <div>
                <strong>{outcome === "failure" ? "认证失败" : outcome === "success" ? "认证成功" : "认证事件"}</strong>
                <Tag>{action}</Tag>
              </div>
              <p>{dataset} · {String(item.user.id ?? item.user.name ?? "未知实体")}</p>
              <code>{item.id}</code>
              {onSelect && <Button type="link" size="small" onClick={() => onSelect(item)}>查看事件证据 →</Button>}
            </div>
            <TimeValue value={item.timestamp} />
          </article>
        );
      })}
    </div>
  );
}

const actionLabels: Record<string, string> = {
  "case.create": "创建案件",
  "case.update": "更新案件",
};

export function ActivityTimeline({ items }: { items: CaseActivity[] }) {
  return (
    <div className="activity-timeline">
      {items.map((item) => (
        <article key={item.id}>
          <div className="activity-dot">
            {item.action === "case.create" ? <CircleDashed size={16} /> : <CircleDot size={16} />}
          </div>
          <div>
            <strong>{actionLabels[item.action] ?? item.action}</strong>
            <p>
              <UserRound size={13} />
              {item.actor || "系统"}
              <span>·</span>
              <TimeValue value={item.occurred_at} />
            </p>
          </div>
          <code>{item.request_id}</code>
        </article>
      ))}
    </div>
  );
}

export function CaseProgress({ status }: { status: string }) {
  const activeIndex = status === "closed" ? 2 : status === "in_progress" ? 1 : 0;
  const steps = [
    { label: "待分派", icon: <AlertTriangle size={15} /> },
    { label: "调查处置", icon: <Clock3 size={15} /> },
    { label: "结案", icon: <ShieldAlert size={15} /> },
  ];
  return (
    <div className="case-progress">
      {steps.map((step, index) => (
        <div className={index <= activeIndex ? "active" : ""} key={step.label}>
          <span>{step.icon}</span>
          <strong>{step.label}</strong>
          {index < steps.length - 1 && <i />}
        </div>
      ))}
    </div>
  );
}

export type CasePrefill = {
  title?: string;
  description?: string;
  severity?: Case["severity"];
  anomalyIds?: string[];
};

export function CreateCaseModal({
  open,
  prefill,
  onClose,
  onCreated,
}: {
  open: boolean;
  prefill?: CasePrefill;
  onClose: () => void;
  onCreated?: (value: Case) => void;
}) {
  const { token } = useAuth();
  const { message } = AntApp.useApp();
  const queryClient = useQueryClient();
  const mutation = useMutation({
    mutationFn: (values: { title: string; severity: Case["severity"]; description?: string }) =>
      api<Case>(
        "/cases",
        token,
        {
          method: "POST",
          headers: { "Idempotency-Key": crypto.randomUUID() },
          body: JSON.stringify({ ...values, anomaly_ids: prefill?.anomalyIds ?? [] }),
        },
        caseSchema,
      ),
    onSuccess: (created) => {
      void message.success("案件已创建");
      void queryClient.invalidateQueries({ queryKey: ["cases"] });
      void queryClient.invalidateQueries({ queryKey: ["overview"] });
      onCreated?.(created);
      onClose();
    },
    onError: (error) => void message.error(error instanceof Error ? error.message : "创建失败"),
  });

  return (
    <Modal
      title="创建调查案件"
      open={open}
      footer={null}
      destroyOnHidden
      onCancel={onClose}
    >
      <Form
        layout="vertical"
        initialValues={{
          title: prefill?.title ?? "",
          description: prefill?.description ?? "",
          severity: prefill?.severity ?? "medium",
        }}
        onFinish={(values) => mutation.mutate(values)}
      >
        <Form.Item name="title" label="案件标题" rules={[{ required: true, max: 300 }]}>
          <Input autoFocus placeholder="描述需要调查的核心问题" />
        </Form.Item>
        <Form.Item name="severity" label="优先级" rules={[{ required: true }]}>
          <Select
            options={[
              { value: "critical", label: "紧急" },
              { value: "high", label: "高危" },
              { value: "medium", label: "中危" },
              { value: "low", label: "低危" },
            ]}
          />
        </Form.Item>
        <Form.Item name="description" label="调查背景">
          <Input.TextArea rows={5} maxLength={5000} showCount />
        </Form.Item>
        <div className="modal-actions">
          <Button onClick={onClose}>取消</Button>
          <Button type="primary" htmlType="submit" loading={mutation.isPending} icon={<ArrowUpRight size={16} />}>
            创建案件
          </Button>
        </div>
      </Form>
    </Modal>
  );
}
