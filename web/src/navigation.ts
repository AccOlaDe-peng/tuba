export const workbenchNavigation: {group:string;items:[string,string,string][]}[] = [
  { group: '调查工作台', items: [
    ['overview', '安全总览', 'anomaly:read'], ['anomalies', '异常调查', 'anomaly:read'],
    ['risks', '实体风险', 'anomaly:read'], ['entities', '实体目录', 'anomaly:read'], ['cases', '案件中心', 'case:read'],
  ] },
  { group: '数据治理', items: [
    ['events', '事件检索', 'event:read'], ['sources', '数据来源', 'source:manage'],
    ['quality', '数据质量', 'event:read'], ['catalog', '数据模型目录', 'event:read'],
  ] },
  { group: '分析运营', items: [
    ['analysis', '分析资产', 'release:read'], ['baseline', '统计基线', 'anomaly:read'], ['feedback', '反馈与评估', 'analysis:feedback'],
  ] },
  { group: '平台管理', items: [
    ['releases', '语义发布', 'release:read'], ['jobs', '任务中心', 'event:read'], ['replay', '回放与回填', 'event:read'],
    ['operations', '运行与容量', 'operations:read'], ['audit', '审计日志', 'user:manage'], ['access', '访问控制', 'user:manage'],
  ] },
];
export const routeTitles: Record<string, string> = Object.fromEntries(workbenchNavigation.flatMap(g => g.items.map(([p,t])=>[p,t])));
Object.assign(routeTitles, {agents:'Agent 与组件', backups:'备份与恢复', exports:'导出任务', publishers:'独立发布授权', login:'登录', states:'界面状态', quarantine:'隔离事件'});
export const routeParents: Record<string,string> = {agents:'sources',backups:'operations',exports:'jobs',publishers:'releases',quarantine:'quality'};
