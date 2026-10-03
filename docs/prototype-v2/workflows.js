'use strict';
// Reviewable target workflows. These records remain in this page's memory.
const workflowDefinitions={
 'case-assign':['分派与调整案件',[['负责人','陈晓'],['优先级','高'],['案件版本','4']], '保存前核对案件版本；并发冲突保留编辑内容，刷新差异后重试。','case'],
 'case-auto':['自动建案策略',[['开关','关闭（默认）|仅预览匹配|启用'],['匹配条件','规则 failure-then-success / severity critical'],['去重范围','相同 finding 业务键'],['负责人','未分派']], '策略需要明确授权、范围和审计。自动建案采用幂等键；修订不会重复建案。','manage'],
 'source-retire':['停用来源',[['动作','暂停接入|排空后停用'],['未确认输入','0'],['保留证据','保留原始证据与历史配置']], 'paused 可恢复；停用先处理队列并撤销来源写入授权。禁止删除未确认输入。','manage'],
 'source-context':['切换来源上下文',[['旧上下文','ctx:windows-139-r2'],['新发布','windows-security 1.1.0'],['读取位置','Security / RecordID 88142'],['范围变化','不扩大接入范围']], '新配置在独立上下文预检；对照旧水位、凭据和发布快照。发生缺口时暂停切换并给出恢复入口。','manage'],
 'agent-bind':['绑定来源实例',[['Agent','WIN-139'],['来源','Windows Security / DC-139'],['组件','Winlogbeat'],['生效版本','v13']], '核验租户、平台兼容与管理身份，来源凭据通过独立数据身份下发。','manage'],
 'agent-access':['Agent 准入控制',[['动作','停用|恢复准入'],['管理身份','agent-win-139'],['绑定来源','source-win-139']], '停用同步撤销绑定来源 Kafka WRITE ACL；撤销部分失败显示需重试。恢复先完成全部 ACL 恢复，再准入。','manage'],
 'filter-publish':['过滤策略发布',[['操作','启用准入过滤|回滚至 shadow'],['候选版本','zeek-noise-control 1.0.0'],['预估影响','2.1% / 受保护事件 0'],['资源批准','APV-DEMO-001']], '明确源端与平台端作用域；对照入站、匹配、过滤和准入计数。保护事件检查通过后才可启用。','manage'],
 'release-lifecycle':['发布版本生命周期',[['操作','暂存版本|回滚到历史版本|退役版本'],['目标版本','windows-security 1.1.0'],['当前生产引用','windows-security 1.0.0 / g1']], '退役先检查活跃任务与生产引用。回滚复核数据格式、风险代次与资源，不直接修改历史资产。','publish'],
 'model-lifecycle':['模型版本生命周期',[['操作','提交发布评审|退役模型'],['模型','auth.source-device.count / 1.0.0'],['训练输入','18 完整日 / 432 样本 / g1']], '评估通过后纳入语义发布。退役时保留历史结果与训练快照，不删除调查证据引用。','manage'],
 'member-disable':['成员有效性',[['成员','zhao.ming'],['操作','停用|恢复'],['对象版本','3']], '每次 API 请求重新校验成员有效性；停用后禁止读取、导出下载及后续写操作。','manage'],
 'service-create':['创建服务身份',[['身份名称','source-zeek-dns'],['作用域','demo-corp / source-dns / 精确 Topic'],['凭据到期','2026-10-30']], '仅显示一次初始凭据。管理身份、数据身份和发布授权独立；禁止通配写入范围。','manage'],
 'publisher-grant':['授予独立发布授权',[['用户 subject','wang.min'],['作用域','semantic-release'],['批准依据','APV-DEMO-008']], '仅平台管理员操作。与租户成员角色分别管理；授权、撤销和每次激活保留审计。','publish']
};
const workflowPanels={
 cases:['案件治理',['case-assign','case-auto']],
 case:['协作与分派',['case-assign']],
 source:['来源生命周期',['source-context','source-retire']],
 agent:['绑定与准入',['agent-bind','agent-access']],
 sources:['过滤生效管理',['filter-publish']],
 release:['暂存、回滚与退役',['release-lifecycle']],
 baseline:['模型发布与退役',['model-lifecycle']],
 access:['身份生命周期',['member-disable','service-create','publisher-grant']]
};
function workflowExtras(p){const d=workflowPanels[p];if(!d)return '';return panel(d[0],body(`<div class="actions">${d[1].map(k=>btn(workflowDefinitions[k][0],'wf:'+k,'',workflowDefinitions[k][3])).join('')}</div><p class="subtitle">操作前展示对象版本、范围与生效条件；提交结果进入审计。</p>`));}
function workflowAction(k){
 const d=workflowDefinitions[k];if(!d||!can(d[3]))return;
 modal(d[0],`<form id="workflow-form"><div class="formgrid">${d[1].map(([l,v],i)=>field(l,v.includes('|')?`<select name="wf${i}">${v.split('|').map(x=>`<option>${esc(x)}</option>`).join('')}</select>`:`<input name="wf${i}" value="${esc(v)}" required>`)).join('')}${field('说明 / 批准依据','<textarea name="reason" rows="3" required placeholder="填写操作目的或批准依据"></textarea>','',true)}</div></form>`+notice(d[2]),btn('确认提交','wf-save','primary'));
 wizard={workflow:k};
}
