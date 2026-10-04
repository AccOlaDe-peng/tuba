import { Tooltip } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { api, getEntity, sourcesSchema } from './api';
import { useAuth } from './auth';

const labels: Record<string,string> = {
 'auth.failure-then-success':'失败后成功登录','auth.failure-burst':'登录失败集中发生',
 true_positive:'确认异常',false_positive:'误报',false_negative:'漏报',inconclusive:'证据不足',benign_positive:'有效但无害',
 active:'已启用',disabled:'已停用',pending:'待处理',running:'运行中',idle:'空闲',failed:'失败',succeeded:'已完成',queued:'排队中',
 draft:'草稿',validated:'已验证',staged:'已暂存',retired:'已退役',open:'待处理',investigating:'调查中',in_progress:'调查中',closed:'已关闭',
 cold_start:'样本不足',ready:'就绪',healthy:'正常',degraded:'异常',unavailable:'不可用',confirmed:'已确认',
 account:'账户',device:'主机',strong:'可靠标识',weak:'待确认标识',resolved:'已关联',unresolved:'未关联',ambiguous:'存在歧义',
 normal:'普通字段',sensitive:'敏感字段',raw:'原始证据',authentication:'认证事件',network:'网络连接',dns:'域名解析',session:'会话',directory:'目录变更',iam:'身份与权限',web:'网页访问',tls:'加密连接',
 starting:'启动中',stopping:'停止中',stopped:'已停止',upgrading:'升级中',rollback:'回退中',error:'异常',enabled:'已启用',online:'在线',offline:'离线',
 actor:'操作发起者',target:'操作对象',observer:'事件观察者',source:'来源',destination:'目标',
 'credential-validation':'凭据验证','AUTH_FAILURE_BURST_THEN_SUCCESS':'短时间内多次认证失败后出现一次成功认证',success:'成功',failure:'失败',
 resolved_by_strong_identifier:'通过稳定身份标识关联到此实体',resolved_by_weak_identifier_occurrence_at_event_time:'根据事件发生时有效的名称或地址关联到此实体',
 multiple_identifiers_agree:'多项身份信息指向同一实体',strong_identifier_priority_over_weak:'优先采用稳定身份标识',
 no_identifiers_reported:'事件没有提供身份信息',all_identifiers_failed_normalization:'事件中的身份信息无法解析',
 'AUTH_FAILURE_BURST':'短时间内多次登录失败','AUTH_FAILURE_THEN_SUCCESS':'多次失败后登录成功',
 vendor_name:'厂商',vendor_product:'产品',vendor_dataset:'数据集',release_id:'发布包',rate_limit:'每秒接入上限',
};
export function readable(value: string | undefined | null, fallback='未提供名称'): string {
 if (!value) return '未提供';
 if (labels[value]) return labels[value];
 if (/^(?:ent:|evt:|rc:|src_|col_|anom:)|^[a-f0-9]{32,}$|^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i.test(value)) return fallback;
 return value;
}
export function readableDescription(value?:string) {
 if (!value) return '未填写调查说明';
 return /\uFFFD/.test(value) ? '调查说明包含无法识别的字符，请联系记录创建者核对原文。' : value;
}
export function ReadableValue({value,label}:{value?:string|null;label?:string}) {
 return <Tooltip title={value ? `技术标识：${value}` : undefined}><span>{readable(value,label)}</span></Tooltip>;
}
export function EntityName({id}:{id:string}) {
 const {token}=useAuth();
 const query=useQuery({queryKey:['entities',id,'profile'],enabled:/^ent:[a-f0-9]{64}$/.test(id),staleTime:300_000,retry:false,queryFn:({signal})=>getEntity(token,id,signal)});
 return <Tooltip title={`实体标识：${id}`}><span>{query.data?.canonical_key||readable(id,query.isLoading?'正在读取实体名称':'实体名称暂不可用')}</span></Tooltip>;
}
export function SourceName({id}:{id:string}) {
 const {token,can}=useAuth();
 const query=useQuery({queryKey:['sources'],enabled:can('source:manage'),staleTime:60_000,retry:false,queryFn:({signal})=>api('/sources',token,undefined,sourcesSchema,signal)});
 const source=query.data?.items.find(item=>item.id===id);
 return <Tooltip title={`来源标识：${id}`}><span>{source?`${source.vendor_product} · ${source.vendor_dataset}`:'来源名称暂不可用'}</span></Tooltip>;
}
