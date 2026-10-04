import { readable } from "./readable";
import { useMemo, useState, type ReactNode } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { Select } from 'antd';
import { anomalyPageSchema, api, getEntity, runQuery, type AnomalySummary } from './api';
import { useAuth } from './auth';
import { AnomalyStatusTag, EmptyState, ErrorState, LoadingBlock, SeverityTag, TimeValue } from './components';
import { Panel } from './workbench';

function PriorityItem({item,rank}:{item:AnomalySummary;rank:number}) {
 const {token}=useAuth();
 const profile=useQuery({queryKey:['entities',item.entity.id,'profile'],enabled:/^ent:[a-f0-9]{64}$/.test(item.entity.id),staleTime:300_000,retry:false,queryFn:({signal})=>getEntity(token,item.entity.id,signal)});
 const entity=profile.data?.canonical_key||readable(item.entity.id,'实体名称暂不可用');
 const names:Record<string,string>={'auth.failure-then-success':'失败后成功登录','auth.failure-burst':'登录失败集中发生'};
 return <article className="wb-priority-card">
  <div className="wb-priority-heading"><span className="wb-priority-rank" aria-label={`优先级 ${rank}`}>{String(rank).padStart(2,'0')}</span><SeverityTag value={item.severity}/><AnomalyStatusTag value={item.status}/></div>
  <Link className="wb-priority-title" to={'/anomalies/'+encodeURIComponent(item.id)}>{names[item.type]||item.type}</Link>
  {item.summary&&item.summary!==item.type&&<p className="wb-priority-summary">{item.summary}</p>}
  <dl className="wb-priority-facts"><div><dt>{item.entity.type==='device'?'主机':'实体'}</dt><dd><Link title={item.entity.id} to={'/entities/'+encodeURIComponent(item.entity.id)}>{entity}</Link>{profile.data?.authority&&<small>{profile.data.authority}</small>}</dd></div><div><dt>发生时间</dt><dd><TimeValue value={item.timestamp}/></dd></div><div><dt>关联证据</dt><dd>{item.evidence_count} 条</dd></div></dl>
  <Link className="wb-priority-action" to={'/anomalies/'+encodeURIComponent(item.id)}>查看证据并调查 <span aria-hidden="true">→</span></Link>
 </article>;
}

export function OverviewInsights({mainBelow,asideBelow}:{mainBelow?:ReactNode;asideBelow?:ReactNode}){
 const {token,can}=useAuth();const [hours,setHours]=useState(24);
 const from=useMemo(()=>new Date(Date.now()-hours*3600000).toISOString(),[hours]);
 const q=useQuery({queryKey:['overview','authentication-trend',hours],enabled:can('event:read'),queryFn:({signal})=>runQuery(token,'search authentication | timechart span=1h count',from,signal)});
 const recent=useQuery({queryKey:['anomalies','recent'],queryFn:({signal})=>api('/anomalies?limit=6',token,undefined,anomalyPageSchema,signal)});
 const aggregate=q.data?.aggregations?.timechart as {buckets?:{key:number;key_as_string?:string;count?:{value?:number|null};doc_count?:number}[]}|undefined;
 const rows=aggregate?.buckets??[];const maximum=Math.max(1,...rows.map(r=>r.count?.value??r.doc_count??0));
 const points=rows.map((r,i)=>({x:45+i*540/Math.max(1,rows.length-1),y:170-(r.count?.value??r.doc_count??0)/maximum*135,label:r.key_as_string??new Date(r.key).toISOString(),value:r.count?.value??r.doc_count??0}));
 const path=points.map((p,i)=>`${i?'L':'M'}${p.x} ${p.y}`).join(' ');
 const severityOrder={critical:4,high:3,medium:2,low:1};
 const priorities=(recent.data?.items??[]).filter(x=>x.status==='open'||x.status==='investigating').slice().sort((a,b)=>severityOrder[b.severity]-severityOrder[a.severity]||b.score-a.score||Date.parse(b.timestamp)-Date.parse(a.timestamp)).slice(0,3);
 return <div className="wb-two-column overview-columns"><div className="overview-column"><Panel title="认证事件趋势" actions={<Select aria-label="事件趋势时间范围" value={hours} onChange={setHours} options={[{value:24,label:'最近 24 小时'},{value:72,label:'最近 3 日'}]}/> }>
 {!can('event:read')?<EmptyState title="没有事件读取权限" description="当前角色只能查看授权调查数据。"/>:q.isLoading?<LoadingBlock/>:q.error?<ErrorState message={q.error.message} retry={()=>void q.refetch()}/>:!points.length?<EmptyState title="范围内没有时间桶" description="查询已完成；当前范围内没有可聚合的认证事件。"/>:<div className="wb-trend"><svg viewBox="0 0 610 215" role="img" aria-label="认证事件按小时计数趋势"><defs><linearGradient id="authentication-fill" x2="0" y2="1"><stop stopColor="#118f89" stopOpacity=".18"/><stop offset="1" stopColor="#118f89" stopOpacity="0"/></linearGradient></defs>{[0,.33,.66,1].map(r=><g key={r}><line x1="45" x2="585" y1={170-r*135} y2={170-r*135} stroke="#e9eeee"/><text x="5" y={174-r*135}>{Math.round(maximum*r)}</text></g>)}<path d={path+`L${points.at(-1)!.x} 170L45 170Z`} fill="url(#authentication-fill)"/><path d={path} stroke="#087f80" strokeWidth="2" fill="none"/>{points.filter((_,i)=>i%Math.max(1,Math.ceil(points.length/6))===0).map(p=><g key={p.label}><text x={p.x} y="198" textAnchor="middle">{new Date(p.label).toLocaleTimeString('zh-CN',{hour:'2-digit',minute:'2-digit',hour12:false})}</text><circle cx={p.x} cy={p.y} r="3" fill="#087f80"><title>{new Date(p.label).toLocaleString('zh-CN')} · {p.value} 条</title></circle></g>)}</svg><small>认证事件计数 · UTC+08:00显示 · 数据更新于 {new Date(q.dataUpdatedAt).toLocaleTimeString('zh-CN')}</small></div>}
 </Panel>{mainBelow}</div><div className="overview-column"><Panel title="今日调查优先级" actions={<Link to="/anomalies">查看全部 →</Link>}>
 <p className="wb-priority-note">从最近 6 条异常中选取待处理项，按风险等级、检测分值排序，最多显示 3 条。</p>
 {recent.isLoading?<LoadingBlock/>:recent.error?<ErrorState message={recent.error.message} retry={()=>void recent.refetch()}/>:priorities.length?<div className="wb-priority-list">{priorities.map((item,index)=><PriorityItem key={item.id} item={item} rank={index+1}/>)}</div>:<EmptyState title="最近异常中没有待处理项" description="可进入异常调查查看完整队列。"/>}
 </Panel>{asideBelow}</div></div>;
}
