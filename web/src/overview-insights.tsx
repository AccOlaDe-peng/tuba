import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { Select } from 'antd';
import { anomalyPageSchema, api, runQuery } from './api';
import { useAuth } from './auth';
import { EmptyState, ErrorState, LoadingBlock, SeverityTag, TimeValue } from './components';
import { Panel } from './workbench';

export function OverviewInsights(){
 const {token,can}=useAuth();const [hours,setHours]=useState(24);
 const from=useMemo(()=>new Date(Date.now()-hours*3600000).toISOString(),[hours]);
 const q=useQuery({queryKey:['overview','authentication-trend',hours],enabled:can('event:read'),queryFn:({signal})=>runQuery(token,'search authentication | timechart span=1h count',from,signal)});
 const recent=useQuery({queryKey:['anomalies','recent'],queryFn:({signal})=>api('/anomalies?limit=6',token,undefined,anomalyPageSchema,signal)});
 const aggregate=q.data?.aggregations?.timechart as {buckets?:{key:number;key_as_string?:string;count?:{value?:number|null};doc_count?:number}[]}|undefined;
 const rows=aggregate?.buckets??[];const maximum=Math.max(1,...rows.map(r=>r.count?.value??r.doc_count??0));
 const points=rows.map((r,i)=>({x:45+i*540/Math.max(1,rows.length-1),y:170-(r.count?.value??r.doc_count??0)/maximum*135,label:r.key_as_string??new Date(r.key).toISOString(),value:r.count?.value??r.doc_count??0}));
 const path=points.map((p,i)=>`${i?'L':'M'}${p.x} ${p.y}`).join(' ');
 return <div className="wb-two-column"><Panel title="认证事件趋势" actions={<Select aria-label="事件趋势时间范围" value={hours} onChange={setHours} options={[{value:24,label:'最近 24 小时'},{value:72,label:'最近 3 日'}]}/> }>
 {!can('event:read')?<EmptyState title="没有事件读取权限" description="当前角色只能查看授权调查数据。"/>:q.isLoading?<LoadingBlock/>:q.error?<ErrorState message={q.error.message} retry={()=>void q.refetch()}/>:!points.length?<EmptyState title="范围内没有时间桶" description="查询已完成；当前范围内没有可聚合的认证事件。"/>:<div className="wb-trend"><svg viewBox="0 0 610 215" role="img" aria-label="认证事件按小时计数趋势"><defs><linearGradient id="authentication-fill" x2="0" y2="1"><stop stopColor="#118f89" stopOpacity=".18"/><stop offset="1" stopColor="#118f89" stopOpacity="0"/></linearGradient></defs>{[0,.33,.66,1].map(r=><g key={r}><line x1="45" x2="585" y1={170-r*135} y2={170-r*135} stroke="#e9eeee"/><text x="5" y={174-r*135}>{Math.round(maximum*r)}</text></g>)}<path d={path+`L${points.at(-1)!.x} 170L45 170Z`} fill="url(#authentication-fill)"/><path d={path} stroke="#087f80" strokeWidth="2" fill="none"/>{points.filter((_,i)=>i%Math.max(1,Math.ceil(points.length/6))===0).map(p=><g key={p.label}><text x={p.x} y="198" textAnchor="middle">{new Date(p.label).toLocaleTimeString('zh-CN',{hour:'2-digit',minute:'2-digit',hour12:false})}</text><circle cx={p.x} cy={p.y} r="3" fill="#087f80"><title>{new Date(p.label).toLocaleString('zh-CN')} · {p.value} 条</title></circle></g>)}</svg><small>认证事件计数 · UTC+08:00显示 · 数据更新于 {new Date(q.dataUpdatedAt).toLocaleTimeString('zh-CN')}</small></div>}
 </Panel><Panel title="今日调查优先级" actions={<Link to="/anomalies">查看全部 →</Link>}>
 {recent.isLoading?<LoadingBlock/>:recent.error?<ErrorState message={recent.error.message} retry={()=>void recent.refetch()}/>:recent.data?.items.length?recent.data.items.slice().sort((a,b)=>b.score-a.score).slice(0,3).map(x=><Link className="wb-priority" key={x.id} to={'/anomalies/'+encodeURIComponent(x.id)}><div><strong>{x.summary||x.type}</strong><small>{x.entity.id.slice(0,25)} · <TimeValue value={x.timestamp}/></small></div><SeverityTag value={x.severity}/></Link>):<EmptyState title="当前没有调查线索" description="新的异常进入队列后会显示在此。"/>}
 </Panel></div>;
}
