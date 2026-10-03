import { useState } from 'react';
import { App, Button, Drawer, Form, Input, List, Popconfirm } from 'antd';
import { useAuth } from './auth';
type Bookmark={name:string;dataset:string;spl:string;range:string};
export function QueryBookmarks({dataset,spl,range,onLoad}:{dataset:string;spl:string;range:string;onLoad:(value:Bookmark)=>void}){
 const {principal}=useAuth();const {message}=App.useApp();const [open,setOpen]=useState(false);const [items,setItems]=useState<Bookmark[]>([]);
 const key='tuba.query-bookmarks:'+JSON.stringify([principal?.subject,principal?.organization_id,principal?.namespace]);
 function show(){try{const value=JSON.parse(sessionStorage.getItem(key)??'[]') as unknown;setItems(Array.isArray(value)?value.filter((x):x is Bookmark=>!!x&&typeof x==='object'&&['name','dataset','spl','range'].every(k=>typeof (x as Record<string,unknown>)[k]==='string')):[]);setOpen(true);}catch{void message.error('无法读取此会话的查询记录');}}
 function save(value:Bookmark[]){try{sessionStorage.setItem(key,JSON.stringify(value));setItems(value);}catch{void message.error('浏览器不允许保存查询');}}
 return <><Button onClick={show}>保存 / 载入查询</Button><Drawer open={open} onClose={()=>setOpen(false)} title="此会话的查询" size={500}><p>仅保存在当前浏览器会话，退出登录后清除；不与其他成员共享。</p><Form layout="vertical" onFinish={({name}:{name:string})=>save([{name,dataset,spl,range},...items.filter(x=>x.name!==name)].slice(0,20))}><Form.Item name="name" label="查询名称" rules={[{required:true,max:100}]}><Input/></Form.Item><Button htmlType="submit" type="primary" disabled={!spl.trim()}>保存当前查询</Button></Form><List dataSource={items} locale={{emptyText:'此会话尚未保存查询'}} renderItem={x=><List.Item actions={[<Button key="load" type="link" onClick={()=>{onLoad(x);setOpen(false);}}>载入</Button>,<Popconfirm key="remove" title="移除此查询？" onConfirm={()=>save(items.filter(v=>v.name!==x.name))}><Button type="link" danger>移除</Button></Popconfirm>]}><List.Item.Meta title={x.name} description={x.dataset+' / '+x.range}/></List.Item>}/></Drawer></>;
}
