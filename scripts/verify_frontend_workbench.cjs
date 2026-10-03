// Isolated browser verification with fixtures. Never connects to deployed APIs.
const {chromium}=require('playwright');
const fs=require('node:fs');
const assert=require('node:assert/strict');
const base=process.env.TUBA_FRONTEND_PREVIEW_URL||'http://127.0.0.1:5174';
const output='.runtime/frontend-workbench';fs.mkdirSync(output,{recursive:true});
const entity='ent:'+ 'a'.repeat(64), source='src_'+ 'b'.repeat(32), agent='col_'+'c'.repeat(32), time='2026-10-03T10:00:00Z';
const permissions=['anomaly:read','event:read','raw:read','case:read','case:write','source:manage','release:read','release:manage','user:manage','operations:read','analysis:feedback'];
const sourceData={id:source,organization_id:'fixture-corp',namespace:'fixture',vendor_name:'Microsoft',vendor_product:'Windows Security',vendor_dataset:'Security',release_id:'windows-1',source_epoch:'e1',source_context_id:'ctx_'+'d'.repeat(32),state:'active',enabled:true,rate_limit:1000,created_at:time,updated_at:time};
const entityData={entity_id:entity,entity_type:'account',authority:'FIXTURE.LOCAL',canonical_key:'fixture.user',identity_strength:'strong',revision:2,valid_from:time};
const collector={id:agent,organization_id:'fixture-corp',namespace:'fixture',hostname:'FIXTURE-WIN',os:'windows',architecture:'amd64',installed_version:'1.0.0',desired_version:'1.0.0',state:'active',config_version:1,desired_config_version:1,online:true,last_heartbeat_at:time,heartbeat:{state:'running',queue_depth:0,sources:[{source_id:source,state:'running',events_read:50,events_sent:50,events_drop:0}],components:[{component:'winlogbeat',version:'8.19.0',phase:'idle',state:'running',restarts:0}]}};
const release={id:'windows-1',version:'1.0.0',manifest:{release_id:'windows-1',version:'1.0.0',assets:[{asset_id:'auth.failure',kind:'analysis',version:'1.0.0',sha256:'a'.repeat(64),dependencies:[]}],compatibility:{uim:'1'}},sha256:'a'.repeat(64),state:'staged',created_at:time};
const anomaly={id:'anom:fixture',type:'auth.failure-burst',severity:'high',status:'open',timestamp:time,score:.86,entity:{id:entity,type:'account'},rule_id:'auth.failure',rule_version:'1.0.0',summary:'测试夹具：认证失败聚集',evidence_count:2,reason_codes:['AUTH_FAILURE_BURST'],evidence_event_ids:['evt:fixture']};
const caseData={id:'fixture-case',title:'测试夹具：异常认证调查',description:'隔离浏览验证',status:'in_progress',severity:'high',assignee:'fixture.analyst',version:1,created_at:time,updated_at:time,anomaly_ids:[anomaly.id]};
const exportData={id:'fixture-export',job_id:'fixture-job',dataset:'authentication',format:'csv',query:'search authentication',from:time,to:time,row_limit:100000,byte_limit:1073741824,include_sensitive:false,include_raw:false,state:'succeeded',row_count:1,byte_count:32,file_sha256:'e'.repeat(64),expires_at:new Date(Date.now()+3600000).toISOString(),created_at:time};
const fixture={
 '/me':{subject:'fixture.analyst',organization_id:'fixture-corp',namespace:'fixture',roles:['tenant_admin'],permissions},
 '/overview':{open_anomalies:24,high_risk:3,latest_anomaly:time,generated_at:time,cases:{active_cases:8,unassigned_cases:2,critical_cases:1,closed_today:3}},
 '/catalog':{catalog_version:1,active_generation:'g1',max_time_range_day:31,datasets:['authentication','session','iam','directory','network','dns','web','tls'].map(name=>({name,kind:'uim-domain',active_generation:'g1',index_pattern:'not-for-browser',quality_statuses:['qualified','partial'],fields:[{name:'@timestamp',type:'date',sensitivity:'normal',searchable:true,aggregable:true},{name:'user.name',type:'keyword',sensitivity:'sensitive',searchable:true,aggregable:true}]}))},
 '/sources':{items:[sourceData]},'/collectors':{items:[collector]},'/releases':{items:[release]},'/releases/windows-1':release,'/releases/windows-1/audit':{items:[]},
 '/entities':{items:[entityData],next_cursor:null},['/entities/'+entity]:{...entityData,document:{},recent_attributions:[]},
 ['/entities/'+entity+'/risk']:{entity_id:entity,projection:{risk_score:2.8,contributions:[]},revision:2,operation:'upsert'},
 '/anomalies':{items:[anomaly],next_cursor:null,total:1},['/anomalies/'+anomaly.id]:anomaly,
 ['/anomalies/'+anomaly.id+'/evidence']:{items:[{id:'evt:fixture',timestamp:time,event:{action:'4625',outcome:'failure',dataset:'authentication'},user:{name:'fixture.user'},ueba:{}}],total:1,truncated:false},
 '/cases':{items:[caseData],next_cursor:null,total:1},'/cases/fixture-case':caseData,
 '/analysis/feedback/evaluation':{rows:[]},'/analysis/feedback/metrics':{rules:[]},'/exports':{items:[exportData],next_cursor:''},'/exports/fixture-export':exportData,'/members':{items:[]},'/audit':{items:[],next_cursor:''},
 '/operations/status':{status:'ok',checked_at:time,uptime_seconds:3600,dependencies:[{name:'PostgreSQL',status:'ok',detail:'测试夹具',latency_ms:3}],analysis:{status:'ok',latest_at:time,age_seconds:32,runtime:{checkpoints:[]}},kafka_configured:true},
};
(async()=>{
 const browser=await chromium.launch({headless:true,channel:'msedge'});const page=await browser.newPage({viewport:{width:1440,height:1000}});const errors=[];const writes=[];const checks=[];let downloads=0;
 page.on('pageerror',e=>errors.push(e.message));
 await page.addInitScript(()=>sessionStorage.setItem('tuba.access_token','fixture-only-token'));
 await page.route('**/api/v1/**',async route=>{
  const req=route.request();const path=decodeURIComponent(new URL(req.url()).pathname.replace('/api/v1',''));
  if(path==='/exports/fixture-export/download'){downloads++;exportData.downloaded_at=time;await route.fulfill({status:200,contentType:'text/csv',body:'fixture\n1\n'});return;}
  let body=fixture[path]??{items:[],next_cursor:''};
  if(path==='/query')body={mode:'stats',items:[],total:0,aggregations:{count:{value:0}}};
  if(req.method()!=='GET'){
   writes.push({path,method:req.method(),body:req.postDataJSON(),key:req.headers()['idempotency-key']});
   if(path==='/sources')body={...sourceData,api_key:'FIXTURE-SECRET-ONLY'};
   else if(path==='/collectors/enrollments')body={enrollment_token:'FIXTURE-ENROLLMENT-ONLY',expires_at:time};
   else if(path.endsWith('/activate')){release.state='active';body=release;}
   else if(path==='/releases')body=release;
  }
  await route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(body)});
 });
 const routes=['overview','anomalies','anomalies/'+encodeURIComponent(anomaly.id),'risks','entities','entities/'+encodeURIComponent(entity),'cases','cases/fixture-case','events','sources','sources/'+source,'agents','agents/'+agent,'quality','catalog','analysis','baseline','feedback','releases','releases/windows-1','jobs','exports/fixture-export','jobs/fixture-job','replay','operations','backups','audit','access','publishers'];
 for(const path of routes){await page.goto(base+'/'+path);await page.waitForSelector('.workspace-crumb');await page.locator('.page-loading').waitFor({state:'hidden'});await page.locator('.loading-block').first().waitFor({state:'hidden'});assert.equal(await page.getByText('Something went wrong').count(),0);checks.push(path);}
 await page.goto(base+'/anomalies');
 await page.getByRole('link',{name:'fixture.user',exact:true}).waitFor();
 assert.equal(await page.getByRole('link',{name:'fixture.user',exact:true}).getAttribute('href'),'/entities/'+encodeURIComponent(entity));
 await page.goto(base+'/sources');await page.getByRole('button',{name:'新增来源',exact:true}).click();
 for(const [name,value]of[['厂商','Microsoft'],['产品','Windows'],['数据集','Security']])await page.getByLabel(name,{exact:true}).fill(value);
 await page.getByRole('button',{name:'下一步',exact:true}).click();await page.getByLabel('语义发布包').click();await page.getByText('windows-1 / staged',{exact:true}).click();
 await page.getByRole('button',{name:'下一步',exact:true}).click();assert.equal(writes.filter(x=>x.path==='/sources').length,0);
 await page.getByRole('button',{name:'确认注册',exact:true}).click();await page.getByLabel('一次性凭据').waitFor();assert.equal(writes.filter(x=>x.path==='/sources').length,1);assert.ok(writes.find(x=>x.path==='/sources').key);
 await page.getByRole('button',{name:'已安全保存，关闭'}).click();assert.equal(await page.getByText('FIXTURE-SECRET-ONLY').count(),0);
 await page.goto(base+'/agents');await page.getByRole('button',{name:'注册 Agent',exact:true}).click();await page.locator('.ant-modal-footer .ant-btn-primary').click();await page.getByLabel('一次性凭据').waitFor();await page.getByRole('button',{name:'已安全保存，关闭'}).click();
 await page.goto(base+'/releases/windows-1');await page.getByRole('button',{name:/激\s*活/}).click();for(let i=0;i<2;i++)await page.getByRole('button',{name:'下一步',exact:true}).click();assert.equal(writes.filter(x=>x.path.endsWith('/activate')).length,0);await page.getByRole('button',{name:'确认提交',exact:true}).click();await page.locator('.ant-modal').waitFor({state:'hidden'});assert.equal(writes.filter(x=>x.path.endsWith('/activate')).length,1);
 await page.goto(base+'/entities/'+encodeURIComponent(entity)+'?tab=risk');await page.getByRole('tab',{name:'风险解释'}).waitFor();assert.equal(await page.getByRole('tab',{name:'风险解释'}).getAttribute('aria-selected'),'true');
 await page.goto(base+'/cases');await page.getByRole('tab',{name:'处置看板'}).click();assert.equal(await page.locator('.wb-lane').count(),3);
 await page.goto(base+'/agents/'+agent);await page.getByRole('button',{name:'组件升级 / 回滚',exact:true}).click();
 await page.getByLabel('受管组件',{exact:true}).click();await page.getByText('winlogbeat',{exact:true}).last().click();
 await page.getByLabel('目标版本（需已验签的本地制品）').fill('8.19.1');
 await page.getByLabel('当前完整配置（JSON）').fill(JSON.stringify({sources:[{id:source}],components:{winlogbeat:{desired_version:'8.19.0'},agent:{desired_version:'1.0.0'}}}));
 for(let i=0;i<2;i++)await page.getByRole('button',{name:'下一步',exact:true}).click();
 await page.getByRole('button',{name:'确认提交',exact:true}).click();await page.locator('.ant-modal').waitFor({state:'hidden'});
 const config=writes.find(x=>x.path==='/collectors/'+agent+'/config').body;assert.equal(config.sources[0].id,source);assert.equal(config.components.agent.desired_version,'1.0.0');assert.equal(config.components.winlogbeat.desired_version,'8.19.1');
 await page.goto(base+'/events');await page.getByRole('button',{name:'保存 / 载入查询',exact:true}).click();await page.getByLabel('查询名称').fill('fixture query');await page.getByRole('button',{name:'保存当前查询',exact:true}).click();await page.getByText('fixture query',{exact:true}).waitFor();await page.getByRole('button',{name:'载入',exact:true}).click();
 await page.goto(base+'/exports/fixture-export');await page.getByRole('button',{name:'下载结果',exact:true}).click();assert.equal(downloads,0);const download=page.waitForEvent('download');await page.locator('.ant-popconfirm .ant-btn-primary').click();await download;assert.equal(downloads,1);assert.equal(await page.getByRole('button',{name:'下载结果',exact:true}).isDisabled(),true);
 await page.goto(base+'/overview');await page.getByRole('heading',{name:'安全总览',exact:true}).waitFor();await page.locator('.loading-block').first().waitFor({state:'hidden'});await page.screenshot({path:output+'/overview.png',fullPage:true});await page.goto(base+'/operations');await page.getByRole('heading',{name:'运行与容量',exact:true}).waitFor();await page.screenshot({path:output+'/operations.png',fullPage:true});
 await page.getByRole('tab',{name:'容量与保留'}).click();await page.getByText('容量遥测尚未接入',{exact:true}).waitFor();
 await page.setViewportSize({width:390,height:844});await page.goto(base+'/catalog');await page.screenshot({path:output+'/mobile.png',fullPage:true});
 // Permission guard must not request protected data for a restricted principal.
 const restricted=await browser.newPage();let protectedCalls=0;await restricted.addInitScript(()=>sessionStorage.setItem('tuba.access_token','fixture-viewer-token'));
 await restricted.route('**/api/v1/**',async r=>{if(new URL(r.request().url()).pathname!=='/api/v1/me')protectedCalls++;await r.fulfill({status:200,contentType:'application/json',body:JSON.stringify({...fixture['/me'],roles:['viewer'],permissions:['anomaly:read']})});});
 await restricted.goto(base+'/sources');await restricted.getByText('当前角色无权访问',{exact:true}).waitFor();assert.equal(protectedCalls,0);assert.equal(await restricted.locator('.command-nav').getByText('数据来源',{exact:true}).count(),0);
 const sourceManager=await browser.newPage();let unauthorizedReleaseReads=0;
 await sourceManager.addInitScript(()=>sessionStorage.setItem('tuba.access_token','fixture-source-manager'));
 await sourceManager.route('**/api/v1/**',async r=>{const path=new URL(r.request().url()).pathname.replace('/api/v1','');if(path==='/releases')unauthorizedReleaseReads++;const value=path==='/me'?{...fixture['/me'],permissions:['source:manage']}:fixture[path];await r.fulfill({status:200,contentType:'application/json',body:JSON.stringify(value)});});
 await sourceManager.goto(base+'/');await sourceManager.getByRole('button',{name:'新增来源',exact:true}).waitFor();assert.ok(sourceManager.url().endsWith('/sources'));await sourceManager.getByRole('button',{name:'新增来源',exact:true}).click();for(const [name,value]of[['厂商','Microsoft'],['产品','Windows'],['数据集','Security']])await sourceManager.getByLabel(name,{exact:true}).fill(value);await sourceManager.getByRole('button',{name:'下一步',exact:true}).click();await sourceManager.getByPlaceholder('输入已批准的 active / staged 发布引用').waitFor();assert.equal(unauthorizedReleaseReads,0);
 await page.getByRole('button',{name:'退出登录'}).click();await page.getByRole('heading',{name:'登录工作台'}).waitFor();assert.equal(await page.evaluate(()=>sessionStorage.getItem('tuba.access_token')),null);assert.equal(await page.evaluate(()=>Object.keys(sessionStorage).filter(k=>k.startsWith('tuba.query-bookmarks:')).length),0);await page.setViewportSize({width:1440,height:1000});await page.screenshot({path:output+'/login.png',fullPage:true});
 assert.deepEqual(errors,[]);const result={routes:checks.length,flows:['source-registration','single-use-secret','enrollment','release-activation','risk-deep-link','case-board','component-config-merge','session-query-save','single-use-export-download','capacity-unavailable','route-permissions','source-manager-without-publisher','logout'],pageErrors:errors};fs.writeFileSync(output+'/verification.json',JSON.stringify(result,null,2));console.log(JSON.stringify(result,null,2));await browser.close();
})().catch(e=>{console.error(e);process.exit(1)});
