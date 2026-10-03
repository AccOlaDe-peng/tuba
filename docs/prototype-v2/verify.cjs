const {chromium}=require('playwright');
const fs=require('fs');
(async()=>{
 const browser=await chromium.launch({headless:true,channel:'msedge'});
 const page=await browser.newPage({viewport:{width:1440,height:1000}});
 const errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto('http://127.0.0.1:8766/');
 const routes=await page.evaluate(()=>Object.keys(pages));
 let tabs=0;
 for(const route of routes){
  await page.goto('http://127.0.0.1:8766/#/'+route);
  await page.waitForTimeout(60);
  if(!await page.locator('h1').count())errors.push('Missing heading '+route);
  const keys=await page.locator('[data-tab]').evaluateAll(xs=>xs.map(x=>x.dataset.tab));
  for(const key of keys){await page.locator(`[data-tab="${key}"]`).first().click();tabs++;}
 }
 await page.goto('http://127.0.0.1:8766/#/anomaly');
 await page.locator('[data-action="create-case"]').click();
 await page.locator('[data-action="save-case"]').click();
 if(!await page.evaluate(()=>state.created))errors.push('Case creation failed');
 await page.goto('http://127.0.0.1:8766/#/sources');
 await page.locator('#role').selectOption('platform');
 const flows=['new-source','replay','train','export','upgrade','restore','activate'];
 for(const flow of flows){
  await page.evaluate(k=>startWizard(k),flow);
  for(let i=0;i<3;i++)await page.locator('[data-action="wizard-next"]').click();
  if(await page.locator('[role="dialog"]').count())errors.push('Wizard failed '+flow);
 }
 let workflows=0;
 for(const flow of await page.evaluate(()=>Object.keys(workflowDefinitions))){
  await page.evaluate(k=>workflowAction(k),flow);
  await page.locator('[name="reason"]').fill('原型评审 / DEMO approval');
  await page.locator('[data-action="wf-save"]').click();workflows++;
 }
 await page.goto('http://127.0.0.1:8766/#/overview');
 await page.evaluate(()=>document.querySelector('#toast').style.display='none');
 await page.screenshot({path:'docs/prototype-v2/overview.png',fullPage:true});
 await page.goto('http://127.0.0.1:8766/#/anomaly');
 await page.screenshot({path:'docs/prototype-v2/investigation.png',fullPage:true});
 await page.setViewportSize({width:390,height:844});
 await page.goto('http://127.0.0.1:8766/#/overview');
 await page.screenshot({path:'docs/prototype-v2/mobile.png',fullPage:true});
 console.log(JSON.stringify({routes:routes.length,tabs,flows:flows.length,workflows,errors},null,2));
 fs.writeFileSync('docs/prototype-v2/verification.json',JSON.stringify({routes:routes.length,tabs,flows:flows.length,workflows,errors},null,2));
 await browser.close();if(errors.length)process.exitCode=1;
})();
