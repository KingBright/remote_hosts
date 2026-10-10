import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import {spawn} from 'node:child_process';
import assert from 'node:assert/strict';
const [root,artifacts]=process.argv.slice(2);
assert(root && artifacts, "usage: node test_personal_access_page.mjs <fixture-pages> <new-evidence-directory>");
assert(!fs.existsSync(artifacts), "evidence directory already exists; retain the original result");
fs.mkdirSync(artifacts,{recursive:true});
const results=[];let browser;let server;let nextRefresh='full';let ws;
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const deadline=async(fn,ms)=>{const started=Date.now();while(Date.now()-started<ms){const x=await fn();if(x)return x;await sleep(100)}throw Error('bounded_wait_expired')};
try {
 server=http.createServer((req,res)=>{
  const url=new URL(req.url,'http://127.0.0.1');
  if(url.pathname==='/status/live.js'){res.setHeader('Content-Type','text/javascript');res.end(fs.readFileSync(path.join(root,'status_live.js')));return}
  const name=url.pathname==='/status'?nextRefresh:path.basename(url.pathname,'.html');
  if(!['full','owner','unknown','offline','exec-disabled','account-readonly','device-readonly'].includes(name)){res.writeHead(404);res.end();return}
  res.setHeader('Content-Type','text/html; charset=utf-8');res.setHeader('Cache-Control','no-store');
  res.end(fs.readFileSync(path.join(root,name+'.html')));
 });
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
 const profile=path.join(artifacts,'isolated-chrome-profile');fs.mkdirSync(profile,{recursive:true});
 const browserLog=fs.openSync(path.join(artifacts,'chrome.log'),'wx');
 browser=spawn('/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',['--headless=new','--remote-debugging-address=127.0.0.1','--remote-debugging-port=0','--user-data-dir='+profile,'--no-first-run','--no-default-browser-check','--disable-background-networking','--disable-component-update','--disable-sync','--disable-extensions','--no-pings','about:blank'],{stdio:['ignore',browserLog,browserLog],detached:true});
 const debugPort=await deadline(()=>{const f=path.join(profile,'DevToolsActivePort');return fs.existsSync(f)?Number(fs.readFileSync(f,'utf8').split('\n')[0]):null},15000);
 const pages=await (await fetch('http://127.0.0.1:'+debugPort+'/json/list')).json();const target=pages.find(p=>p.type==='page');assert(target);
 ws=new WebSocket(target.webSocketDebuggerUrl);await new Promise((resolve,reject)=>{ws.onopen=resolve;ws.onerror=reject});
 let id=0;const requests=new Map();
 ws.onmessage=e=>{const data=JSON.parse(e.data);if(data.id){const r=requests.get(data.id);if(r){requests.delete(data.id);data.error?r.reject(Error(data.error.message)):r.resolve(data.result)}}};
 const call=(method,params={})=>new Promise((resolve,reject)=>{const key=++id;const timer=setTimeout(()=>{requests.delete(key);reject(Error('cdp_timeout:'+method))},10000);requests.set(key,{resolve:v=>{clearTimeout(timer);resolve(v)},reject:e=>{clearTimeout(timer);reject(e)}});ws.send(JSON.stringify({id:key,method,params}))});
 const evaluate=async expression=>{const r=await call('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(r.exceptionDetails)throw Error(r.exceptionDetails.text);return r.result.value};
 await call('Page.enable');await call('Runtime.enable');const version=await call('Browser.getVersion');results.push({browser:version.product});
 const visible="(()=>({text:document.body.innerText,advanced:document.querySelector('#advanced-permissions')?.open,audit:document.querySelector('#operation-audit')?.open,disconnect:document.querySelector('#access-disconnect')?.open,scroll:document.documentElement.scrollWidth,width:innerWidth,grantForms:document.querySelectorAll('input[name=task_id]').length,revision:document.querySelector('#rh-status')?.dataset.revision}))()";
 for(const width of [1280,390,320]){
  await call('Emulation.setDeviceMetricsOverride',{width,height:900,deviceScaleFactor:1,mobile:false});
  for(const scenario of ['full','owner','unknown','offline','exec-disabled','account-readonly','device-readonly']){
   nextRefresh=scenario;await call('Page.navigate',{url:origin+'/'+scenario+'.html'});
   await deadline(async()=>evaluate("location.pathname==="+JSON.stringify("/"+scenario+".html")+"&&document.readyState==='complete'&&document.querySelector('#rh-status')?true:false"),5000);
   const view=await evaluate(visible);
   assert.equal(view.advanced,false);assert.equal(view.audit,false);assert.equal(view.disconnect,false);
   assert.equal(view.grantForms,0);assert(view.scroll<=view.width+1,'horizontal_overflow:'+scenario+':'+width);
   assert(view.text.includes('撤销或断开'));assert(!view.text.includes('code:read'));assert(!view.text.includes('/fixtures/project'));assert(!view.text.includes('限时'));
   const full=view.text.includes('完全访问');assert.equal(full,scenario==='full','full_access_claim:'+scenario+':'+width);
   const png=await call('Page.captureScreenshot',{format:'png',captureBeyondViewport:true});fs.writeFileSync(path.join(artifacts,scenario+'-'+width+'.png'),Buffer.from(png.data,'base64'));
   results.push({scenario,width,default_closed:true,horizontal_overflow:false,full_access_claim:full,visible_text:view.text});
  }
 }
 await call('Emulation.setDeviceMetricsOverride',{width:390,height:900,deviceScaleFactor:1,mobile:false});
 nextRefresh='full';await call('Page.navigate',{url:origin+'/full.html'});await deadline(()=>evaluate("location.pathname==='/full.html'&&document.readyState==='complete'&&document.querySelector('#advanced-permissions')?true:false"),5000);
 const before=await evaluate(visible);await evaluate("document.querySelector('#advanced-permissions summary').click();document.querySelector('#operation-audit summary').click();true");
 nextRefresh='exec-disabled';await deadline(async()=>{const v=await evaluate(visible);return v.revision!==before.revision?v:null},9000);
 const after=await evaluate(visible);assert.equal(after.advanced,true);assert.equal(after.audit,true);assert(!after.text.includes('完全访问'));assert(after.text.includes('执行开关已关闭'));
 const png=await call('Page.captureScreenshot',{format:'png',captureBeyondViewport:true});fs.writeFileSync(path.join(artifacts,'advanced-refresh-390.png'),Buffer.from(png.data,'base64'));
 results.push({scenario:'advanced-refresh',width:390,advanced_preserved:true,audit_preserved:true,full_access_claim:false});
 fs.writeFileSync(path.join(artifacts,'browser-report.json'),JSON.stringify({state:'passed',cases:results,scope:'synthetic local fixtures; no real authentication/grant/revocation or production access'},null,2));
 console.log(JSON.stringify({state:'passed',cases:results.length-1,report:path.join(artifacts,'browser-report.json')}));
} catch(e) {
 fs.writeFileSync(path.join(artifacts,'browser-report.json'),JSON.stringify({state:'failed',error:String(e),cases:results},null,2));
 console.log(JSON.stringify({state:'failed',error:String(e),report:path.join(artifacts,'browser-report.json')}));process.exitCode=1;
} finally {
 if(ws)ws.close();if(browser){try{process.kill(-browser.pid,'SIGTERM')}catch{}}if(server)await new Promise(resolve=>server.close(resolve));
}