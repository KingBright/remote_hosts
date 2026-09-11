"""Record actual 0.4.1 outcomes. No installation, retries or new acceptance runs."""
import hashlib,json,pathlib,shutil,sys,tarfile,time
R=pathlib.Path('/Users/jinliang/Workspace/remote_hosts');D=R/'docs/releases/0.4.1'
sys.path.insert(0,str(R/'dist/remote-hosts-code-0.4.1'));import release_receipts as rr
load=lambda p:json.loads(p.read_text())
def save(path,data):
 if path.exists():assert load(path)==data,'retained evidence conflict'
 else:rr.atomic_json(path,data)
plan=rr.verified_build(R/'target/iteration-041-r2/pipeline.json','0.4.1')
v=load(D/'verification.json');p=load(D/'pipeline.json');old=load(D/'deployment-r1.json')
assert plan['tests']['passed']==312 and old['state']=='needs_recovery' and not old['targets_accepted']
mac=load(D/'macbook-runtime-proof.json');nas=load(D/'nas-updater.json');ctx=load(D/'context-readonly-macbook.json')
assert mac['updater']['state']==nas['state']=='upgraded' and mac['version']==nas['version']=='0.4.1'
assert ctx['state']=='passed' and ctx['temporary_oauth_revoked'] is True
studio={'version':'0.4.1','candidate_sha256':plan['artifacts']['remote-hosts-code-macos-arm64']['sha256'],
 'state':'failed','phase':'waiting_idle','gateway_preflight':{'attempts':1,'elapsed_ms':1539},'service_changed':False,
 'error':'agent still has active work; upgrade not applied',
 'attempt_receipt':'/Users/jinliang/.local/share/remote-hosts-code/releases/upgrade-jobs/be0211f82f705d0c84ce27f8/result-attempts/78ac5f9202b5412f9e2c2577f447fa47.json'}
save(D/'studio-updater-failed.json',{'source':'native terminal_read for original read-only observer','terminal_id':'6b51e845-6219-4729-9e3b-69c4b399ae16','updater':studio})
source=pathlib.Path(p['snapshot']);checkout=pathlib.Path(p['execution_root']);archive=D/'verified-source.tar.gz'
assert all(rr.digest(source/n)==h for n,h in v['source_inputs'].items())
if not archive.exists():
 with tarfile.open(archive,'x:gz') as t:
  for n in sorted(v['source_inputs']):t.add(source/n,arcname=n)
  t.add(source/'source-snapshot.json',arcname='source-snapshot.json')
with tarfile.open(archive,'r:gz') as t:
 assert set(t.getnames())==set(v['source_inputs'])|{'source-snapshot.json'}
 for n,h in v['source_inputs'].items():assert hashlib.sha256(t.extractfile(n).read()).hexdigest()==h
save(D/'source-archive.json',{'version':'0.4.1','snapshot_id':p['snapshot_id'],'files':len(v['source_inputs']),'sha256':rr.digest(archive),'path':str(archive.relative_to(R))})
logs=D/'logs';logs.mkdir(exist_ok=True)
for name,gate in v['checks'].items():
 original=checkout/gate['log'];assert rr.digest(original)==gate['sha256'];dest=logs/(name+'.log')
 if not dest.exists():shutil.copyfile(original,dest)
 assert rr.digest(dest)==gate['sha256']
for name in ('publish.py','live_features.py','live_parallel.py','record_status.py'):
 original=R/'target/iteration-041-r1'/name;dest=D/name
 if not dest.exists():shutil.copyfile(original,dest)
 assert rr.digest(dest)==rr.digest(original)
incidents={'version':'0.4.1','items':[
 {'kind':'host_pre_dispatch_block','key':'rh041-negative-test-run-r1','execution_started':False,'scope':'old implementation baseline','note':'No claim of old-code failing execution. Changed candidate later passed complete fixed-input checks.'},
 {'kind':'edit_preflight_conflict','operation_id':'83890663-5211-457f-811a-dbbb74d2990f','changed':False,'resolution':'Used unique contextual anchors for two transaction statements.'},
 {'kind':'macos_test_path_alias','initial_verification':'target/iteration-041-r1/pipeline-logs/verification.json','passed':311,'failed':1,'resolution':'Expected path normalized using resolve; no product identity checks weakened. Final r2 complete run passed312.'},
 {'kind':'host_pre_dispatch_block','key':'rh041-studio-pre-cutover-state-r1','execution_started':False,'scope':'Studio active-work metadata read'},
 {'kind':'studio_busy','evidence':'docs/releases/0.4.1/studio-updater-failed.json','service_changed':False,'handling':'Other work not cancelled; no forced installation or duplicate request.'},
 {'kind':'host_pre_dispatch_block','key':'rh041-macbook-full-live-acceptance-r1','execution_started':False,'scope':'standard code/write/terminal/file acceptance','handling':'Not repackaged or rerouted. Separate read-only context paging test does not cover this missing gate.'}
]}
save(D/'incidents.json',incidents)
summary={'version':'0.4.1','state':'partially_deployed_acceptance_incomplete','all_requested_targets_accepted':False,
 'manifest_sha256':plan['manifest_sha256'],'source_verification':'docs/releases/0.4.1/verification.json','tests':plan['tests'],
 'nas':{'state':'upgraded','version':'0.4.1','pid':nas['pid'],'sha256':nas['installed_sha256'],'evidence':'docs/releases/0.4.1/nas-updater.json'},
 'macbook':{'state':'upgraded_readiness_and_context_verified','version':'0.4.1','pid':mac['updater']['pid'],'sha256':mac['installed_sha256'],'evidence':'docs/releases/0.4.1/macbook-runtime-proof.json'},
 'studio':{'state':'busy_upgrade_not_applied','running_version':'0.4.0','candidate_version':'0.4.1','candidate_staged':True,'service_changed':False,'evidence':'docs/releases/0.4.1/studio-updater-failed.json'},
 'acceptance':{'macbook_context_readonly':'passed','standard_macbook':'blocked_before_execution','both_device_standard':'not_run','new_transfer_and_range_live':'not_run','parallel_exports_live':'not_run'},
 'source_archive':'docs/releases/0.4.1/verified-source.tar.gz','source_archive_sha256':rr.digest(archive),
 'publisher_final':'docs/releases/0.4.1/deployment-r1.json','publisher_ended':True,'temporary_oauth_revoked':old['temporary_oauth_revoked'] and ctx['temporary_oauth_revoked'],
 'next_actions':['Retain already deployed NAS/MacBook; do not reinstall to repeat acceptance.','Observe original Studio busy outcome, preserve other work and start a separate bounded installation attempt only after idle is established.','Complete the explicit MacBook and both-device standard gates through a normally authorized host operation; do not infer success from read-only context checks.'],
 'background_work_scheduled':False,'git_commit_or_push':False}
save(D/'deployment.json',summary)
path=R/'docs/product/backlog.json';before=path.read_bytes();b=json.loads(before);backup=D/'backlog-before-release.json'
if not backup.exists():backup.write_bytes(before)
b['updated_at']='2026-09-11';b['release_policy'].update(current_candidate='0.4.1',last_observed_agents='NAS and MacBook running0.4.1; Studio remains0.4.0 because its updater rejected active work before installation',publication_evidence='docs/releases/0.4.1/deployment.json',current_candidate_evidence='docs/releases/0.4.1/verification.json',candidate_validation='312 tests passed on136 fixed inputs; both release builds passed. NAS/MacBook upgraded; MacBook readiness and read-only context paging accepted. Standard live acceptance blocked before execution; all-target release is not accepted.',selected_release_targets=['NAS gateway','MacBook-M2-Max','Mac-Studio'])
by={x['id']:x for x in b['items']}
updates={
 'RH-009':'0.4.1短控制事务提前取得写意向；8路同幂等恢复只形成一个代际，避免读转写SQLITE_BUSY。未来代际回执不再被当作过期成功确认，缺少代际结果不能覆盖新尝试。全回归通过，NAS/MacBook已安装，Studio未安装。',
 'RH-010':'0.4.1精确恢复重试允许刷新同代授权URL但不创建新任务/代际；已取消/已完成或被后代取代时不覆盖新授权。新旧授权隔离测试通过；综合现场门禁未运行，不能据此关闭。',
 'RH-014':'修复取消结果被外层写成completed进度，候选回归通过并随NAS/MacBook发布。confirmed_bytes变化推进游标在0.4.0已存在，本轮仅补回归。取消现场验收本次未完成。',
 'RH-015':'0.4.1增加活跃过滤、状态计数、终端keyset分页及绑定工作区/过滤器的游标。MacBook只读现场3页6个唯一终端ID、active_only和汇总通过，授权已撤销；不是完整Git/对话上下文或事件日志。',
 'RH-040':'0.4.1恢复控制短事务BEGIN IMMEDIATE避免读转写锁升级竞争，8路相同动作回归无重复效果。泛化数据库热点与满负载指标仍保留。',
 'RH-047':'正式包纳入已完成构建与产物检查、双方原回执有界等待和持久阶段日志，14项模块回归通过。实际发布NAS/MacBook成功；Studio因其他工作忙在安装前退出。发布编排先等所有设备版本再检查回执导致额外等待，并挡住健康目标的后续自动验收；需按目标独立推进、及早观察失败且不让观察器成为idle阻塞。综合验收另受宿主拦截，不误报整版完成。'}
for key,text in updates.items():
 x=by[key];x.update(status='partial',current_resolution=text,target_version='0.4.1' if key!='RH-047' else '0.4.2',last_reviewed='2026-09-11')
 for e in ['docs/releases/0.4.1/verification.json','docs/releases/0.4.1/deployment.json','docs/releases/0.4.1/incidents.json']:
  if e not in x['evidence']:x['evidence'].append(e)
by['RH-015']['evidence'].append('docs/releases/0.4.1/context-readonly-macbook.json')
by['RH-007']['current_resolution']+=' 0.4.1补充网关If-Range条件回退及后缀范围实现，回归通过；该版对应现场范围测试尚未执行。'
if not any(x['version']=='0.4.1' for x in b['milestones']):b['milestones'].append({'version':'0.4.1','goal':'恢复控制、授权刷新与回执代际、准确取消进度、范围下载、工作区分页、发布输入/原回执门禁'})
assert path.read_bytes()==before,'concurrent product edit; reconcile separately'
rr.atomic_json(path,b)
print(json.dumps({'state':summary['state'],'version':'0.4.1','tests':plan['tests'],'issues':len(b['items']),'archive_sha256':rr.digest(archive),'standard_acceptance_completed':False}),flush=True)
