#!/usr/bin/env python3
"""账单更正导入端到端实测：乱序、整批拒绝、重试幂等、作废重放、证据失效、
在途复核撞上更正、交叉并发、权限与可查性。直连数据库断言最终账单状态。"""
import json,time,threading,urllib.request,urllib.error,sys
import psycopg2
BASE='http://127.0.0.1:19240'
DB=dict(host='127.0.0.1',port=17740,user='app',dbname='query_review')
FAILURES=[]
def check(name,cond,detail=''):
    line=f"{'PASS' if cond else 'FAIL'}  {name}"+(f'  | {detail}' if detail and not cond else '')
    print(line,flush=True)
    if not cond:FAILURES.append(name+((' | '+str(detail)) if detail else ''))
def api(path,token=None,method='GET',body=None):
    if body is not None and method=='GET':method='POST'
    req=urllib.request.Request(BASE+path,method=method)
    if token:req.add_header('Authorization','Bearer '+token)
    data=None
    if body is not None:
        req.add_header('Content-Type','application/json');data=json.dumps(body).encode()
    try:
        with urllib.request.urlopen(req,data) as r:return r.status,json.loads(r.read())
    except urllib.error.HTTPError as e:
        raw=e.read() or b'{}'
        try:return e.code,json.loads(raw)
        except Exception:return e.code,{'detail':raw.decode(errors='replace')[:300]}
def dbq(sql,args=None):
    with psycopg2.connect(**DB) as c:
        with c.cursor() as cur:cur.execute(sql,args);return cur.fetchall()
def invoice(iid):
    rows=dbq('SELECT version,amount,voided,customer,status FROM invoices WHERE invoice_id=%s',(iid,))
    return rows[0] if rows else None
def data_version():return int(dbq("SELECT value FROM meta WHERE key='data_version'")[0][0])
def csv(rows):return 'invoice_id,version,customer,route,amount,temperature,status,void\n'+'\n'.join(','.join(map(str,r)) for r in rows)
def import_file(token,name,rows):
    return api('/imports',token,'POST',{'file_name':name,'content':csv(rows)})
def wait_batch(token,bid,timeout=40):
    end=time.time()+timeout
    while time.time()<end:
        _,rows=api('/batches',token)
        b=next((x for x in rows if x['id']==bid),None)
        if b and b['status'] not in ('queued','running'):return b
        time.sleep(0.4)
    raise TimeoutError(f'batch {bid} not terminal')
def login(email):return api('/auth/login',body={'email':email,'password':'ColdChain!17'})[1]['token']
admin=login('admin@chenggang.test');operator=login('operator@chenggang.test');viewer=login('viewer@chenggang.test')
print('== 登录成功，三种角色令牌就绪 ==',flush=True)
ts=int(time.time())
# ---------- 1. 乱序：同号仅采用新版本 ----------
s,r=import_file(operator,f'{ts}-A-v2.csv',[(10,2,'南港鲜食','东线','999.00','4.0','settled','false')])
check('导入A(bill10 v2)生效',s==201 and r['status']=='applied' and r['applied_rows']==1,r)
inv=invoice(10);check('bill10 现为 v2/999',inv[0]==2 and float(inv[1])==999.0,inv)
s,r=import_file(operator,f'{ts}-B-v1.csv',[(10,1,'南港鲜食','东线','111.00','4.0','settled','false')])
check('乱序旧版 v1 被跳过',r['status']=='applied' and r['applied_rows']==0 and r['skipped_rows']==1,r)
inv=invoice(10);check('乱序后 bill10 仍 v2/999（不丢新值）',inv[0]==2 and float(inv[1])==999.0,inv)
# ---------- 2. 同版本异内容整批拒绝 + 原子性 ----------
before11=invoice(11);dv0=data_version()
s,r=import_file(operator,f'{ts}-C-conflict.csv',[(10,2,'南港鲜食','东线','555.00','4.0','settled','false'),(11,3,'澄港海产','西线','222.00','5.0','settled','false')])
check('同版本异内容整批拒绝',r['status']=='rejected' and '冲突' in (r['reject_reason'] or ''),r)
check('整批回滚：bill11 未被写入',invoice(11)==before11,(invoice(11),before11))
check('整批回滚：bill10 保持 v2/999',invoice(10)[0]==2 and float(invoice(10)[1])==999.0,invoice(10))
check('拒绝批不推进数据版本',data_version()==dv0,(data_version(),dv0))
s,detail=api(f"/imports/{r['id']}",viewer)
check('拒绝原因与逐行明细可查',s==200 and detail['status']=='rejected' and all(i['disposition']=='rejected' for i in detail['items']),detail.get('reject_reason'))
rejected_id=r['id']
# ---------- 3. 重试不重复写账 ----------
dv0=data_version()
s,r2=import_file(operator,f'{ts}-A-retry.csv',[(10,2,'南港鲜食','东线','999.00','4.0','settled','false')])
first_a=[x for x in api('/imports',viewer)[1] if x['file_name']==f'{ts}-A-v2.csv'][0]
check('相同文件重试命中首个导入记录且不重复写账',r2.get('replayed') is True and r2['id']==first_a['id'],(r2,first_a['id']))
check('重试不推进数据版本',data_version()==dv0,(data_version(),dv0))
check('重试后 bill10 仍 v2/999',invoice(10)[0]==2 and float(invoice(10)[1])==999.0,invoice(10))
# ---------- 4. 作废终态与作废重放 ----------
s,r=import_file(operator,f'{ts}-D-void.csv',[(12,2,'北湾冷运','东线','41.00','5.0','settled','true')])
check('作废导入生效',r['status']=='applied' and r['applied_rows']==1,r)
inv=invoice(12);check('bill12 已作废 v2',inv[0]==2 and inv[2] is True,inv)
dv0=data_version()
s,r2=import_file(operator,f'{ts}-D-void-replay.csv',[(12,2,'北湾冷运','东线','41.00','5.0','settled','true')])
check('作废文件重放幂等',r2.get('replayed') is True and data_version()==dv0,(r2,data_version(),dv0))
s,r=import_file(operator,f'{ts}-E-oldercreate.csv',[(12,1,'北湾冷运','东线','40.00','5.0','settled','false')])
check('作废后旧版本不复活',r['skipped_rows']==1 and invoice(12)[2] is True and invoice(12)[0]==2,(r,invoice(12)))
s,r=import_file(operator,f'{ts}-E2-newer.csv',[(12,3,'北湾冷运','东线','45.00','5.0','settled','false')])
check('作废后更高版本也不复活（终态）',r['skipped_rows']==1 and invoice(12)[2] is True and invoice(12)[0]==2,(r,invoice(12)))
s,r=import_file(operator,f'{ts}-F-tombstone.csv',[(9001,2,'澄港海产','东线','10.00','3.0','pending','true')])
check('不存在账单的作废留下墓碑',r['applied_rows']==1 and invoice(9001)[2] is True,(r,invoice(9001)))
s,r=import_file(operator,f'{ts}-G-after-tomb.csv',[(9001,1,'澄港海产','东线','10.00','3.0','pending','false')])
check('墓碑阻止旧账补入',r['skipped_rows']==1 and invoice(9001)[2] is True,(r,invoice(9001)))
# ---------- 5. 证据失效：受影响失效，未受影响保持有效 ----------
s,s1=api('/samples',operator,'POST',{'name':f'未结算汇总-{ts}','description':'pending 口径','original_sql':"SELECT customer, SUM(amount) AS total FROM invoices WHERE status='pending' GROUP BY customer ORDER BY customer"})
check('登记 pending 样本',s in (200,201),s1)
sid=s1['id'] if isinstance(s1,dict) and 'id' in s1 else api('/samples',viewer)[1][-1]['id']
s,b1=api('/batches',operator,'POST',{'name':f'e1-settled-{ts}','sample_id':2,'candidate_sql':"SELECT route, ROUND(AVG(amount),2) AS average FROM invoices WHERE status='settled' GROUP BY route ORDER BY route"})
s,b2=api('/batches',operator,'POST',{'name':f'e2-pending-{ts}','sample_id':sid,'candidate_sql':"SELECT customer, SUM(amount) AS total FROM invoices WHERE status='pending' GROUP BY customer ORDER BY customer"})
b1=wait_batch(operator,b1['id']);b2=wait_batch(operator,b2['id'])
check('两个基线批次均通过',b1['status']=='passed' and b2['status']=='passed',(b1['status'],b2['status']))
_,e1=api(f"/evidence/{b1['id']}",viewer);_,e2=api(f"/evidence/{b2['id']}",viewer)
check('更正前证据均有效',e1['stale'] is False and e2['stale'] is False,(e1['stale'],e2['stale']))
s,rH=import_file(operator,f'{ts}-H-pending.csv',[(7,2,'澄港海产','西线','77.00','3.0','pending','false')])
check('触及 pending 账单的更正生效',rH['status']=='applied',rH)
_,e1b=api(f"/evidence/{b1['id']}",viewer);_,e2b=api(f"/evidence/{b2['id']}",viewer)
check('受影响证据(pending口径)失效',e2b['stale'] is True and rH['id'] in e2b['invalidated_by'],e2b.get('invalidated_by'))
check('未受影响证据(settled口径)保持有效',e1b['stale'] is False,e1b.get('invalidated_by'))
s,rI=import_file(operator,f'{ts}-I-settled.csv',[(8,2,'南港鲜食','西线','88.00','6.0','settled','false')])
_,e1c=api(f"/evidence/{b1['id']}",viewer)
check('settled 更正后该证据也失效',e1c['stale'] is True and rI['id'] in e1c['invalidated_by'],e1c.get('invalidated_by'))
# ---------- 6. 在途复核撞上更正：不能发布有效通过；长复核不拖住更正 ----------
s,hv=api('/samples',operator,'POST',{'name':f'重查询-{ts}','description':'自连接放大执行窗口','original_sql':'SELECT COUNT(*) AS pairs FROM invoices a, invoices b WHERE a.temperature <= b.temperature AND b.amount >= a.amount'})
hid=hv['id'] if isinstance(hv,dict) and 'id' in hv else [x for x in api('/samples',viewer)[1] if x['name']==f'重查询-{ts}'][0]['id']
s,b3=api('/batches',operator,'POST',{'name':f'collision-{ts}','sample_id':hid,'candidate_sql':'SELECT COUNT(*) AS pairs FROM invoices a, invoices b WHERE a.temperature <= b.temperature AND b.amount >= a.amount'})
stop=threading.Event();lat=[];imp_errors=[]
def spam():
    v=100
    while not stop.is_set():
        t0=time.time()
        try:
            st,rr=import_file(operator,f'{ts}-spam-{v}.csv',[(20,v,'澄港海产','东线',f'{v}.00','4.0','settled','false')])
            lat.append(time.time()-t0)
            if rr.get('status')!='applied':imp_errors.append(rr)
        except Exception as exc:imp_errors.append(str(exc))
        v+=1;time.sleep(0.02)
th=threading.Thread(target=spam);th.start()
b3=wait_batch(operator,b3['id'],timeout=60);stop.set();th.join()
_,e3=api(f"/evidence/{b3['id']}",viewer)
not_valid_pass = not (b3['status']=='passed' and e3.get('stale') is False)
check('撞上更正的在途复核未发布有效通过',not_valid_pass,f"status={b3['status']} stale={e3.get('stale')}")
check('碰撞批次留有可查证据',e3.get('batch_id')==b3['id'] and 'result' in e3,e3.get('id'))
check('更正导入全部生效（长复核不拖住更正）',len(imp_errors)==0 and len(lat)>=3,(imp_errors[:2],len(lat)))
check('更正延迟不受长复核影响(<2s)',lat and max(lat)<2.0,max(lat) if lat else None)
inv=invoice(20);check('连番更正后 bill20 采用最高版本',inv[0]==100+len(lat)-1,(inv,len(lat)))
# ---------- 7. 交叉并发更正不丢新值 ----------
def concurrent_round(bx,by,tag):
    dv0=data_version()
    f1=[(bx,10,'澄港海产','东线','1000.00','4.0','settled','false'),(by,10,'南港鲜食','西线','2000.00','5.0','pending','false')]
    f2=[(bx,12,'澄港海产','东线','1200.00','4.0','settled','false'),(by,9,'南港鲜食','西线','900.00','5.0','pending','false')]
    res=[]
    def fire(rows,name):res.append(import_file(operator,name,rows))
    t1=threading.Thread(target=fire,args=(f1,f'{ts}-{tag}-1.csv'));t2=threading.Thread(target=fire,args=(f2,f'{ts}-{tag}-2.csv'))
    t1.start();t2.start();t1.join();t2.join()
    ok=all(r.get('status')=='applied' for _,r in res)
    ix,iy=invoice(bx),invoice(by)
    return ok and ix[0]==12 and float(ix[1])==1200.0 and iy[0]==10 and float(iy[1])==2000.0 and data_version()==dv0+2,(res,ix,iy,data_version(),dv0)
for k,(bx,by) in enumerate([(30,31),(32,33),(34,35)]):
    ok,detail=concurrent_round(bx,by,f'cc{k}')
    check(f'交叉并发第{k+1}轮：高版本生效不丢新值',ok,detail)
dv0=data_version();same=[(40,2,'澄港海产','东线','500.00','4.0','settled','false')]
res=[]
def fire_same():res.append(import_file(operator,f'{ts}-same.csv',same))
t1=threading.Thread(target=fire_same);t2=threading.Thread(target=fire_same);t1.start();t2.start();t1.join();t2.join()
ids={r[1]['id'] for r in res}
check('相同文件并发导入只写一次',len(ids)==1 and data_version()==dv0+1 and invoice(40)[0]==2,(res,data_version(),dv0))
# ---------- 8. 权限与查询限制 ----------
s,r=import_file(viewer,f'{ts}-viewer.csv',[(50,2,'澄港海产','东线','1.00','1.0','pending','false')])
check('观察员导入被拒绝(403)',s==403,(s,r))
s,r=api('/imports',viewer);check('观察员可查导入列表',s==200 and isinstance(r,list),s)
s,r=api(f'/imports/{rejected_id}',viewer)
check('观察员可查拒绝明细',s==200 and r['status']=='rejected',s)
# ---------- 9.  malformed 文件留档可查 ----------
s,r=api('/imports',operator,'POST',{'file_name':f'{ts}-bad.csv','content':'invoice_id,version\n1,2'})
check('缺列文件整批拒绝并留档',r['status']=='rejected' and '缺少必需列' in (r['reject_reason'] or ''),r)
s,lst=api('/imports',viewer)
check('导入列表包含全部批次(生效+拒绝)',s==200 and any(x['id']==rejected_id for x in lst) and any(x['status']=='applied' for x in lst),len(lst) if isinstance(lst,list) else lst)
# ---------- 10. 文件内重复与多版本递增 ----------
dup_same=[(60,2,'澄港海产','东线','10.00','3.0','pending','false'),(60,2,'澄港海产','东线','10.00','3.0','pending','false')]
s,r=import_file(operator,f'{ts}-dup-same.csv',dup_same)
check('文件内同版本同内容去重后生效',r['status']=='applied' and r['total_rows']==1 and invoice(60)[0]==2,(r,invoice(60)))
dup_diff=[(61,2,'澄港海产','东线','10.00','3.0','pending','false'),(61,2,'澄港海产','东线','99.00','3.0','pending','false')]
s,r=import_file(operator,f'{ts}-dup-diff.csv',dup_diff)
check('文件内同版本异内容整批拒绝',r['status']=='rejected' and '整批拒绝' in (r['reject_reason'] or '') and invoice(61)[0]==1,(r,invoice(61)))
multi=[(62,2,'澄港海产','东线','20.00','3.0','pending','false'),(62,4,'澄港海产','东线','40.00','3.0','pending','false'),(62,3,'澄港海产','东线','30.00','3.0','pending','false')]
s,r=import_file(operator,f'{ts}-multi.csv',multi)
inv=invoice(62)
check('文件内乱序多版本最终采用最高版',r['status']=='applied' and inv[0]==4 and float(inv[1])==40.0,(r,inv))
s,r=import_file(admin,f'{ts}-admin.csv',[(63,2,'澄港海产','东线','10.00','3.0','pending','false')])
check('管理员可导入',s==201 and r['status']=='applied',(s,r))
print(flush=True)
if FAILURES:
    print(f'==== {len(FAILURES)} 项失败 ====')
    for f in FAILURES:print(' -',f)
    sys.exit(1)
print('==== 全部通过 ====')
