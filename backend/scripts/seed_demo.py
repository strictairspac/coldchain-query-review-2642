from sqlalchemy import select,text
from src.config.database import Base,engine,Session
from src.models.user import User
from src.models.sample import Sample
from src.models.batch import Batch
from src.models.evidence import Evidence
from src.models.audit import Audit
from src.models.invoice import Invoice
from src.models.meta import Meta
from src.models.correction import Import,ImportItem
from src.utils.password import hash_password
ORIGINAL="SELECT DISTINCT i.customer, (SELECT SUM(j.amount) FROM invoices j WHERE j.customer=i.customer AND j.temperature<=8) AS total FROM invoices i WHERE (SELECT SUM(k.amount) FROM invoices k WHERE k.customer=i.customer AND k.temperature<=8)>100 ORDER BY customer"
def seed():
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS invoices (invoice_id INTEGER PRIMARY KEY, customer TEXT NOT NULL, route TEXT NOT NULL, amount NUMERIC(12,2) NOT NULL, temperature NUMERIC(4,1) NOT NULL, status TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1, voided BOOLEAN NOT NULL DEFAULT FALSE)"))
        # 旧库就地演进：补充更正导入所需的列与证据失效判定字段
        conn.execute(text("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1"))
        conn.execute(text("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS voided BOOLEAN NOT NULL DEFAULT FALSE"))
        conn.execute(text("ALTER TABLE evidence ADD COLUMN IF NOT EXISTS data_version INTEGER"))
        conn.execute(text("ALTER TABLE evidence ADD COLUMN IF NOT EXISTS read_invoice_ids JSON"))
        conn.execute(text("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='query_reader') THEN CREATE ROLE query_reader NOLOGIN; END IF; END $$"))
        conn.execute(text('GRANT SELECT ON invoices TO query_reader'))
        conn.execute(text("INSERT INTO meta(key,value) VALUES ('data_version','1') ON CONFLICT(key) DO NOTHING"))
        conn.execute(text("INSERT INTO invoices SELECT i, (ARRAY['澄港海产','南港鲜食','北湾冷运'])[1+(i%3)], (ARRAY['东线','西线'])[1+(i%2)], 35+(i%17)*7, 2+(i%9), CASE WHEN i%7=0 THEN 'pending' ELSE 'settled' END, 1, FALSE FROM generate_series(1,600) i ON CONFLICT DO NOTHING"))
    with Session() as db:
        for role in ('admin','operator','viewer'):
            email=role+'@chenggang.test'
            if not db.scalar(select(User).where(User.email==email)):db.add(User(email=email,role=role,password_hash=hash_password('ColdChain!17')))
        samples=[('低温结算汇总','09:00 澄港海产、南港鲜食、北湾冷运的温控合格运费，600 行固定账单。',ORIGINAL),('路线平均费用','按路线观察已结算记录的均值；用 ROUND 保持两位小数。',"SELECT route, ROUND(AVG(amount),2) AS average FROM invoices WHERE status='settled' GROUP BY route ORDER BY route"),('异常温控账单','温度大于8度的账单，为差异复核提供反例。','SELECT customer, COUNT(*) AS count FROM invoices WHERE temperature > 8 GROUP BY customer ORDER BY customer')]
        for name,desc,sql in samples:
            if not db.scalar(select(Sample).where(Sample.name==name)):db.add(Sample(name=name,description=desc,original_sql=sql))
        db.commit()
        return {t:db.execute(text('SELECT COUNT(*) FROM '+t)).scalar() for t in ('users','samples','batches','evidence','invoices','imports')}
if __name__=='__main__':print(seed())
