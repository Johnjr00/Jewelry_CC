import sys, io, csv
from pathlib import Path
import importlib.util

ROOT = Path(__file__).resolve().parent
DB = ROOT / 'inventory.db'
if DB.exists():
    DB.unlink()

# Import app as real module name so dataclasses don't break
spec = importlib.util.spec_from_file_location('app', str(ROOT/'app.py'))
appmod = importlib.util.module_from_spec(spec)
sys.modules['app'] = appmod
spec.loader.exec_module(appmod)

appmod.init_db()
app = appmod.app
client = app.test_client()


def assert_(cond, msg):
    if not cond:
        raise AssertionError(msg)

# 1) First hit should redirect to setup if no users
r = client.get('/', follow_redirects=False)
assert_(r.status_code in (301,302), f"expected redirect, got {r.status_code}")
assert_('/setup' in r.headers.get('Location',''), f"expected /setup redirect, got {r.headers.get('Location')}")

# 2) Setup first admin user
r = client.post('/setup', data={'username':'admin','password':'password123'}, follow_redirects=False)
assert_(r.status_code in (301,302), f"setup post should redirect, got {r.status_code}")

# 3) Login
r = client.post('/login', data={'username':'admin','password':'password123'}, follow_redirects=True)
assert_(r.status_code == 200, f"login should land on 200, got {r.status_code}")

# 4) Create physical case 01
r = client.post('/cases/new', data={'case_code':'01','case_name':'Front Left'}, follow_redirects=True)
assert_(r.status_code == 200, f"create case should 200, got {r.status_code}")

# 5) Receive items into NEW-RECEIPTS (Ring, UPC 111 qty2)
r = client.post('/receive', data={'item_type':'Ring','description':'Gold Ring','upcs':'111,2'}, follow_redirects=True)
assert_(r.status_code == 200, f"receive should 200, got {r.status_code}")

# 6) API list NEW-RECEIPTS items
r = client.get('/api/case/NEW-RECEIPTS/items')
assert_(r.status_code == 200, f"api items should 200, got {r.status_code}")
js = r.get_json()
assert_(js.get('ok') is True, f"api ok false: {js}")
assert_(any(it['upc']=='111' and int(it['qty'])==2 for it in js['items']), f"expected qty2 in new receipts, got {js['items']}")

# 7) Move 1 from NEW-RECEIPTS to 01
r = client.post('/move', data={'from_case_code':'NEW-RECEIPTS','to_case_code':'01','description':'','upcs':'111,1','upcs_picked':''}, follow_redirects=True)
assert_(r.status_code == 200, f"move should 200, got {r.status_code}")

# 8) Verify case 01 has qty1
r = client.get('/api/case/01/items')
js = r.get_json(); assert_(js.get('ok'), f"api case 01 not ok: {js}")
assert_(any(it['upc']=='111' and int(it['qty'])==1 for it in js['items']), f"expected qty1 in case 01, got {js['items']}")

# 9) Sell 1 from case 01 WITH required sold fields
sell_data = {
    'case_code':'01',
    'upcs':'111,1',
    'trans_reg':'T123/R5',
    'dept_no':'34',
    'brief_desc':'Gold Ring',
    'ticket_price':'199.99',
    'diamond_test':'NRT'
}
r = client.post('/cases/01/sell_out', data=sell_data, follow_redirects=True)
assert_(r.status_code == 200, f"sell_out should 200, got {r.status_code}")

# 10) Verify case 01 empty now
r = client.get('/api/case/01/items')
js = r.get_json(); assert_(js.get('ok'), f"api case 01 not ok: {js}")
assert_(len(js['items'])==0, f"expected case 01 empty after sale, got {js['items']}")

# 11) Receive another then move then mark missing
client.post('/receive', data={'item_type':'Ring','description':'Gold Ring','upcs':'111,1'}, follow_redirects=True)
client.post('/move', data={'from_case_code':'NEW-RECEIPTS','to_case_code':'01','description':'','upcs':'111,1','upcs_picked':''}, follow_redirects=True)
r = client.post('/cases/01/missing_out', data={'upcs':'111,1'}, follow_redirects=True)
assert_(r.status_code == 200, f"missing_out should 200, got {r.status_code}")

# 12) Counts page and count submission
r = client.get('/counts')
assert_(r.status_code == 200, f"counts page should 200, got {r.status_code}")
r = client.post('/counts/01', data={'bracelets':0,'rings':0,'earrings':0,'necklaces':0,'other':0,'notes':'test'}, follow_redirects=True)
assert_(r.status_code == 200, f"count_case post should 200, got {r.status_code}")

# 13) History page loads and export history csv works
r = client.get('/history')
assert_(r.status_code == 200, f"history should 200, got {r.status_code}")
r = client.get('/export/history.csv')
assert_(r.status_code == 200, f"export history should 200, got {r.status_code}")
assert_('text/csv' in r.headers.get('Content-Type',''), 'history export should be csv')

# 14) Export inventory csv + export case csv
r = client.get('/export/inventory.csv')
assert_(r.status_code == 200, f"export inventory should 200, got {r.status_code}")
r = client.get('/export/case/01.csv')
assert_(r.status_code == 200, f"export case should 200, got {r.status_code}")

# 15) Daily reports endpoints
r = client.get('/reports/daily')
assert_(r.status_code == 200, f"daily reports page should 200, got {r.status_code}")
r = client.get('/reports/daily/01.xlsx')
assert_(r.status_code == 200, f"daily report download should 200, got {r.status_code}")
assert_('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in r.headers.get('Content-Type',''), 'daily report should be xlsx')

# 16) Users admin page + create staff user
r = client.get('/admin/users')
assert_(r.status_code == 200, f"users admin page should 200, got {r.status_code}")
r = client.post('/admin/users', data={'username':'staff1','password':'password123','role':'staff'}, follow_redirects=True)
assert_(r.status_code == 200, f"create staff user should 200, got {r.status_code}")

print('SMOKE_TEST_OK')
