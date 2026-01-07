# Jewelry Inventory (Case-Based)

A mobile-first, web-based inventory tracker for a jewelry store with:
- Case grid picker (01–30 style)
- Virtual **New Receipts** case
- UPC-based quantities (UPCs can repeat)
- Required Item Type on receive (Earring/Ring/Necklace/Bracelet)
- Move / Sell / Missing actions (bulk scanning supported)
- Case history logging + CSV exports
- User logins + roles (admin/staff)
- Admin case rename (Edit Case)

## Quick start (Windows / macOS / Linux)

### 1) Install Python 3.10+
Download from python.org, or use your OS package manager.

### 2) Unzip this project, then open a terminal in the folder

You should see:
- app.py
- requirements.txt
- templates/

### 3) Create a virtual environment

**Windows (PowerShell):**
```powershell
python -m venv venv
venv\Scripts\Activate.ps1
```

**macOS/Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### 4) Install dependencies
```bash
pip install -r requirements.txt
```

### 5) Run the server
```bash
python app.py
```

### 6) First-time setup (create admin)
Open:
- http://127.0.0.1:5000/setup

Then log in:
- http://127.0.0.1:5000/login

## Notes
- The database is a local SQLite file: `inventory.db` (created automatically).
- If you want a clean reset, stop the app and delete `inventory.db`.
- Item Type is stored per UPC (product). The system fills a blank item type, but won’t overwrite an existing one.
