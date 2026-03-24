import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = "1YAZWwCZ5Vf-GhMkRJaBoMlysJtkxyQothxnte6xU_cg"  # paste your Sheet ID here

creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)

print("Connected! Tabs found:")
for ws in sh.worksheets():
    print(f"  - {ws.title}")