from sqlalchemy import create_engine
import json
import pandas as pd

# --- Load Databricks credentials ---
with open("dbx_creds092525.json") as f:
    config = json.load(f)

access_token = config["access_token"]
server_hostname = config["server_hostname"]
http_path = config["http_path"]
catalog = config["catalog"]
schema = config["schema"]
table = "bi_rate_daily"

engine = create_engine(
    url=f"databricks://token:{access_token}@{server_hostname}?"
        f"http_path={http_path}&catalog={catalog}&schema={schema}"
)

df = pd.read_csv("bquxjob_5050da6f_1997f2fd1ba.csv")

df.to_sql(
    name="bi_rate_daily",
    con=engine,
    if_exists="append",
    index=False
)

print(" Data inserted successfully into Databricks!")
