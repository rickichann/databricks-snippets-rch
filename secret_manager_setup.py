# Databricks notebook source
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Step 1: Create the secret scope
w.secrets.create_scope(scope="rds_creds")

# Step 2: Store each credential
w.secrets.put_secret(scope="rds_creds", key="host", string_value="")
w.secrets.put_secret(scope="rds_creds", key="port", string_value="")
w.secrets.put_secret(scope="rds_creds", key="username", string_value="")
w.secrets.put_secret(scope="rds_creds", key="password", string_value="")

# Step 3: Verify
for secret in w.secrets.list_secrets(scope="rds_creds"):
    print(secret.key)

# COMMAND ----------

# DBTITLE 1,Test - Retrieve secrets
# Test: Retrieve secrets to verify they are accessible
host = dbutils.secrets.get(scope="rds_creds", key="host")
port = dbutils.secrets.get(scope="rds_creds", key="port")
username = dbutils.secrets.get(scope="rds_creds", key="username")
password = dbutils.secrets.get(scope="rds_creds", key="password")

# Print confirmation (values will be redacted by Databricks for security)
print(f"IP: {host}")
print(f"Port: {port}")
print(f"Username: {username}")
print(f"Password: {password}")
print("\n All secrets retrieved successfully!")