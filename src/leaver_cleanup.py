# Databricks notebook source
# MAGIC %md
# MAGIC # Leaver cleanup
# MAGIC
# MAGIC Entra ID SCIM provisioning **deactivates** a user in Azure Databricks when they leave
# MAGIC (unassigned from the SCIM app, disabled or soft-deleted). Deactivation blocks sign-in and
# MAGIC API access, but whatever the user owned stays behind, and their scheduled jobs fail.
# MAGIC
# MAGIC This notebook finds those users and hands over what they still own:
# MAGIC
# MAGIC | Step | Finds | Action when `dry_run = false` |
# MAGIC |---|---|---|
# MAGIC | 1 | Users deactivated in the account, plus `leaver_emails` | none |
# MAGIC | 2 | Jobs owned by, or running as, a leaver | non-bundle jobs: owner and `run_as` move to `fallback_job_sp`; bundle jobs: reported, fix in the bundle |
# MAGIC | 3 | Catalogs, schemas, tables, views and volumes owned by a leaver | `ALTER ... OWNER TO fallback_owner` (streaming tables and materialized views: reported, fix via the pipeline's Run as) |
# MAGIC | 4 | Personal access tokens created by a leaver | revoked when `revoke_tokens = true` |
# MAGIC
# MAGIC Every finding is appended to `<catalog>.<schema>.leaver_cleanup_log`.
# MAGIC Run it as a service principal that is a workspace admin and can change ownership of the
# MAGIC Unity Catalog objects in scope (for example a metastore admin, or `MANAGE` on the catalogs).

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "identity_lifecycle")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])
dbutils.widgets.dropdown("revoke_tokens", "true", ["true", "false"])
dbutils.widgets.text("fallback_owner", "")
dbutils.widgets.text("fallback_job_sp", "")
dbutils.widgets.text("leaver_emails", "")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DRY_RUN = dbutils.widgets.get("dry_run").lower() != "false"
REVOKE_TOKENS = dbutils.widgets.get("revoke_tokens").lower() == "true"
FALLBACK_OWNER = dbutils.widgets.get("fallback_owner").strip()
FALLBACK_JOB_SP = dbutils.widgets.get("fallback_job_sp").strip()
EXTRA_LEAVERS = {e.strip().lower() for e in dbutils.widgets.get("leaver_emails").split(",") if e.strip()}

assert CATALOG and SCHEMA, "catalog and schema are required"
if not DRY_RUN:
    assert FALLBACK_OWNER, "fallback_owner is required when dry_run = false"
    assert FALLBACK_JOB_SP, "fallback_job_sp is required when dry_run = false"

LOG_TABLE = f"`{CATALOG}`.`{SCHEMA}`.leaver_cleanup_log"
print(f"dry_run={DRY_RUN} revoke_tokens={REVOKE_TOKENS} log={LOG_TABLE}")

# COMMAND ----------

import datetime
import json

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam, jobs

w = WorkspaceClient()
RUN_TS = datetime.datetime.now(datetime.timezone.utc)
findings = []


def record(leaver, object_type, object_id, finding, action, status, detail=""):
    findings.append(
        dict(
            run_ts=RUN_TS,
            dry_run=DRY_RUN,
            leaver=leaver,
            object_type=object_type,
            object_id=str(object_id),
            finding=finding,
            action=action,
            status=status,
            detail=detail[:1000],
        )
    )


def quote(name):
    return ".".join(f"`{part}`" for part in name.split("."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leavers
# MAGIC Account users with `active = false`, read through the account SCIM API that workspace admins
# MAGIC can call from the workspace (`/api/2.0/account/scim/v2/Users`).

# COMMAND ----------

def list_account_users():
    users, start, page = [], 1, 100
    while True:
        resp = w.api_client.do(
            "GET",
            "/api/2.0/account/scim/v2/Users",
            query={"attributes": "id,userName,active", "startIndex": start, "count": page},
        )
        batch = resp.get("Resources", [])
        users.extend(batch)
        if not batch or start + len(batch) > int(resp.get("totalResults", 0)):
            return users
        start += len(batch)


account_users = list_account_users()
by_email = {u["userName"].lower(): u for u in account_users if u.get("userName")}

leavers = {email: u["id"] for email, u in by_email.items() if u.get("active") is False}
for email in EXTRA_LEAVERS:
    leavers.setdefault(email, by_email.get(email, {}).get("id"))

for email, user_id in sorted(leavers.items()):
    source = "leaver_emails" if email in EXTRA_LEAVERS else "deactivated in account"
    record(email, "user", user_id or "", f"leaver ({source})", "none", "found")

print(f"{len(account_users)} account users, {len(leavers)} leavers: {sorted(leavers)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Jobs
# MAGIC A leaver's scheduled jobs fail once they are deactivated. Jobs deployed by a bundle are only
# MAGIC reported: changing them here would be overwritten by the next `bundle deploy`.

# COMMAND ----------

def job_owner(job_id):
    acl = w.permissions.get(request_object_type="jobs", request_object_id=str(job_id))
    for entry in acl.access_control_list or []:
        if any(p.permission_level == iam.PermissionLevel.IS_OWNER for p in entry.all_permissions or []):
            return (entry.user_name or entry.service_principal_name or entry.group_name or "").lower()
    return ""


this_job_id = None
try:
    ctx = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson())
    this_job_id = ctx.get("attributes", {}).get("jobId")
except Exception:
    pass

if leavers:
    for base in w.jobs.list():
        if str(base.job_id) == str(this_job_id):
            continue
        job = w.jobs.get(job_id=base.job_id)
        owner = job_owner(job.job_id)
        run_as = (job.run_as_user_name or "").lower()
        hits = {e for e in (owner, run_as) if e in leavers}
        if not hits:
            continue

        name = job.settings.name if job.settings else str(job.job_id)
        leaver = sorted(hits)[0]
        finding = f"owner={owner} run_as={run_as}"
        deployment = job.settings.deployment if job.settings else None
        is_bundle = (deployment is not None and deployment.kind == jobs.JobDeploymentKind.BUNDLE) or (
            (job.settings.tags or {}).get("managed_by") == "dab"
        )

        if is_bundle:
            record(leaver, "job", job.job_id, finding, f"fix run_as in the bundle for '{name}'", "manual")
        elif DRY_RUN:
            record(leaver, "job", job.job_id, finding, f"would move owner and run_as of '{name}' to {FALLBACK_JOB_SP or '<fallback_job_sp>'}", "planned")
        else:
            try:
                w.permissions.update(
                    request_object_type="jobs",
                    request_object_id=str(job.job_id),
                    access_control_list=[
                        iam.AccessControlRequest(
                            service_principal_name=FALLBACK_JOB_SP,
                            permission_level=iam.PermissionLevel.IS_OWNER,
                        )
                    ],
                )
                w.jobs.update(
                    job_id=job.job_id,
                    new_settings=jobs.JobSettings(run_as=jobs.JobRunAs(service_principal_name=FALLBACK_JOB_SP)),
                )
                record(leaver, "job", job.job_id, finding, f"moved owner and run_as of '{name}' to {FALLBACK_JOB_SP}", "done")
            except Exception as e:
                record(leaver, "job", job.job_id, finding, "move owner and run_as", "failed", str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Unity Catalog objects
# MAGIC Read from `system.information_schema`, which only shows objects the running principal can see.

# COMMAND ----------

UC_QUERY = """
SELECT 'CATALOG' AS securable, catalog_name AS full_name, catalog_owner AS owner, NULL AS table_type
FROM system.information_schema.catalogs
UNION ALL
SELECT 'SCHEMA', catalog_name || '.' || schema_name, schema_owner, NULL
FROM system.information_schema.schemata
UNION ALL
SELECT 'TABLE', table_catalog || '.' || table_schema || '.' || table_name, table_owner, table_type
FROM system.information_schema.tables
UNION ALL
SELECT 'VOLUME', volume_catalog || '.' || volume_schema || '.' || volume_name, volume_owner, NULL
FROM system.information_schema.volumes
"""

# Ownership of these table types can't be changed with ALTER ... OWNER TO.
NOT_TRANSFERABLE = {"STREAMING_TABLE", "MATERIALIZED_VIEW"}

if leavers:
    owned = spark.sql(UC_QUERY).where("lower(owner) IN ({})".format(",".join(f"'{e}'" for e in leavers))).collect()
    for row in owned:
        leaver = row.owner.lower()
        securable = "VIEW" if row.table_type in ("VIEW", "METRIC_VIEW") else row.securable
        object_type = (row.table_type or securable).lower()
        statement = f"ALTER {securable} {quote(row.full_name)} OWNER TO `{FALLBACK_OWNER or '<fallback_owner>'}`"

        if row.table_type in NOT_TRANSFERABLE:
            record(leaver, object_type, row.full_name, f"owner={row.owner}", "set the pipeline's Run as to a service principal (moves table ownership)", "manual")
        elif DRY_RUN:
            record(leaver, object_type, row.full_name, f"owner={row.owner}", statement, "planned")
        else:
            try:
                spark.sql(statement)
                record(leaver, object_type, row.full_name, f"owner={row.owner}", statement, "done")
            except Exception as e:
                record(leaver, object_type, row.full_name, f"owner={row.owner}", statement, "failed", str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Personal access tokens
# MAGIC Deactivation already blocks these tokens. Revoking them removes them for good.

# COMMAND ----------

for email, user_id in sorted(leavers.items()):
    if not user_id:
        continue
    for token in w.token_management.list(created_by_id=int(user_id)):
        finding = f"token comment='{token.comment or ''}'"
        if not REVOKE_TOKENS:
            record(email, "token", token.token_id, finding, "none (revoke_tokens = false)", "skipped")
        elif DRY_RUN:
            record(email, "token", token.token_id, finding, "would revoke", "planned")
        else:
            try:
                w.token_management.delete(token_id=token.token_id)
                record(email, "token", token.token_id, finding, "revoked", "done")
            except Exception as e:
                record(email, "token", token.token_id, finding, "revoke", "failed", str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Log

# COMMAND ----------

from pyspark.sql import types as T

LOG_SCHEMA = T.StructType(
    [
        T.StructField("run_ts", T.TimestampType()),
        T.StructField("dry_run", T.BooleanType()),
        T.StructField("leaver", T.StringType()),
        T.StructField("object_type", T.StringType()),
        T.StructField("object_id", T.StringType()),
        T.StructField("finding", T.StringType()),
        T.StructField("action", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("detail", T.StringType()),
    ]
)

log_df = spark.createDataFrame(findings, LOG_SCHEMA)
log_df.write.mode("append").saveAsTable(LOG_TABLE)
display(log_df)

summary = {s: sum(1 for f in findings if f["status"] == s) for s in sorted({f["status"] for f in findings})}
print(f"Logged {len(findings)} rows to {LOG_TABLE}: {summary}")
if summary.get("failed"):
    raise RuntimeError(f"{summary['failed']} actions failed, see {LOG_TABLE}")
