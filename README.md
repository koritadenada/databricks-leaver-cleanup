# Leaver cleanup

When someone leaves, Entra ID SCIM provisioning **deactivates** them in Azure Databricks: they can't sign in or use the APIs anymore. What they owned stays behind, though, and their scheduled jobs start failing. This bundle deploys a weekly job that finds those users and hands their objects over.

| Step | Finds | Action when `dry_run = false` |
|---|---|---|
| 1 | Users deactivated in the account, plus any `leaver_emails` you pass in | none |
| 2 | Jobs owned by, or running as, a leaver | non-bundle jobs: owner and `run_as` move to `fallback_job_sp`. Bundle jobs are reported only (fix `run_as` in the bundle) |
| 3 | Catalogs, schemas, tables, views and volumes owned by a leaver | `ALTER ... OWNER TO <fallback_owner>`. Streaming tables and materialized views are reported only |
| 4 | Personal access tokens created by a leaver | revoked (`revoke_tokens = true`) |

Every finding and action is appended to `<catalog>.<schema>.leaver_cleanup_log`. **The default is a dry run**: it reports what it would do and changes nothing.

## Layout

```
databricks.yml                       bundle, variables, dev/prod targets
resources/leaver_cleanup.job.yml     weekly serverless job (Monday 07:00 Europe/Tallinn)
resources/leaver_audit.schema.yml    schema for the log table
src/leaver_cleanup.py                the notebook
```

## Deploy and run

```bash
databricks bundle validate -t dev --profile <profile>
databricks bundle deploy   -t dev --profile <profile> --var catalog=<catalog>
databricks bundle run leaver_cleanup -t dev --profile <profile>   # dry run
```

Apply for real, once the dry-run report looks right:

```bash
databricks bundle run leaver_cleanup -t dev --profile <profile> \
  --params dry_run=false,fallback_owner=<group>,fallback_job_sp=<sp-application-id>
```

For `prod`, set `run_as` in `databricks.yml` to a service principal. That service principal needs to be a **workspace admin** (to read account users, change job permissions and revoke tokens) and able to change ownership in Unity Catalog (metastore admin, or `MANAGE` on the catalogs). In `prod` the schedule is unpaused.

## Variables

| Variable | Default | Purpose |
|---|---|---|
| `catalog` | `dev_governance` / `prod_governance` | catalog for the log schema |
| `schema` | `identity_lifecycle` | schema for `leaver_cleanup_log` |
| `dry_run` | `true` | `false` applies the changes |
| `revoke_tokens` | `true` | revoke leavers' personal access tokens when applying |
| `fallback_owner` | | group or service principal that takes over Unity Catalog objects |
| `fallback_job_sp` | | application ID of the service principal that takes over jobs |
| `leaver_emails` | | extra leavers, comma-separated (for example, someone who is still active but leaving) |

## Notes

- Leavers are read from the account SCIM API (`/api/2.0/account/scim/v2/Users`, users with `active = false`). That covers users deactivated by Entra SCIM, by automatic identity management, or by hand.
- With automatic identity management, a user deleted in Entra can't sign in right away but shows as *Active: Removed From EntraID* until the next identity sync deactivates them. Pass them in `leaver_emails` if you need to act sooner.
- Users that Entra has already hard-deleted (30 days after a soft delete) are no longer in the account, so pass them in `leaver_emails` if they still own things.
- Streaming tables and materialized views can't change owner with `ALTER`. Set their pipeline's **Run as** to a service principal instead, which moves ownership of the tables it publishes.
- Docs: [SCIM with Entra ID](https://learn.microsoft.com/en-us/azure/databricks/admin/users-groups/scim/aad), [Deactivate a user](https://learn.microsoft.com/en-us/azure/databricks/admin/users-groups/users#deactivate-a-user), [Token Management API](https://docs.databricks.com/api/azure/workspace/tokenmanagement).
