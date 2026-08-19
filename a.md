We need to replace `crm_statuses` with `workspace_crm_statuses` in our `mv_open_leads` MATERIALIZED.

```
DROP MATERIALIZED VIEW IF EXISTS mv_open_leads;

CREATE MATERIALIZED VIEW mv_open_leads AS
SELECT
    c.id AS customer_id,
    c.workspace_id,
    c.assignee_id AS user_id,
    COALESCE(cd.contact_intent, 'NOT_AVAILABLE'::character varying) AS contact_intent,
    cd.last_conversation
FROM customer_details cd
LEFT JOIN customers c
    ON c.id = cd.customer_id
LEFT JOIN workspace_crm_statuses wcs
    ON wcs.workspace_id = c.workspace_id
   AND wcs.crm_status_id = cd.crm_status_id
WHERE wcs.category::text = ANY (
    ARRAY[
        'NEW'::character varying,
        'IN_PROGRESS'::character varying
    ]::text[]
)
AND cd.last_conversation >= CURRENT_DATE - INTERVAL '30 days';

ALTER MATERIALIZED VIEW mv_open_leads
    OWNER TO misc_actions_user;

CREATE INDEX idx_mv_open_leads_workspace_id
    ON mv_open_leads (workspace_id);

CREATE INDEX idx_mv_open_leads_user_id
    ON mv_open_leads (user_id);

CREATE INDEX idx_mv_open_leads_workspace_user
    ON mv_open_leads (workspace_id, user_id);

CREATE UNIQUE INDEX idx_mv_open_leads_customer_id
    ON mv_open_leads (customer_id);
```