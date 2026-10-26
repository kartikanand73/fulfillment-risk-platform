# Decisions

| # | Decision | Why |
|---|---|---|
| D-001 | **New repository.** Code from `foundry-skill-agent-platform` (agent core, guardrails, identity, tracing, connectors) is copied in when a phase needs it, not shared. | Separate project and lifecycle; the existing platform keeps working unchanged. |
| D-002 | **Allocation order:** `order_date` ASC, then customer tier (Enterprise, Mid-Market, SMB, unknown), then `order_id`, then `line_number`. | First-come-first-served is the usual fulfillment policy. Tier breaks same-day ties, and ids make the result fully deterministic. |
| D-003 | **Stock pooled across locations:** available = Σ(on_hand − reserved), floored at 0 per location. | Version 1 answers "can we fulfil?", not "from where?". Per-location allocation can come later without changing the contract. |
| D-004 | **"Open" means not yet shipped.** Phase 5's Databricks query selects `status = 'Processing'`. | Shipped orders have already consumed inventory. The status list is a connector setting. |
| D-005 | **Unknown SKU** (not in the inventory feed) → `DATA_ISSUE`, consumes nothing. **Unknown customer** (not in Salesforce) → `DATA_ISSUE`, but still consumes stock in its priority slot. | An unknown SKU means we can't judge it. An unknown-customer order is still a real order competing for stock, and ignoring it would make others look safer than they are. |
| D-006 | **Roll-up:** order and customer status = worst line status (DATA_ISSUE > UNFULFILLABLE > PARTIAL > FULFILLABLE). Customers with no open orders aren't stored. | Simple and explainable, and it keeps the stored result as small as possible. |
| D-007 | **Money:** Decimal throughout; short value = short qty × unit net value, rounded half-up to cents. Also `blocked_order_value` = total value of orders that can't ship complete. | No float drift. "What's short" and "what's held up" are both useful. |
| D-008 | **Publish gates:** every source answers; freshness ≤ 24 h; customers and inventory not empty; data issues ≤ 10% of lines. Failing any gate means not published. | A wrong snapshot is worse than an old one, and readers always see how old it is. |
| D-009 | **Safe publish:** results and the `published_snapshot` pointer change in one transaction; a session advisory lock allows one refresh at a time; leftover `running` runs become `abandoned`. | Failed refreshes never overwrite the last good snapshot. |
| D-010 | **Reader access is views only:** `fr_reader` has SELECT on `fr.v_*`, with no base tables and no writes. `fr_writer` can't change the schema. | Least privilege for the agent. Version 1 is read-only end to end. |
| D-011 | **Zero model tokens on the refresh path**, enforced by tests: no model SDK imports in the package, and the engine runs with networking disabled. | The scheduled job must be deterministic and free to run. |
| D-012 | **Scheduled extraction uses Databricks SQL, not Genie** (Phase 5). | The dashboard dataset must be deterministic. Genie stays a conversational tool. |
| D-013 | **No Power BI.** | Out of scope by request. The `fr.v_*` views are the read surface for the agent, and any future dashboard could use them too. |
