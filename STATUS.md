# Spicetopia BMS — Where We Are / What's Next

A short snapshot that travels with the code. Full history lives in `CLAUDE.md`
(LAST SESSION blocks), `BUILT.md`, and `SCHEMA.md` at the project root.

_Last updated: 2026-06-28_

## Status
- **PROD is current** (Railway auto-deploys on `git push origin master`; pre-push overseer hook gates).
- **DEV is stale** — PROD is the source of truth. Pre-launch: PROD has no real customer data yet.
- Planning module is live and in **pilot** with senior managers (admin logins for now).

## Recently shipped (2026-06-28)
- ERP nav/UX: Start-Here Home, collapsible (default-collapsed) sidebar, twisty nav sections,
  command palette (⌘/Ctrl+K), Home→Dashboard links, "Take an order" deep-link.
- Whole-ERP Editorial-Luxury retheme (terracotta/paper/ink, Fraunces + Inter).
- Planning: scenario-type vs status fix; Release card pinned atop Review (Approve folded in);
  header plan dropdown → Plans-hub switcher; first-run walkthrough + **? Guide**;
  one-page PDF quick-start (`Spicetopia_Planning_Quick_Start.pdf`, project root, not in repo).
- Planning: **forecast by zone** (grain month × SKU × channel × zone_id; reuses ERP zones + covering rep).
- ERP **per-module ? Help** walkthroughs + a "How it all connects" data-flow overview.

## Next up
1. **Planning ↔ ERP integration sprint** (the priority): (a) shared login — drop planning's own
   `spx_plan_token`, run on `erp_token`; (b) pricing reuse from `compute_standard_cost()` + live
   price book (`plan_pricing` = overrides only); (c) drop `list_active_variants` for the ERP source;
   (d) role-gate via Super User + `user_permissions` (replaces interim admin-only).
2. Per-zone rollup on planning Review (volume by area + covering rep + coverage-gap flag).
3. Remaining ERP ? Help decks: field-orders, review-queue, ap-payments, ap-aging, pl-report,
   margins, products, customers, suppliers, users, costing.

## Standing open items
- Ingredients → PO handoff (release raises ingredient POs up front).
- Plan → customer-orders handoff + committed/uncommitted model (spec'd).
- Release reconciliation panel (decision-support only).
- Off-site backup automation (R2/B2/S3) — deferred.
- Repo cleanup: stale `ARCHITECTURE.md` / `README.md` / `sprint_deploy.sh` + untracked
  `test_server_8770.py` are deliberately left out of deploys — review/clean.

## Deploy (reminder)
- PROD: `git push origin master` (overseer pre-push gate). DEV: `railway up --detach` from inside `spicetopia-bms/`.
- All new migrations go in `modules/migrations.py` (idempotent), wired in `server.py` at startup.
