]# Middle Seat Digital — Data Analyst Technical Assessment
**Completed by:** Christina Marikos  
**Submitted:** March 2026

---

## Overview

This repo contains my completed work for the Middle Seat Data Analyst technical assessment. The project uses a dbt + Redshift stack to transform raw ActBlue and Shopify donation data into a standardized reporting layer for the Benito for President campaign.

---

## What I Did

### Task 1 — Code Comments
Added comments to the following models and macros

- `macros/likely_source_type.sql` — Classifies a donation source channel from available fields. Checks source_type first, then pattern matches refcode and form_name. Creates a fallback for ActBlue Express Donor Dashboard from form_name.
- `macros/get_precore_tables.sql` — Dynamically discovers and returns a list of database tables matching the given schema and model patterns. Finds client source tables without hardcoding them.
- `models/precore/precore_actblue__donations.sql` — Enriches raw ActBlue donation records with client codes, source classifications, and finance exclusions. Produces one row per donation with standardized fields ready for the core layer.
- `models/core/core__donations.sql` — Unions ActBlue and Shopify data into a single standardized table.

### Task 2 — Schema Descriptions
Added column descriptions to `models/core/_core_schema.yml` for the `core__donations` model. Descriptions are written for non-technical teammates and include:
- Plain English descriptions written for selected fields
- Clarification in plain english on the meanings of integer and boolean flag fields
- Notes on known data limitations (e.g. `is_recurring_cancelled` does not update retroactively)
- Data tests include `accepted_values` on `recurring_type` and `likely_source_type`


### Task 3 — New Reporting Model
Created `models/reporting/reporting__donations_donor_analytics.sql` — a new reporting model built on `core__donations` designed to power donor and recurring program analytics in Looker Studio.

**Key additions over the existing `reporting__donations_by_category_by_day` model:**

- `unique_donors` — count of distinct donor emails per day and source
- `new_donors` — donors making their first ever donation, identified using a window function (`MIN(et_created_at) OVER (PARTITION BY email)`)
- `new_recurring_donors` — donors starting a new recurring giving plan
- `avg_donation_size` — average post-refund gift size
- `avg_recurring_donation_size` — average gift size for recurring donors only
- `avg_recurring_sequence_length` — how long recurring donors stay active on average
- `avg_sequence_at_cancellation` — average number of gifts made before a recurring donor cancels, useful for understanding donor churn

**Note on donor identity:** Email is used as a proxy for unique donor ID. There is a small possibility of double-counting donors who use different email addresses across ActBlue and Shopify.

**Note on running this project:** This project is configured for Middle Seat's internal Redshift instance and cannot be run locally without valid credentials. `dbt compile` can be used to validate SQL syntax without a live connection.

### Task 4 — Looker Studio Dashboard

[Dashboard Link](https://lookerstudio.google.com/reporting/ec1efb2a-b444-44de-85f5-f527f910e095)

The dashboard covers:
- **Donation stats** — total donations, dollars raised, average gift size by source and day
- **Donor stats** — unique donors, new donors, average gift size trends
- **Recurring program stats** — new recurring donors, average recurring gift size, average sequence length, cancellation patterns


## Notes

- My commenting north star guide is the [Stack Overflow code commenting best practices](https://stackoverflow.blog/2021/12/23/best-practices-for-writing-code-comments/)
- Jinja comments (`{# #}`) are used in files with Jinja templating and (`{#- -#}`) is used when preserving whitespace is important; SQL comments (`--`) are used for inline notes within SQL bodies
- Regardless of the outcome this was fun project and well put together test, thanks for giving me a whack at it

![Thanks!](https://www.hollywoodreporter.com/music/music-news/bad-bunny-album-of-the-year-spanish-language-1236489983/)