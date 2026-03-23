# Middle Seat - Data Analyst Technical Assessment
**Completed by:** Christina Marikos  
**Submitted:** March 23, 2026

---

## Overview

This repo contains my completed work for the Middle Seat Data Analyst technical assessment. The project uses a dbt + Redshift stack to transform raw ActBlue and Shopify donation data into a standardized reporting layer for the Benito for President campaign.

---

## What I Did

### Task 1 - Copy the [project Google sheet](https://docs.google.com/spreadsheets/d/1yoqaUQke6F_6_83maT2TXMFeJg3iktn0Naa30XAjHzs/edit?gid=1483213299#gid=1483213299) and the [project Data Studio](https://lookerstudio.google.com/reporting/7609085c-cbea-4551-8438-1d75d18a544e/page/p_co1trq1irc)

---

### Task 2 — Code Comments
Added comments to the following models and macros

- `macros/likely_source_type.sql` — Classifies a donation source channel from available fields. Checks source_type first, then pattern matches refcode and form_name. Creates a fallback for ActBlue Express Donor Dashboard from form_name.
- `macros/get_precore_tables.sql` — Dynamically discovers and returns a list of database tables matching the given schema and model patterns. Finds client source tables without hardcoding them.
- `models/precore/precore_actblue__donations.sql` — Enriches raw ActBlue donation records with client codes, source classifications, and finance exclusions. Produces one row per donation with standardized fields ready for the core layer.
- `models/core/core__donations.sql` — Unions ActBlue and Shopify data into a single standardized table.

---

### Task 3 — Schema Descriptions
Added column descriptions to `models/core/_core_schema.yml` for the `core__donations` model. Descriptions are written for non-technical teammates and include:
- Plain English descriptions written for selected fields
- Clarification in plain english on the meanings of integer and boolean flag fields
- Notes on known data limitations (e.g. `is_recurring_cancelled` does not update retroactively)
- Data tests include `accepted_values` on `recurring_type` and `likely_source_type`

---

### Task 4 — New Reporting Model
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

---

### Task 5 — Looker Studio Dashboard

[Dashboard Link](https://lookerstudio.google.com/reporting/ec1efb2a-b444-44de-85f5-f527f910e095)

My dashboard using the [project Google sheet](https://docs.google.com/spreadsheets/d/1ox_lWqmR0KXZzc7UDKVGWoC2rHtdcNGIqS_5sAUI-q4/edit?gid=1483213299#gid=1483213299) 

**New additions to the Benito for President dashboard:**

- **Date range filtering:** Added a dynamic date range control that applies across all charts. Comparison metrics in the Dollars Raised by Source chart automatically adjust to the equivalent preceding period. For example, selecting a week compares to the prior week, selecting a month compares to the prior month.

- **Time series chart:** Added a time series chart showing dollars raised over time, responsive to the date range filter and the source filter.

- **Summary scorecards:** Duplicated the total summary cards so one set is static (full campaign totals) and one set is dynamic (filtered to the selected date range). Added average donation size and average recurring donation size to the dynamic card set.

- **Donations by source channel (pie chart):** Added a pie chart showing percentage of donations by source channel, cross-filtered with the date range control and source control filters.

- **Recurring vs one-time by source:** Added a stacked bar chart showing total dollars raised per source channel broken out by recurring vs one-time donations. Built using a calculated field that simplifies `recurring_type` into either recurring and one-time.

- **Filterable dollars raised by source:** The existing dollars raised by source chart is now connected to the date range and source control filters and shows period-over-period comparison against the preceding equivalent time range.


**Notes on model from Task 4 and source data in project table**
- The provided sample sheet contains rows where `dollars_raised > 0` but `number_of_donations = 0`. In the dashboard, rows with `number_of_donations = 0` are excluded from average donation size calculations to avoid division by zero.
- Several fields in `reporting__recurring_by_source_by_day.sql` — including `unique_donors`, `new_donors`, `new_recurring_donors`, and the recurring sequence metrics cannot be surfaced in the Looker Studio dashboard without a live Redshift connection. These fields are documented in the model and would populate automatically in production.
- To simulate some of the model's output, calculated fields have been added directly in Looker Studio using the provided sample data. These approximate the intent of the model's metrics where possible.

---

## Notes

- My commenting north star guide is the [Stack Overflow code commenting best practices](https://stackoverflow.blog/2021/12/23/best-practices-for-writing-code-comments/)
- Jinja comments (`{# #}`) are used in files with Jinja templating and (`{#- -#}`) is used when preserving whitespace is important; SQL comments (`--`) are used for inline notes within SQL bodies
- Regardless of the outcome this was fun project and well put together test, thanks for giving me a whack at it

![Thanks!](https://www.hollywoodreporter.com/wp-content/uploads/2026/02/GettyImages-2259501591.jpg?crop=0px%2C200px%2C5000px%2C2798px&resize=2000%2C1126)