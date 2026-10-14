# Dataset Discovery, Selection, and Initial Scope

**Date:** October 14, 2026

## Context

The goal is to understand Toronto’s housing-development and building-permit lifecycle, identify where permits remain active or age, and investigate potential processing bottlenecks.

I started by exploring the City’s [Building Permit information](https://www.toronto.ca/services-payments/building-construction/building-permit/) to understand when permits are required and what happens before and after an application. I learned that a building permit is City authorization required for certain construction or demolition work, while zoning by-laws govern how land and buildings may be used; these are related but distinct controls. This research introduced me to application review, inspections, and zoning review, and helped me identify relevant datasets through [Toronto Open Data](https://open.toronto.ca/).

## Research findings

### 1. Active and Cleared Permits — the initial foundation

The City’s [Active Permits dataset](https://open.toronto.ca/dataset/building-permits-active-permits/) provides information about currently active building applications and permits, including updates made by City staff during application review and inspection.

The City’s [permit-status service](https://www.toronto.ca/services-payments/building-construction/building-permit/after-you-apply-for-a-building-permit/search-the-status-of-a-building-permit-application/) helped me understand the different states an application or permit may occupy, such as *Not Started*, *Under Review*, *Notice Sent*, *Issuance Pending*, *Permit Issued*, and *Inspection*. These states suggest ways to classify active work, although a status snapshot alone does not prove the full history of every transition.

The City’s [Exploring Cleared Building Permits analysis](https://open.toronto.ca/exploring-cleared-building-permits/) describes five lifecycle stages: application, review, issue, inspection, and close. It explains that permits remain Active until closed and suggests considering Active and Cleared permits together for a more complete picture.

**Finding:** Active Permits support current-work and aging analysis; Cleared Permits provide completed outcomes and lifecycle durations. Together, they are the most direct starting point for the project’s initial questions.

### 2. Development Applications — a potential upstream view

The City’s [Development Pipeline](https://www.toronto.ca/city-government/data-research-maps/research-reports/planning-development/development-pipeline/) brings planning-application and building-permit information together to analyse development projects. It counts housing units and non-residential floor area only once across multiple approvals.

**Potential value:** Explore whether planning-application information can add earlier-stage context to permit analysis.

**Limitation:** The City’s Pipeline works at project/site level and has a defined scope. We must not assume that directly joining the available datasets would reproduce its results or reliably link every application to a permit.

### 3. Preliminary Zoning Reviews — another potential upstream stage

The City’s [Preliminary Zoning Reviews information](https://www.toronto.ca/services-payments/building-construction/building-permit/before-you-apply-for-a-building-permit/preliminary-zoning-reviews-information/) explains how zoning or applicable-law requirements are assessed for a property. The City’s permit-status service also reports Preliminary Zoning Review applications, including Zoning Use Reviews and Preliminary Project Reviews for Signs.

I also learned that Ontario’s [Planning Act, section 34](https://www.ontario.ca/laws/statute/90p13#BK66) provides for municipal zoning by-laws regulating land and building uses.

**Potential value:** Investigate whether available review records provide useful information about pre-permit activity or review timelines.

**Limitation:** We have not demonstrated reliable links between zoning reviews, development applications, and building permits. They must not be assumed to represent the same process or a one-to-one sequence.

### 4. Address Points — a potential geographic reference

The City’s [Address Points dataset](https://open.toronto.ca/dataset/address-points-municipal-toronto-one-address-repository/) provides point representations of Toronto addresses.

The City’s Cleared Permits analysis identifies `Geo ID` as a potential link between permit records and Address Points to support more accurate mapping.

**Potential value:** Improve geographic analysis and investigate more reliable address matching.

**Limitation:** We still need to test identifier uniqueness, completeness, match coverage, and join cardinality before using this dataset.

## Decision

The initial ingestion scope is **Active Permits and Cleared Permits only**. They directly support the initial lifecycle and aging questions and provide a manageable foundation for validating ingestion reliability, data quality, and reproducible analytics.

Development Applications, Preliminary Zoning Reviews, and Address Points remain research candidates. They are excluded from the initial ingestion configuration and implementation.

## Trade-offs

Starting with two datasets enables faster, more verifiable delivery but limits our view of the wider planning-to-construction process. Adding more datasets could provide useful context, but only if their grain, identifiers, and relationships can be validated. Otherwise, joins could introduce duplicate counts or misleading lifecycle durations.

## Revisit criteria

Consider an additional dataset when:

- It answers a specific question that the existing permit data cannot adequately answer.
- Its native grain, key fields, and relevant date semantics are understood.
- Linkage coverage, unmatched records, ambiguous matches, and join cardinality have been measured.
- Tests demonstrate that integration does not duplicate measures or distort existing permit metrics.

## References

### A. Initial discovery and building-permit process

1. [Toronto Open Data Portal](https://open.toronto.ca/) — starting point for discovering municipal datasets.
2. [City of Toronto — Building Permits](https://www.toronto.ca/services-payments/building-construction/building-permit/) — general introduction to building permits and the application process.
3. [City of Toronto — When Do I Need a Building Permit?](https://www.toronto.ca/services-payments/building-construction/building-permit/before-you-apply-for-a-building-permit/when-do-i-need-a-building-permit/) — official explanation of common permit requirements.
4. [City of Toronto — Building Permit Application & Inspection Status](https://www.toronto.ca/services-payments/building-construction/building-permit/after-you-apply-for-a-building-permit/search-the-status-of-a-building-permit-application/) — permit-status definitions and Preliminary Zoning Review status information.

### B. Dataset sources

5. [Toronto Open Data — Active Permits](https://open.toronto.ca/dataset/building-permits-active-permits/) — source description for active building applications and permits.
6. [Toronto Open Data — Cleared Permits](https://open.toronto.ca/dataset/building-permits-cleared-permits/) — source description for completed/closed permits.
7. [Toronto Open Data — Development Applications](https://open.toronto.ca/dataset/development-applications/) — potential source for investigating the planning-application stage.
8. [Toronto Open Data — Preliminary Zoning Reviews](https://open.toronto.ca/dataset/preliminary-zoning-reviews/) — potential source for investigating zoning-review activity.
9. [Toronto Open Data — Address Points (Municipal), Toronto One Address Repository](https://open.toronto.ca/dataset/address-points-municipal-toronto-one-address-repository/) — potential geographic reference for address matching and mapping.
10. [Active Permits — CKAN resource preview](https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/building-permits-active-permits/resource/6d0229af-bc54-46de-9c2b-26759b01dd05/view/d66378a6-499b-4f9f-b7aa-bf03c37acb5b) — resource-level link retained from the original discovery notes. The dataset landing page above is the canonical reference if this preview is unavailable.

### C. City analysis and analytical context

11. [City of Toronto Open Data — Exploring Cleared Building Permits (2017)](https://open.toronto.ca/exploring-cleared-building-permits/) — documents the permit lifecycle, historical analysis questions, permit revision identifiers, the potential `Geo ID` link to Address Points, and reasons to analyse Active and Cleared permits together.
12. [City of Toronto — Development Pipeline](https://www.toronto.ca/city-government/data-research-maps/research-reports/planning-development/development-pipeline/) — prior art for connecting planning applications and building permits at the development-project/site level while avoiding double-counting across approvals.
13. [City of Toronto — Housing Data Hub: Housing Data](https://www.toronto.ca/city-government/data-research-maps/toronto-housing-data-hub/housing-data/) — official housing, planning-application and permit-related metrics. Definitions, populations, and time units must be checked before comparing them with this project.

### D. Zoning and legal background

14. [City of Toronto — Preliminary Zoning Reviews & Information](https://www.toronto.ca/services-payments/building-construction/building-permit/before-you-apply-for-a-building-permit/preliminary-zoning-reviews-information/) — official information about zoning reviews and property information.
15. [Ontario Planning Act, section 34 — Zoning by-laws](https://www.ontario.ca/laws/statute/90p13#BK66) — provincial legislation; consult section 34 for the relevant zoning-by-law provisions.

### E. Supplementary background reading

16. [BVM Homes — Toronto Zoning Review](https://www.bvmhomes.com/blog/toronto-zoning-review) — practical, non-government background on Toronto zoning reviews; supplementary reading rather than primary evidence.
17. [Town of Banff — What Is a Bylaw?](https://banff.ca/677/What-is-a-Bylaw) — general background on municipal by-laws. This is an Alberta municipal resource, not authority for Toronto- or Ontario-specific legal claims.