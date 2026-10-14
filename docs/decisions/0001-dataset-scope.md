# Dataset Selection: Start with Active and Cleared Permits

**Decision date:** October 14, 2026

**Record type:** Retrospective decision record reconstructed from project discovery notes. The five datasets were not all selected at the beginning.

## Goal and discovery path

The project aims to understand Toronto's building-permit lifecycle, identify permits that remain active or age, and investigate potential processing bottlenecks.

I started with the City's building-permit guidance to understand when permits are required and what happens before and after an application, then explored Toronto Open Data and found Active and Cleared Permits [1–3]. The City's permit-status guidance lists states such as Application Received, Not Started, Under Review, Notice Sent, Issuance Pending, Permit Issued, and Inspection, alongside special states such as Cancellation Pending and Violation [4]. These labels can help classify current work, but a status snapshot alone does not establish the full transition history.

The research also introduced me to Preliminary Zoning Reviews and zoning by-laws. A zoning review and a building permit are distinct processes; Ontario's Planning Act provides for municipal zoning by-laws [14, 15].

## What the City research established

The City's analysis of Cleared Building Permits describes five stages—application, review, issue, inspection, and close—and explains why considering Active and Cleared permits together gives a more complete picture [11]. It also surfaced important modeling cautions:

- `Permit Num` and `Revision Num` identify a permit revision; this project's source-grain checks also account for permit type.
- One site may have multiple permit types, so a site, permit, and revision should not be assumed to be one-to-one.
- `Geo ID` is a potential link between permit records and Address Points, but is not proof of complete or unambiguous matches in our data [9, 11].

The City's Development Pipeline links planning applications and building permits at development-project/site level and avoids counting housing units or floor area more than once across approvals [12]. This demonstrates the value of project-level lifecycle analysis—not that this project's public source rows can be joined directly to reproduce the City's pipeline. The City also publishes planning and permit-related measures in its Housing Data Hub [13]; these are useful context, but definitions and populations must be checked before comparison.

## Dataset choices: evidence and open questions

| Dataset | What the sources support | Potential value | What must be proven before integration |
|---|---|---|---|
| **Active Permits + Cleared Permits** | The City publishes the two populations separately and explains why they should be considered together [5, 6, 11]. | Analyse the active population and age since application, alongside completed outcomes and elapsed time between available lifecycle dates. | Date quality, metric definitions, exclusions, and comparability across populations and periods. A current status is not a full transition history. |
| **Development Applications** | The City's Development Pipeline combines planning-application and permit information at project/site level [12]. A public dataset is available [7]. | Investigate upstream planning context and longer development timelines. | Native grain, key quality, match coverage, unmatched records, and duplicate-count risk. |
| **Preliminary Zoning Reviews** | City guidance covers zoning reviews, and the permit-status service includes Preliminary Zoning Review applications [4, 14]. A dataset is available [8]. | Explore an earlier review process and possible review timelines. | Date semantics and reliable links to applications or permits; do not assume every review leads to a permit. |
| **Address Points** | The City's permit analysis identifies `Geo ID` as a potential link to Address Points [11]. The dataset is available [9]. | Improve geographic analysis and investigate address matching. | Key uniqueness and completeness, match coverage, ambiguous matches, and one-to-many relationships. |

The sources establish a credible reason to explore all five datasets. Only Active and Cleared directly support the initial permit-lifecycle and aging questions; the joinability and added analytical value of the other three remain hypotheses until tested against the actual data.

## Decision

**The initial ingestion scope is Active Permits and Cleared Permits only.** They are the most direct foundation for lifecycle and aging analysis and provide a bounded scope for validating repeatable extraction, data quality, and downstream analytics.

Development Applications, Preliminary Zoning Reviews, and Address Points are research candidates only. They are excluded from the initial ingestion configuration, source-specific tests, and published data assets. Documenting them does not commit the project to implementing them.

## Trade-off and expansion gate

Starting with two datasets limits the initial view of the wider planning-to-construction process, but lets us deliver and validate a reliable pipeline sooner. Adding sources prematurely could introduce incompatible grains, misleading durations, unmatched populations, or duplicate counts.

Reconsider a deferred dataset only when it answers a specific unanswered question and profiling demonstrates its grain, key/date quality, linkage coverage, ambiguity, and join cardinality. Tests and reconciliation must show that integration does not duplicate measures or distort the existing permit products.

## References

**Official discovery and permit-process guidance**

1. [Toronto Open Data Portal](https://open.toronto.ca/) — starting point for dataset discovery.
2. [City of Toronto — Building Permits](https://www.toronto.ca/services-payments/building-construction/building-permit/) — general background on permit requirements and the application process.
3. [City of Toronto — When Do I Need a Building Permit?](https://www.toronto.ca/services-payments/building-construction/building-permit/before-you-apply-for-a-building-permit/when-do-i-need-a-building-permit/) — common building-permit requirements.
4. [City of Toronto — Building Permit Application & Inspection Status](https://www.toronto.ca/services-payments/building-construction/building-permit/after-you-apply-for-a-building-permit/search-the-status-of-a-building-permit-application/) — published status definitions, including Preliminary Zoning Review status.
5. [Toronto Open Data — Active Permits](https://open.toronto.ca/dataset/building-permits-active-permits/) — active building applications and permits.
6. [Toronto Open Data — Cleared Permits](https://open.toronto.ca/dataset/building-permits-cleared-permits/) — completed/closed permits.
7. [Toronto Open Data — Development Applications](https://open.toronto.ca/dataset/development-applications/) — candidate source for planning-application context.
8. [Toronto Open Data — Preliminary Zoning Reviews](https://open.toronto.ca/dataset/preliminary-zoning-reviews/) — candidate source for zoning-review activity.
9. [Toronto Open Data — Address Points (Municipal), Toronto One Address Repository](https://open.toronto.ca/dataset/address-points-municipal-toronto-one-address-repository/) — candidate geographic/address reference.
10. [Active Permits — CKAN resource preview](https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/building-permits-active-permits/resource/6d0229af-bc54-46de-9c2b-26759b01dd05/view/d66378a6-499b-4f9f-b7aa-bf03c37acb5b) — resource-level link from the original discovery notes; use the Active Permits dataset page if this preview is unavailable.

**City analysis and supplementary reading**

11. [City of Toronto Open Data — Exploring Cleared Building Permits (2017)](https://open.toronto.ca/exploring-cleared-building-permits/) — lifecycle stages, permit revision identifiers, the potential `Geo ID` link to Address Points, and the rationale for considering Active and Cleared together.
12. [City of Toronto — Development Pipeline](https://www.toronto.ca/city-government/data-research-maps/research-reports/planning-development/development-pipeline/) — prior art for project/site-level analysis and avoiding double-counting across approvals.
13. [City of Toronto — Housing Data Hub: Housing Data](https://www.toronto.ca/city-government/data-research-maps/toronto-housing-data-hub/housing-data/) — official planning and permit-related measures; definitions and populations may differ from this project.
14. [City of Toronto — Preliminary Zoning Reviews & Information](https://www.toronto.ca/services-payments/building-construction/building-permit/before-you-apply-for-a-building-permit/preliminary-zoning-reviews-information/) — official zoning-review and property-information guidance.
15. [Ontario Planning Act, section 34](https://www.ontario.ca/laws/statute/90p13#BK66) — legal reference for municipal zoning by-laws.
16. [BVM Homes — Toronto Zoning Review](https://www.bvmhomes.com/blog/toronto-zoning-review) — supplementary practical reading, not primary evidence for City policy.
17. [Town of Banff — What Is a Bylaw?](https://banff.ca/677/What-is-a-Bylaw) — general background only; this Alberta source is not authority for Toronto- or Ontario-specific legal claims.
