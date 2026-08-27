# ICCA source access and database map

This note records what has been verified empirically about the ICCA reporting database used by `redsan`. It distinguishes database facts from interpretations inferred from observed data.

## Access path

`redsan` accesses ICCA natively from R through DBI + ODBC + FreeTDS. The high-level pseudonymized path starts from an EDSaN EVTID:

```text
EVTID
  -> EDSaN CT
  -> IEP (transient)
  -> CISReportingDB.dbo.D_Encounter.encounterNumber
  -> ICCA internal keys
  -> target ICCA object
  -> EVTID reattached to the result
```

The real IEP is used as a transient lookup key. `query_icca()` remains the unrestricted read-only SQL interface; `get_icca()` is the EVTID-oriented interface.

## Database, schemas, tables, and views

`CISReportingDB` is the SQL Server database. A schema is a namespace inside the database, analogous to `package::function` namespaces in R: two objects can have the same object name if they live in different schemas.

The catalog examined contained 542 user tables/views:

```text
schema   tables   views
CUS          10       0
dbo         299      88
DAR           0     145
-----------------------
total       309     233
```

`dbo` therefore contains the physical relational model plus some views. `DAR` contains reporting views. `CUS` contains additional tables; its precise semantic role has not been established from official documentation.

A view is best understood here as a saved SQL transformation, analogous to a saved R pipeline. It does not normally represent another independent copy of the same data.

## The Pt -> Patient reporting pattern

For several important clinical domains the following pattern was verified from the SQL view definitions:

```text
dbo.PtAssessment
      -> DAR.PtAssessment
      -> DAR.PatientAssessment

dbo.PtMedication
      -> DAR.PtMedication
      -> DAR.PatientMedication

dbo.PtLabResult
      -> DAR.PtLabResult
      -> DAR.PatientLabResult

dbo.PtVentilation
      -> DAR.PtVentilation
      -> DAR.PatientVentilation
```

The `DAR.Pt*` views enrich the underlying `dbo.Pt*` rows with ICCA dictionaries and labels. For example, numeric intervention/attribute identifiers become readable labels.

The `DAR.Patient*` views then start from the corresponding `DAR.Pt*` view and add encounter/patient and care-context information such as unit, bed, care provider, census period, and document context. They use a mixture of inner and left joins.

For the validated encounter (`encounterId = 3`):

```text
PtAssessment       5423 rows
PatientAssessment  5423 rows
PtMedication       3360 rows
PatientMedication  3360 rows
```

Direct checks found zero `PtAssessment` rows and zero `PtMedication` rows missing from the corresponding `Patient*` views for this encounter. This demonstrates lossless enrichment for this tested encounter, not a formal global guarantee.

## Assessment and medication row structure

`PtAssessment` and `PtMedication` are long-form structures. A database row is not necessarily one complete clinical observation or one medication administration.

For the validated encounter, `cisPtInterventionId` behaved consistently as an intervention-instance grouping key:

```text
one cisPtInterventionId
  -> one interventionId
  -> one chartTime
  -> one or more attribute/value rows
```

Observed examples included a single SpO2 instance represented by one row, a contact-person intervention represented by four attribute rows, and medication interventions represented by many attribute rows (dose, route, formulation, material, etc.).

Across the full validated encounter:

```text
PtAssessment: 5423 rows -> 3084 distinct cisPtInterventionId
PtMedication: 3360 rows -> 232 distinct cisPtInterventionId
```

No rows lacked `cisPtInterventionId`, and no tested `cisPtInterventionId` mapped to more than one `interventionId` or more than one `chartTime` in either domain.

This is strong empirical evidence for grouping semantics, but it is not presented as vendor-documented meaning.

## Relations available from SQL Server metadata

Two complementary relation systems were extracted from SQL Server itself.

### Foreign keys

Foreign keys describe declared table-to-table column relations in the physical model. The extracted metadata contained 603 foreign-key column rows (602 constraints; one constraint spans two columns). They were all between `dbo` objects.

`dbo.PtAssessment`, for example, has declared links to objects such as:

```text
encounterId     -> dbo.D_Encounter.encounterId
interventionId  -> dbo.D_Intervention.interventionId
attributeId     -> dbo.D_Attribute.attributeId
clinicalUnitId  -> dbo.D_ClinicalUnit.clinicalUnitId
careProviderId  -> dbo.D_CareProvider.careProviderId
bedId           -> dbo.D_Bed.bedId
materialId      -> dbo.D_Material.materialId
siteId          -> dbo.D_Site.siteId
systemId        -> dbo.D_System.systemId
```

The same broad pattern occurs across many clinical tables.

### View dependencies

SQL expression dependencies describe which objects are used to build views. The extracted metadata contained 904 dependency rows and covered all 145 `DAR` views.

For example:

```text
DAR.PatientAssessment
  -> DAR.PtAssessment
  -> DAR.AllEncounter
  -> DAR.PtCensus
  -> DAR.CareProvider
  -> DAR.Material
  -> dbo.D_Bed
  -> dbo.D_ClinicalUnit
  -> dbo.D_Day
  -> dbo.D_Document
  ...
```

and `DAR.PtAssessment` depends on `dbo.PtAssessment` plus dictionary/descriptor objects.

A view dependency tells us which objects are used, but not necessarily the exact join columns. Exact join predicates remain available from the view definition when needed.

## ICCA is a graph, not a fixed source list

The reporting database contains hundreds of related objects. Only a subset contains `encounterId` directly. This does **not** imply that other clinical objects are unrelated to an encounter: they may be linked through `patientId`, `episodeId`, `systemId`, order/intervention/document identifiers, or longer relation paths.

Accordingly, `redsan` should not treat `assessment`, `medication`, and `encounter` as a closed whitelist.

The intended interface is:

```r
icca_catalog()
icca_describe("DAR.PatientVentilation")
icca_relations("DAR.PatientVentilation")
query_icca(...)
get_icca(evtids, source = "DAR.PatientVentilation")
```

`query_icca()` provides unrestricted read-only access to any object. `get_icca()` resolves EVTIDs to ICCA anchor keys and can directly retrieve any object exposing a compatible `D_Encounter` key. Objects requiring indirect traversal remain accessible through `query_icca()` while their relation paths are investigated.

The short aliases `assessment` and `medication` are conveniences only; they are not a whitelist.

## What remains unknown

The structural map is now substantially understood, but several semantic questions remain intentionally open:

- the official business meaning of many individual ICCA objects;
- the precise vendor-defined semantics of internal identifiers such as `ongoingId`, `descriptorId`, `valueInstanceId`, and some order/intervention identifiers;
- whether the observed lossless `Pt* -> Patient*` enrichment holds globally or only for the encounters tested;
- how best to choose or traverse indirect relation paths when an object does not expose a direct `D_Encounter` anchor;
- the intended semantic role of the `CUS` schema;
- which of multiple overlapping reporting views is preferable for a given clinical analysis.

No useful object descriptions were found in SQL Server extended properties, so semantic understanding must come from view definitions, relations, observed values, and external/vendor documentation when available.
