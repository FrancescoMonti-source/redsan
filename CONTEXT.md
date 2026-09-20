# Context: redsan Domain Model

## Core Concepts

### EDSAN
The Clinical Data Warehouse (Entrepôt de Données de Santé) of the Rouen University Hospital (CHU de Rouen), archiving longitudinal patient health records, inpatient stays, biology results, and clinical documents across hospital wards since 2000.

### Document & EHR Identifiers
- **PATID**: Unique patient identifier across all encounters and stays.
- **EVTID**: Encounter / stay identifier (identifies a specific hospitalization or outpatient episode).
- **ELTID**: Unique element identifier for a single clinical document, biology report, or procedure.
- **SEJUM**: Medical service code (Unité Médicale, e.g. `NEPH`, `DIGE`, `PEDI`).
- **SEJUF**: Functional hospital ward / clinic unit code (Unité Fonctionnelle, e.g. `5133`, `398`).
- **RECTYPE**: Clinical document type code (e.g. `CR2AAF`, `CRH2AB`, `DICT`, `IMAG`, `ORDON7`, `XWAYTC`).

### DOCEDS & RECTXT
The clinical document domain within EDSAN. `RECTXT` is the raw text body of a clinical document as extracted from hospital transcription and electronic health record systems.

### Event Bundle (`edsan_event_bundle`)
The primary composite aggregate in `redsan`, packaging all multimodal data for a single patient stay (`event_id`):
- `doceds`: Clinical documents (`RECTXT`, `RECTYPE`, dates, ward codes).
- `pmsi`: Inpatient administrative coding (main stay data, CCAM surgical acts, CIM-10 diagnoses).
- `biol`: Laboratory exam measurements (LOINC codes, raw values, reference ranges).

### Grounding Guarantee
The invariant that any transformation, summarization, or trimming of a `RECTXT` document must map preserved clinical narrative back to exact character intervals `[start, end]` in the raw, unedited source text to ensure auditability in downstream medical coding.

