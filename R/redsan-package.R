#' redsan: Retrieve and normalize EDSAN warehouse data
#'
#' `redsan` is the source-access and normalization layer for the EDSAN data
#' warehouse. It records the source contract of each supported module, retrieves
#' source payloads with adaptive batching, and converts them to stable R tables
#' while preserving native identifiers and source time fields.
#'
#' @section Start with the source registry:
#' Call [edsan_source_catalog()] to inspect the modules and normalized tables supported
#' by the installed package. The registry describes row grain, identifiers,
#' query date keys, batching keys, and whether source time is a point or an
#' interval. It is a registry of the sources implemented by `redsan`, not an
#' exhaustive catalogue of EDSAN.
#'
#' @section Retrieve and normalize:
#' [edsan_get()] is the main retrieval entry point. It batches requests by source
#' time and native identifiers when needed. By default, it also applies the
#' module normalizer:
#'
#' * [doceds_normalize()] for documents;
#' * [pmsi_normalize()] for PMSI stays, actes, and diagnoses;
#' * [biol_normalize()] for biology results;
#' * [viro_normalize()] for virology results.
#'
#' Set `process = FALSE` in [edsan_get()] when the raw response is the artifact
#' to retain or normalize later.
#'
#' @section Use reference mappings:
#' [edsan_reference_catalog()] lists the code mappings distributed with the package.
#' [label_pmsi()] adds the packaged CIM-10 and CCAM/CDAM labels to normalized
#' PMSI objects and can be applied to older artifacts. [pmsi_normalize()] reuses
#' it so new normalized outputs are labelled automatically.
#' [label_biol()] similarly adds packaged analyte labels to normalized biology
#' rows, and [biol_normalize()] reuses it for new outputs. [edsan_reference()]
#' returns any normalized mapping for explicit joins. Enrichment preserves
#' native codes and row grain; unmatched codes receive a missing label.
#'
#' @section Work at event level:
#' [edsan_get_event_bundle()] and [edsan_get_event_bundles()] retrieve several normalized
#' modules for one or more `EVTID` values. When the normalized sources are
#' already available locally, use [edsan_event_bundle()] or
#' [edsan_event_bundles()] to partition them without another EDSAN request.
#' [edsan_render_event_bundle()] serializes a bundle to neutral JSON without selecting
#' clinical content.
#'
#' @section Translate identifiers:
#' [edsan_ct()] resolves explicit IPP, IEP, PATID, or EVTID correspondence and
#' returns joinable semantic identifier columns. Set `identity = TRUE` to append
#' the associated patient identity while preserving the requested direct-match
#' status and multiplicity.
#'
#' @section Query CORA and ICCA:
#' [cora_query()] and [icca_query()] execute read-only queries against their
#' respective systems. [icca_get()] retrieves ICCA data by EDSaN EVTID.
#'
#' @section Source and downstream responsibilities:
#' `redsan` owns source mechanics: retrieval, batching, parsing, normalized table
#' shape, identifiers, and source time fields. Clinical concepts, cohort rules,
#' measurements, and model-specific selection belong in downstream projects.
#' No function in this package should be treated as assigning clinical meaning
#' merely from the shape of a source table.
#'
#' Live retrieval requires the optional EDSAN client package `d2imr` in the
#' calling environment. Normalization, registry inspection, local bundle
#' building, and rendering do not require a live EDSAN connection.
#'
#' @seealso
#' [edsan_source_catalog()], [edsan_reference_catalog()], [label_pmsi()],
#' [label_biol()], [edsan_get()], [edsan_ct()], [edsan_get_event_bundle()]
#'
#' @keywords internal
#' @importFrom magrittr %>%
"_PACKAGE"
