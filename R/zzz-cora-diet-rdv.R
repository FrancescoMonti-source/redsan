# Extend CORA Diet retrieval to RDV-linked documents -------------------------
#
# CORA Diet documents are stored in T_DOCUMENT/T_DOCUMENT_MEMO regardless of
# event type, but the route from an IEP to T_DOCUMENT differs:
# - H (hospitalisation): MVTUS.NOSEJ -> MVTUS.NOMVTUS = T_DOCUMENT.NOEVT
# - R (rendez-vous):     T_EVT_RDV.NOSEJ -> T_EVT_RDV.NOEVT = T_DOCUMENT.NOEVT
#
# This file is intentionally collated after cora-diet.R and replaces the
# H-only query helper while preserving the public get_cora_diet() interface.

.cora_diet_documents_sql <- function(ieps) {
  ieps <- .cora_validate_ieps(ieps)
  quoted <- paste0("'", ieps, "'", collapse = ", ")

  paste0(
    "SELECT *\n",
    "FROM (\n",
    "  SELECT DISTINCT\n",
    "    m.NOSEJ AS IEP,\n",
    "    d.NODOCUMENT,\n",
    "    d.TYPEDOC,\n",
    "    d.NOEVT AS CORA_NOEVT,\n",
    "    d.DTDOC,\n",
    "    d.TIMECREATE,\n",
    "    d.REDACTEUR,\n",
    "    m.NOUSHEB,\n",
    "    m.NOUSRESP\n",
    "  FROM ICSF.MVTUS m\n",
    "  JOIN ICSF.T_DOCUMENT d\n",
    "    ON d.NOEVT = m.NOMVTUS\n",
    "   AND d.TYPEEVT = 'H'\n",
    "  WHERE m.NOSEJ IN (", quoted, ")\n",
    "    AND d.NOSOUSVOLET = 443\n",
    "    AND d.ETATDOC = 1\n",
    "\n",
    "  UNION ALL\n",
    "\n",
    "  SELECT DISTINCT\n",
    "    r.NOSEJ AS IEP,\n",
    "    d.NODOCUMENT,\n",
    "    d.TYPEDOC,\n",
    "    d.NOEVT AS CORA_NOEVT,\n",
    "    d.DTDOC,\n",
    "    d.TIMECREATE,\n",
    "    d.REDACTEUR,\n",
    "    CAST(NULL AS VARCHAR2(10)) AS NOUSHEB,\n",
    "    CAST(NULL AS VARCHAR2(10)) AS NOUSRESP\n",
    "  FROM ICSF.T_EVT_RDV r\n",
    "  JOIN ICSF.T_DOCUMENT d\n",
    "    ON d.NOEVT = r.NOEVT\n",
    "   AND d.TYPEEVT = 'R'\n",
    "  WHERE r.NOSEJ IN (", quoted, ")\n",
    "    AND d.NOSOUSVOLET = 443\n",
    "    AND d.ETATDOC = 1\n",
    ")\n",
    "ORDER BY IEP, DTDOC"
  )
}

.cora_query_diet_documents <- function(connection, ieps) {
  DBI::dbGetQuery(connection, .cora_diet_documents_sql(ieps))
}
