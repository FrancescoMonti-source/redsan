# DOCEDS runtime validation

`check_doceds_trimmer_artifact.R` checks an installed, versioned
`edsan-doc-trimmer` artifact through the R integration. It accepts any version
the package validator accepts; pass an optional third argument to require an
exact `artifact_version`. See the script for arguments and required runtime
files.

Model training, annotation, review, and inference policy live in
`edsan-doc-trimmer`. The retired regex trimmer's exploration, family
measurement, prose audit, and manual reviewer workflows are no longer maintained.
