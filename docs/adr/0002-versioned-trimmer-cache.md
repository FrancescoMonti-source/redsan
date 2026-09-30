---
status: accepted
date: 2026-09-30
---

# Versioned trimmer cache with explicit discovery

## Context

[ADR 0001](0001-decouple-drbert-trimmer-from-redsan.md) makes the trimmer a
versioned artifact owned by `edsan-doc-trimmer`. The first cache design gave
that artifact one installation slot (`<cache>/v1`). The `v1` named the cache
layout, not the artifact, so installing a new archive silently replaced the
old one, two artifact versions could not coexist, and switching back meant
finding the old archive again. Discovery also fell back to a sibling
development checkout, so a trimming call could run an artifact nobody
installed, and nothing told the caller which artifact had run.

## Decision

Each installed artifact lives in its own folder, named exactly after the
`artifact_version` of its manifest, under a single cache root. Discovery is an
ordered, explicit rule. Every trimming call says which artifact it used.

### Cache layout (contract)

The **cache root** is `tools::R_user_dir("edsan_doc_trimmer", "cache")`,
returned by `edsan_trimmer_cache_dir()`. The root does not contain a layout
suffix such as `v1`. `R_user_dir()` resolves it as
`<base>/R/edsan_doc_trimmer`, where `<base>` is the first of these that is set:

| Platform | `<base>` |
|---|---|
| any | `R_USER_CACHE_DIR` |
| any | `XDG_CACHE_HOME` |
| Windows | `%LOCALAPPDATA%\R\cache` |
| macOS | `~/Library/Caches/org.R-project.R` |
| other Unix | `~/.cache` |

(On Windows the default is `%LOCALAPPDATA%\R\cache\R\edsan_doc_trimmer`; on
Linux `~/.cache/R/edsan_doc_trimmer`.)

```
<root>/
  1.2.0/
    model.onnx
    tokenizer.json
    trim_batch_service.py
    artifact.json
  1.3.0/
    ...
```

An **installation** is a direct child folder of the root that satisfies all of:

1. its name matches `^[0-9]+(\.[0-9]+)*$` (a dotted numeric version);
2. it contains the four runtime files above and `artifact.json` is a valid
   manifest (`artifact_version >= 1.2.0`, `worker_contract = "model-only-v1"`);
3. the manifest's `artifact_version` equals the folder name, character for
   character.

Every other child is ignored: `v1.3` (not a version), a folder whose manifest
disagrees with its name, incomplete folders, and installer staging or backup
folders (`trimmer-stage-*`, `trimmer-backup-*`). Versions are ordered as
numeric versions (`1.10.0 > 1.9.0`).

The legacy single-slot install `<root>/v1/` (runtime files directly inside)
is not an installation by the rules above. It is read only by the fallback
described below.

### Discovery order (contract)

When no `model_dir` is passed, the artifact directory is the first of:

1. `EDSAN_TRIMMER_PATH`, or `REDSAN_TRIMMER_PATH` when the former is unset. It
   names an artifact directory, or a `model.onnx` inside one. It may point
   anywhere, and it is the only way to select an artifact outside the cache
   (for example a development checkout's export). If it does not contain
   `model.onnx`, it is an error; like a pin, an explicit path never falls
   through, because falling through would silently run another artifact.
2. `EDSAN_TRIMMER_VERSION`: the installation whose folder name equals the value
   exactly. A value with no matching installation is an error that lists the
   installed versions; it never falls through to another version.
3. The highest installation.
4. The legacy slot `<root>/v1`, when it holds a valid artifact. It is used with
   a message asking the user to reinstall into the versioned layout.
5. An error with installation instructions.

There is no implicit development-checkout fallback and no automatic download.

The Python side of `edsan-doc-trimmer` that resolves a model directory
without R (edsan-doc-trimmer#7) must follow the same layout, folder rules, and
order, so both runtimes select the same artifact for the same environment.
`redsan` passes the resolved directory to the worker as `--onnx_dir`; the worker
is never asked to discover it.

### Installer

`edsan_install_trimmer(zip_path, overwrite = FALSE)`; `dest_dir` is removed.

- The archive is extracted and validated in a staging folder inside the cache
  root. The target is `<root>/<artifact_version>`, taken from the manifest.
- An `artifact_version` that is not a dotted numeric version is rejected,
  because it could not be discovered.
- Same version and same content digest (`doceds_onnx_spec()$digest`): no-op with
  a message.
- Same version and a different digest: error, unless `overwrite = TRUE`, which
  replaces that folder with rollback if publishing fails.
- The success message states the version, the path, and whether the artifact is
  now the selected one (and, if not, which is, and why).

### Listing and transparency

- `edsan_trimmer_versions()` returns `version`, `path`, and `selected`, highest
  version first, where `selected` follows the full discovery order.
- Each top-level `trim_doceds_onnx()` call that runs the worker prints
  `edsan-doc-trimmer <version> (<path>)` once, not per chunk or per bundle.
- `doceds_onnx_spec()` gains a `path` field. `digest` remains the field to
  compare between runs; it is computed and cached as described in AGENTS.md.

## Consequences

- Breaking change: `dest_dir` is removed and the cache root path changes
  (`<root>/v1` becomes `<root>`). Existing `v1` installs keep working through
  the legacy fallback until reinstalled. Callers of `edsan_trimmer_cache_dir()`
  that appended files to it must adapt.
- Upgrading is installing: the highest installed version runs, and rolling back
  is `EDSAN_TRIMMER_VERSION` rather than hunting for an old archive.
- Reproducibility is stated, not implied: the announced line, the spec `path`,
  and the digest identify what ran.
- Runs that relied on the development-checkout fallback must now name the
  checkout with `EDSAN_TRIMMER_PATH`.
