# Document Library

Document Library is a self-hosted library under development. Its shared Python
core currently provides catalog categories, invitation-based authentication,
upload permissions, fenced jobs, immutable object publication, backup sets and
isolated restoration. The complete web application and ingestion pipeline are
not yet released.

Categories include novel text, novel scans, webtoons, comics and documents.
Text and image editions coexist. Title matching ignores separators while
preserving digits, English letters and Hangul; matching alone does not merge
distinct works or files.

## Validate the exported core

Use Python 3.14 and uv with the included dependency lock:

```sh
uv sync --locked
PYTHONPATH=packages/core uv run --locked python -m unittest discover -s tests -v
```

Tests use synthetic data in temporary directories. The migration tests create
fresh databases; restore tests verify complete synthetic backup sets after the
original source is removed. No existing library, NAS credentials or private
workspace is required. These tests do not enable a service or demonstrate
production power-loss recovery.

A restored database deliberately disables authentication and job execution.
Operator reconciliation and new credentials are required before a future
production activation workflow. Offline restore verification checks current
bytes and blocked state, not successful disk flush or activation authority.

This source tree contains no private Git history. PUBLIC_EXPORT.json inventories
the exported bytes; it is a review aid, not a signature, secret-scan guarantee,
release approval or service activation receipt.

## Build the Document Library interface

The shared web source supports separate public and private product names. Select
`--brand public` to build the page title, offline page, PWA manifest and interface
configuration as **Document Library**. The private operating build defaults to
**Novel Library**.

Provide the official `pdfjs-dist-6.4.299.tgz` and `tus-js-client-4.3.1.tgz`
archives named in `apps/web/dependencies.lock.json`. The offline builder checks
the pinned archive contents and retains their license files; it does not download
or install packages. Use absolute paths and a new output directory outside the
repository:

```sh
uv run --locked python ops/build_web_assets.py \
  --source /absolute/document-library/apps/web \
  --archives /absolute/npm-archives \
  --output /absolute/document-library-web \
  --brand public
```

Building assets does not start or configure the server.

## License and release status

License: **MIT**. Copyright (c) 2026 silesfly. See [LICENSE](LICENSE).

This project remains under development. The MIT license choice does not mean
that the complete application has been released or production-certified.
Dependency license review, complete application validation and release approval
remain pending.
