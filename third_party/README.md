Third-party vendor directory
===========================

This folder contains local, vendored copies of several upstream projects used for development, testing, or local integration. These copies were cloned shallowly and their `.git` directories were removed so they are treated as normal files (not git submodules).

Vendored projects (path -> approximate size)
- cassandra         : 145M
- ffmpeg            : 109M
- influxdb          : 5.3M
- minio             : 40M
- mongo             : 778M
- neo4j             : 138M
- opensearch        : 157M
- maintable          : 153M
- redis             : 22M
- wazuh             : 289M

Why vendored?
- Convenience for local development and offline access.
- Enables targeted modifications to upstream code without adding submodules.

Licenses and provenance
- Each vendored project contains its upstream license file (e.g., `LICENSE`, `COPYING`, etc.). Review those files before redistributing.
- Upstream repositories were cloned from their public GitHub mirrors. If you need exact commit SHAs for attribution, re-clone the project upstream or check the vendor backup (if applied operations created one).

Rename / rebrand script
- A safe, idempotent script was added at `scripts/rename_opensearch_to_density.sh`.
- Dry-run: list files that would be changed without modifying them:
  bash scripts/rename_opensearch_to_density.sh third_party/opensearch

- Apply (destructive): creates a backup under `third_party/opensearch_backup_<timestamp>/` then updates file contents and renames files/dirs:
  bash scripts/rename_opensearch_to_density.sh third_party/opensearch --apply

Notes and next steps
- After applying the rename, update build configuration, tests, and any documentation that references the original project name.
- Consider committing the vendor tree on a feature branch instead of `main` (recommended), due to the large size.
- If you want me to sanitize AWS-specific integrations across these vendors (e.g., remove S3-specific code, test fixtures), tell me which projects and I will scan and prepare a safe change list.

Contact
- If anything in this vendor tree needs to be reverted, I can restore from the backup created by the rename script (if you used --apply) or re-clone the specific project.
