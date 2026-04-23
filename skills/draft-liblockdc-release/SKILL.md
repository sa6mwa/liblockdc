---
name: draft-liblockdc-release
description: Validate a tagged liblockdc release from the repository root, run the full local test and make world gates, verify dist/ artifacts and checksums, then draft or publish the matching GitHub release with gh once the tag and commit are on origin. Use when preparing a liblockdc GitHub release from a vX.Y.Z tag on HEAD.
---

# Draft liblockdc Release

Use this skill from the `liblockdc` repository root when a release should be validated locally first and only drafted on GitHub after the tagged commit has been pushed to `origin`.

## Workflow

1. Run the helper in the foreground:

```bash
skills/draft-liblockdc-release/scripts/draft_liblockdc_release.sh draft
```

2. The helper must succeed in this order:
   - confirm `git`, `gh`, `make`, `ctest`, `sha256sum`, and `tar` are available
   - confirm `gh` is authenticated and can access the `origin` GitHub repository and API
   - confirm `HEAD` has exactly one `vX.Y.Z` tag
   - compare it to the previous release tag merged into `main`
   - run `make clean`
   - run `make test-all`
   - run `make test-e2e`
   - run `make world` in the foreground
   - rerun the release-package tests
   - verify `dist/` filenames and `CHECKSUMS`
   - only then check whether the tagged commit and tag are present on `origin`
   - create a GitHub draft release and upload every file named in `CHECKSUMS` plus the checksum file itself

3. If the helper stops because the current tag is not a semantic bump over the previous release tag, ask the user whether the tag is correct. Only continue if the user explicitly confirms. Then rerun:

```bash
skills/draft-liblockdc-release/scripts/draft_liblockdc_release.sh draft --allow-nonbump-tag
```

4. If the helper stops after local validation because `origin/main` or the release tag is not present yet, ask the user to push the commit and tag. After the user confirms that push is done, continue without rerunning `make world`:

```bash
skills/draft-liblockdc-release/scripts/draft_liblockdc_release.sh resume-draft
```

5. After the draft exists, ask the user whether to publish it or keep it as a draft.
   - To publish:

```bash
skills/draft-liblockdc-release/scripts/draft_liblockdc_release.sh publish
```

   - If the user wants to keep it as a draft, stop there.

## Required behavior

- Do not pull credentials from elsewhere. Use the existing `gh` authentication only.
- Fail immediately if `HEAD` is not tagged with a proper `vX.Y.Z`.
- Fail immediately if tests, `make world`, package tests, artifact checks, or checksum validation fail.
- Treat the local validation phase as the release-readiness gate before anything must be on `origin`.
- Use the exact tag value as the GitHub release title.
- Derive the release notes from the commit message on `HEAD`.
- Treat `dist/liblockdc-<version>-CHECKSUMS` as the source of truth for uploaded assets.

## Notes

- This skill is intentionally deterministic. It should not silently skip gates.
- `make world` is expensive and must be run in the foreground so the invoking agent keeps contact with the run.
- `resume-draft` exists specifically so a push to `origin` does not force another full rebuild.
