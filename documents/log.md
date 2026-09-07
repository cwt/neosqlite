---
type: log
title: "NeoSQLite Knowledge Base Bundle Log"
description: "Chronological update log tracking structural modifications, additions, and refactoring within the NeoSQLite OKF knowledge bundle."
tags:
  - log
  - bundle
  - changelog
timestamp: 2026-09-07T00:00:00Z
version: "1.15.1"
lifecycle: active
---

# NeoSQLite Knowledge Base Bundle Log

This document records the chronological history of structural changes, additions, deprecations, and refactoring operations performed on the NeoSQLite Open Knowledge Format (OKF) bundle.

---

## 2026-09-07 — Migration to Google Open Knowledge Format (OKF) v0.2

### Summary
Migrated the entire `documents/` tree into a formal **Google Open Knowledge Format (OKF) v0.2** bundle, retired the legacy monolithic `CHANGELOG.md`, modernized all filenames to lowercase kebab-case, and established directory indexing with GitHub symlinks.

### Key Modifications
1. **Removed `CHANGELOG.md`**:
   - Retired the 3,086-line redundant root `CHANGELOG.md` file.
   - Preserved full historical release details in version-specific release notes under `documents/releases/` (v1.0.0 through v1.15.1).
   - Created `documents/releases/index.md` listing all 52 releases in reverse chronological order with direct links, dates, and summaries.

2. **Renamed ALL-CAPS Files to Lowercase Kebab-Case**:
   - Replaced all uppercase and underscore-delimited filenames across `documents/` with modern lowercase kebab-case naming.
   - Renamed `documents/TODO/` directory to `documents/todo/`.
   - Recorded all renames via Mercurial (`hg rename`) to preserve full file commit history.

3. **Injected OKF v0.2 YAML Frontmatter**:
   - Added metadata blocks (`type`, `title`, `description`, `tags`, `timestamp`, `version`, `lifecycle`) to all 24 concept and roadmap documents.
   - Added metadata blocks (`type: release_notes`) with extracted descriptions and commit timestamps to all 52 release notes.

4. **Created Directory Indexes & Progressive Disclosure Maps**:
   - `documents/index.md`: Root bundle catalog organizing documents into Architecture, Core Features/APIs, Internal Subsystems, Operations/Testing, and Subdirectories.
   - `documents/releases/index.md`: Chronological table of all release notes.
   - `documents/todo/index.md`: Catalog of future roadmap proposals (geospatial, vector search).

5. **Configured GitHub Rendering Symlinks**:
   - Added relative symbolic links `README.md -> index.md` in `documents/`, `documents/releases/`, and `documents/todo/` for automatic directory rendering on GitHub.

6. **Updated Cross-References**:
   - Updated root `README.md` documentation table and changelog link.
   - Updated internal relative links across all concept documents to point to the new lowercase kebab-case paths.
