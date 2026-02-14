# Apache Iceberg V2 Completion Roadmap

Current status of V2 feature implementation and what's needed for full compliance.

## Current V2 Status: ~70% Complete

### ✅ Implemented V2 Features

**Core V2 Metadata:**
- ✅ Format version 2 support
- ✅ Sequence numbers for ordering commits
- ✅ Last sequence number tracking
- ✅ Last column ID (for schema evolution)
- ✅ Last partition ID (for partition evolution)
- ✅ Default spec ID
- ✅ Snapshot log
- ✅ Metadata log

**Sort Orders:**
- ✅ Sort order specification in metadata
- ✅ Default sort order (unsorted)

**Write Audit Publishing:**
- ✅ Write summaries
- ✅ Added files count
- ✅ Added rows count
- ✅ Added files size

**All V1 Features:**
- ✅ All primitive types
- ✅ All complex types (struct, list, map)
- ✅ All partition transforms
- ✅ Manifest files (Avro)
- ✅ Manifest lists (Avro)
- ✅ Snapshots
- ✅ Table metadata
- ✅ CREATE TABLE
- ✅ INSERT (append)
- ✅ INSERT OVERWRITE

---

## ❌ Missing V2 Features

### 1. Row-Level Deletes (Critical for V2)

**Priority:** 🔴 **HIGH** - Core V2 feature

The biggest gap in V2 compliance. V2's main innovation is support for row-level modifications.

#### Position Deletes
Deletes rows by file position (file path + row index).

**Required:**
- [ ] Position delete file format
- [ ] Position delete manifest entries
- [ ] Position delete manifest creation
- [ ] Merge delete files during reads (or document that reader must handle)

**Implementation complexity:** Medium
**Estimated effort:** 2-3 weeks

#### Equality Deletes
Deletes rows matching specific column values.

**Required:**
- [ ] Equality delete file format
- [ ] Equality delete manifest entries
- [ ] Equality delete manifest creation
- [ ] Equality field specification

**Implementation complexity:** Medium-High
**Estimated effort:** 2-3 weeks

**Files to modify/create:**
- New: `lib/iceberg/delete_file.ex`
- New: `lib/iceberg/delete_manifest.ex`
- Modify: `lib/iceberg/manifest_list.ex` (add delete manifest tracking)
- Modify: `lib/iceberg/snapshot.ex` (handle delete operations)
- Modify: `lib/iceberg/metadata.ex` (track delete manifests)

---

### 2. Delete Operations (Built on Row-Level Deletes)

**Priority:** 🟡 **MEDIUM** - Depends on #1

#### DELETE Operation
Standard SQL DELETE.

**Required:**
- [ ] `Iceberg.Table.delete(table, where_clause, opts)`
- [ ] Generate position or equality delete files
- [ ] Create delete snapshot

**Implementation complexity:** Low (once deletes are implemented)
**Estimated effort:** 1 week

#### UPDATE Operation
Implemented as DELETE + INSERT.

**Required:**
- [ ] `Iceberg.Table.update(table, set_clause, where_clause, opts)`
- [ ] Generate delete files for updated rows
- [ ] Generate insert files for new row versions
- [ ] Single snapshot for atomic update

**Implementation complexity:** Medium
**Estimated effort:** 1-2 weeks

#### MERGE Operation
Conditional INSERT/UPDATE/DELETE.

**Required:**
- [ ] `Iceberg.Table.merge(table, source, on_clause, when_clauses, opts)`
- [ ] Complex matching logic
- [ ] Generate appropriate delete/insert files
- [ ] Single snapshot for atomic merge

**Implementation complexity:** High
**Estimated effort:** 2-3 weeks

**Files to modify:**
- Modify: `lib/iceberg/table.ex` (add new operations)
- New: `lib/iceberg/operations/delete.ex`
- New: `lib/iceberg/operations/update.ex`
- New: `lib/iceberg/operations/merge.ex`

---

### 3. Schema Evolution

**Priority:** 🟡 **MEDIUM** - Useful but not V2-specific

While tracked in V2 metadata (last-column-id), schema evolution works in V1 too.

**Required:**
- [ ] Add column
- [ ] Drop column
- [ ] Rename column
- [ ] Update column type (limited)
- [ ] Reorder columns
- [ ] Field ID assignment for new columns

**Implementation complexity:** Medium
**Estimated effort:** 1-2 weeks

**Files to modify:**
- Modify: `lib/iceberg/schema.ex` (add evolution functions)
- Modify: `lib/iceberg/metadata.ex` (track schema changes)
- New: `lib/iceberg/schema_evolution.ex`

---

### 4. Partition Evolution

**Priority:** 🟢 **LOW** - Advanced feature

**Required:**
- [ ] Change partition spec over time
- [ ] Track partition spec versions
- [ ] Partition spec ID management
- [ ] Migrate data to new partition layout (optional)

**Implementation complexity:** Medium
**Estimated effort:** 1-2 weeks

**Files to modify:**
- Modify: `lib/iceberg/schema.ex` (partition spec evolution)
- Modify: `lib/iceberg/metadata.ex` (track partition specs)

---

### 5. Sort Order Enforcement

**Priority:** 🟢 **LOW** - Optional optimization

**Required:**
- [ ] Enforce sort order during writes
- [ ] Track sort order in data files
- [ ] Multi-column sorting support

**Implementation complexity:** Low
**Estimated effort:** 1 week

**Files to modify:**
- Modify: `lib/iceberg/table.ex` (add sort enforcement option)
- Modify: `lib/iceberg/manifest.ex` (track sort order)

---

## Implementation Priority Order

### Phase 1: Core V2 Compliance (Critical)
**Goal:** Full V2 specification compliance
**Time:** 4-6 weeks

1. **Row-level deletes** (position + equality)
2. **DELETE operation**
3. **UPDATE operation**

**Why:** These are the defining features of V2. Without them, you're essentially running V1 with V2 metadata.

### Phase 2: Essential Operations (Important)
**Goal:** Complete CRUD operations
**Time:** 2-3 weeks

4. **MERGE operation**
5. **Schema evolution** (add/drop/rename columns)

**Why:** Makes the library fully functional for real-world use cases.

### Phase 3: Advanced Features (Nice to have)
**Goal:** Advanced table management
**Time:** 2-3 weeks

6. **Partition evolution**
7. **Sort order enforcement**

**Why:** Optimization features that improve performance but aren't required for basic V2 compliance.

---

## Total Estimated Effort

**Full V2 Compliance:** 8-12 weeks of focused development

**Breakdown:**
- Phase 1 (Core V2): 4-6 weeks
- Phase 2 (Essential): 2-3 weeks  
- Phase 3 (Advanced): 2-3 weeks

---

## What You Have vs. What's Missing

### Write Operations

| Operation | Status | V2 Requirement |
|-----------|--------|----------------|
| CREATE TABLE | ✅ | V1 |
| INSERT (append) | ✅ | V1 |
| INSERT OVERWRITE | ✅ | V1 |
| DELETE | ❌ | **V2** |
| UPDATE | ❌ | **V2** |
| MERGE | ❌ | V2 |

### Data File Types

| File Type | Status | V2 Requirement |
|-----------|--------|----------------|
| Data files (Parquet) | ✅ | V1 |
| Position delete files | ❌ | **V2** |
| Equality delete files | ❌ | **V2** |

### Manifest Types

| Manifest Type | Status | V2 Requirement |
|---------------|--------|----------------|
| Data manifests | ✅ | V1 |
| Delete manifests | ❌ | **V2** |

---

## Minimal V2 Compliance

If you want to claim "V2 support" with minimal work, implement **Phase 1 only**:

**Minimum viable V2:**
1. Position deletes
2. Equality deletes  
3. DELETE operation

This gives you:
- ✅ All V2-required metadata fields (already done)
- ✅ Delete file support (new)
- ✅ Row-level modification capability (new)
- ✅ Basic CRUD operations (CREATE/READ/UPDATE/DELETE)

**Time:** ~4-6 weeks

**Tradeoff:** No UPDATE/MERGE convenience operations, but DELETE works and you can implement UPDATE as DELETE + INSERT at the application level.

---

## Recommendations

### For Production Use Today

**Current state is production-ready for:**
- ✅ Write-heavy workloads (append, overwrite)
- ✅ Immutable data lakes
- ✅ Event streaming to Iceberg
- ✅ ETL pipelines (extract, transform, load)
- ✅ Data warehouse staging tables

**Not ready for:**
- ❌ OLTP-style updates/deletes
- ❌ CDC (change data capture) with updates
- ❌ Mutable dimension tables

### Short-term Path (Next Release)

**Focus on Phase 1** (4-6 weeks):
1. Implement position deletes
2. Implement equality deletes
3. Add DELETE operation
4. Release as **v0.2.0** with "Full V2 Compliance"

### Long-term Path (Future Releases)

**v0.3.0:** Add UPDATE and MERGE  
**v0.4.0:** Add schema evolution  
**v0.5.0:** Add partition evolution and sort enforcement

---

## Questions to Consider

1. **Do you need DELETE/UPDATE now?**
   - If no: Current library is sufficient
   - If yes: Prioritize Phase 1

2. **What's your primary use case?**
   - Append-only data lake: Current version is perfect
   - Mutable tables: Need DELETE/UPDATE (Phase 1)
   - Complex MERGE logic: Need Phase 2

3. **Timeline?**
   - Need V2 compliance soon: Focus on minimal viable (Phase 1)
   - Can wait: Implement all phases for complete V2

---

**Last Updated:** 2026-02-14  
**Current Version:** 0.1.0  
**V2 Compliance:** ~70% (metadata complete, operations incomplete)
