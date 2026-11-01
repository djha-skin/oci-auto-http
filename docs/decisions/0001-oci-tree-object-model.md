# ADR 0001: OCI tree object model and path identity

- **Status:** Proposed (independent review pending in `oci-auto-http-c2c.1.3`)
- **Version:** 1.0.0
- **Date:** 2026-11-01
- **Scope:** Durable path identity, OCI representation, directory enumeration, mutation consistency, and recovery.

## Context

The service exposes a generic byte tree through HTTP mounts backed by OCI registries. OCI Distribution APIs address manifests and blobs in a known repository; they do not provide a portable registry-wide repository listing or a hierarchical filesystem. The mapping must therefore support exact path identity and directory enumeration using ordinary OCI Distribution operations, without a local durable database or assumptions about package formats.

This record defines the storage contract. HTTP status, autoindex rendering, and request authentication are specified separately. “MUST”, “MUST NOT”, and “SHOULD” are normative.

## Decision

### 1. Mount and object identity

A mount identifies an OCI Distribution registry endpoint, repository, and root tag. Its URL prefix is only a routing alias: it is not encoded into an object name. The root tag defaults to `oci-auto-http-tree-v1`; it may be configured to another valid OCI tag when sharing a repository. A mount's identity is the tuple `(registry endpoint, repository, root tag)`. Two URL prefixes configured with the same tuple are aliases of the same tree, not independent copies. A tuple has at most one active writer.

The repository name follows the OCI Distribution repository-name grammar and is passed as a configured value; request path segments MUST NOT be used to construct registry hosts, repositories, or tags. Invalid registry/repository/tag configuration returns the distinct `InvalidMountTarget` result before the listener accepts requests. The root tag is reserved for this tree format. A dedicated repository is RECOMMENDED where unrelated writers or artifacts must be isolated.

A logical tree consists of UTF-8 file names, directories, and exact file bytes. It has no symlinks, hard links, ownership, permissions, or implicit package semantics. Directories are first-class, including empty directories. A file and directory cannot have the same name in one parent.

### 2. URL path normalization and limits

Routing first selects exactly one mount using its configured URL prefix; the prefix is removed before storage-path decoding. The path decoder MUST use the original escaped path, decode percent escapes exactly once, and never apply form decoding (`+` remains `+`). A proxy/server that normalizes paths before routing MUST reject ambiguous encodings or preserve the original escaped path so this contract can be enforced.

A storage path is zero or more slash-separated components. Each component:

- is decoded to valid UTF-8 and preserved byte-for-byte; names are case-sensitive and are not Unicode-normalized (NFC and NFD spellings are distinct);
- MUST NOT be empty, `.` or `..`, contain `/`, `\\`, NUL, or Unicode C0/C1 control characters;
- MUST be at most 255 UTF-8 bytes.

The complete relative path MUST be at most 4096 UTF-8 bytes including separators and at most 128 components. Exceeding any bound is an invalid path, not a truncation. A raw or percent-encoded slash/backslash within a component, malformed percent escape, invalid UTF-8, repeated interior slash, or dot segment MUST be rejected with the distinct `InvalidPath` result. A single trailing slash is a directory spelling and is not part of the stored name; it MUST NOT make a file path valid. The empty relative path names the mount root.

For generated links and canonical examples, encode each UTF-8 component independently: leave only RFC 3986 unreserved bytes literal, percent-encode all other bytes with uppercase hex, and join components with literal `/`. This makes `%` encode as `%25`; decoding is never recursive. The HTTP request-target limit is a separate listener setting and MUST be large enough for the configured mount prefix plus the percent-encoded maximum logical path, or reject the request before storage access.

Sibling ordering is lexicographic by unsigned UTF-8 bytes, not locale, case folding, or Unicode collation. This ordering is stable across implementations.

### 3. OCI representation

All durable state is reachable from the configured root tag. A successful tree read resolves that tag once to a manifest digest, then follows only digest-addressed descriptors from that snapshot. Implementations MUST use the OCI Distribution API for the explicitly configured registry/repository; they MUST NOT depend on `_catalog`, undocumented repository listing, or registry-specific search endpoints.

The root tag points to an OCI image index (`application/vnd.oci.image.index.v1+json`) representing the root directory. Every directory node is an OCI image index. A directory index is either:

- a **leaf**, whose `manifests` descriptors are its sorted immediate file/directory entries; or
- a **branch**, whose descriptors point to child directory-index pages covering non-overlapping, ordered name ranges.

The index-level annotation `io.github.djha-skin.oci-auto-http.node-kind` is `directory-leaf` or `directory-branch`; `io.github.djha-skin.oci-auto-http.format-version` is `1`. In a leaf, each descriptor has exactly one `io.github.djha-skin.oci-auto-http.entry-name` annotation containing the exact UTF-8 child component. The descriptor media type distinguishes a directory index from a file image manifest. In a branch, descriptors point only to directory-index pages and carry the exact annotations `io.github.djha-skin.oci-auto-http.first-name` and `io.github.djha-skin.oci-auto-http.last-name` as inclusive bounds; they MUST NOT carry `entry-name`. The bounds MUST equal the first and last names in the referenced subtree, and ranges MUST be sorted and disjoint. A page's own `node-kind` and format version MUST be validated. Empty directories are empty leaf indexes. A root directory can itself be a leaf or branch.

A leaf or branch MUST contain no more than 128 descriptors and its serialized manifest MUST be no larger than 1 MiB. Implementations MUST split an overflowing page into non-empty, ordered ranges before either limit is exceeded. There is no minimum occupancy for a non-empty page. On deletion, implementations MUST remove empty non-root pages, update affected branch bounds, and collapse any branch with only one child into that child; the root of an empty directory is an empty leaf. This is the complete rebalance contract; implementations need not redistribute entries between otherwise valid non-empty pages. Pages are immutable and addressed by their OCI digest. A descriptor name/bound MUST satisfy the component limits above. Readers MUST reject duplicate names, malformed bounds, cycles, wrong node types, unsupported format versions, and invalid or missing descriptor targets rather than returning an ambiguous tree.

A file is an OCI image manifest (`application/vnd.oci.image.manifest.v1+json`) with `artifactType` `application/vnd.oci-auto-http.file.v1`, exactly one config descriptor, and exactly one uncompressed opaque-byte layer descriptor of media type `application/vnd.oci-auto-http.file.data.v1`. The config blob media type is `application/vnd.oci-auto-http.file.config.v1+json`; its UTF-8 JSON has exactly the fields `formatVersion` (integer `1`) and `size` (non-negative integer equal to the layer descriptor size). The layer is the file's exact bytes (not a tar archive or text conversion). Its descriptor digest and size are authoritative and MUST be checked while streaming. Zero-byte files are valid. Generated digests MUST use SHA-256; a content size MUST fit the OCI descriptor's non-negative signed 64-bit size field.

For clarity, a leaf's `manifests` array contains one descriptor per immediate child, with `entry-name` set on each descriptor. A branch's `manifests` array contains one descriptor per child page, with `first-name` and `last-name` set on each descriptor. Both use the standard OCI descriptor `mediaType`, `digest`, and `size` fields; no path-derived tag or separate registry lookup is implied. Top-level node annotations identify node kind/version; file manifests identify their artifact type as above.

The tree index is the sole authority for names and reachability: file paths are not turned into OCI tags or repository names. Content and index manifests are immutable by digest; only the root tag is mutable. On first use, an absent root tag is initialized by publishing an empty version-1 directory leaf. If the tag exists, its digest and format MUST be validated; an unrelated or unsupported manifest MUST cause a clear error and MUST NOT be overwritten implicitly. All JSON is UTF-8 without a BOM, has no duplicate keys, and uses deterministic serialization for stable output. Digests are always calculated over the exact serialized bytes sent to the registry; clients MUST NOT assume the registry rewrites or canonicalizes manifests.

### 4. Mutation, atomicity, and concurrency

A mutation is copy-on-write. Upload new content blobs and file manifests first, then new/changed directory pages bottom-up. Publish the new root index by putting its manifest under the configured root tag **last**. No individual path tag is updated. Before publication, validate the complete newly constructed path and index metadata. The root-tag update is the commit point: readers that pinned the previous root see the previous tree; readers that resolve the new root see the complete new tree. A reader MUST NOT resolve the root tag separately for each path component in one operation.

OCI Distribution has no portable compare-and-swap or lease operation for tags. Therefore writes to a `(registry, repository, root tag)` MUST be serialized by an exclusive writer. A single server process MUST use a per-target write lock; a CLI writer or second service process MUST use an external coordination mechanism or be stopped while another writer is active. Deployments that cannot guarantee this are read-only. The implementation MUST NOT claim that checking the old root digest and then replacing the tag prevents races; without exclusive coordination, concurrent writers can lose updates.

The registry is required to make each manifest PUT visible as either the old or new complete manifest and to provide read-after-write visibility for the root tag. A backend with weaker semantics is unsupported for writes. After publication, the writer SHOULD resolve the tag and confirm it names the new root digest before reporting success. If the PUT result is ambiguous, the writer MUST inspect the tag while still holding the exclusive lock: the intended digest means committed; the old digest permits a safe retry; any other digest or an unreadable tag returns the distinct `CommitOutcomeUnknown` result and MUST NOT be silently retried as a fresh mutation.

File writes create missing parent directories. Replacing a file replaces its bytes as one tree update. Writing the same bytes to the same path is a logical no-op: it MUST leave exactly one entry and MUST NOT change the root digest. Removing a file removes its leaf entry. Removing an empty directory removes that directory entry; removing a non-empty directory MUST fail unless the caller explicitly requests recursive removal. Recursive removal is one root publication, not a sequence of visible per-file deletions.

### 5. Listing, pagination, and recovery

OCI registry pagination is not used to enumerate paths. The tree's index pages provide deterministic, bounded traversal. A listing pins one root digest and walks the immediate children of one directory in bytewise name order; implementations SHOULD stream entries and MUST avoid buffering file contents to list names. A paged listing accepts a positive entry limit and returns at most that many immediate children plus an opaque, integrity-protected continuation token when more entries remain. The token MUST bind to the pinned root digest, directory identity, and last emitted name. It MUST be rejected with the distinct `InvalidCursor` result if used for a different directory or altered, and MUST NOT silently continue against a newer root. If the pinned root or any required page has been removed by registry retention/garbage collection, return a distinct `StaleCursor` result and let the caller restart the listing. Corrupt reachable OCI tree metadata returns a distinct `IntegrityError`. HTTP mapping of these storage results is defined by the HTTP contract.

The root tag is the commit record and source of truth after a crash. Blobs or manifests uploaded before a failed root publication are unreachable orphans and MUST NOT be exposed by guessing names; registry garbage collection may later remove unreachable content. While the writer still holds its lock, an ambiguous root-tag PUT MUST be resolved by reading the tag and comparing it with the intended and previous root digests, as specified in section 4. After a process crash, however, this design has no durable record of the interrupted operation's intended root; the restarted process MUST treat the current root tag as authoritative and MUST NOT automatically replay or roll back the interrupted operation. The interrupted caller's outcome is `CommitOutcomeUnknown` unless the caller independently confirms the resulting path state. It MUST validate fetched descriptors and blob digests/sizes on use. Corrupt reachable state returns `IntegrityError`; the server MUST NOT invent missing entries or silently rewrite the root. Operators may restore a known-good root digest only as an explicit, coordinated administrative operation. The service does not delete blobs directly because they may be referenced by a committed tree or shared artifacts.

### 6. Worked mappings

Assume mount prefix `/releases`, registry `https://registry.example:5000`, repository `team/releases`, and root tag `oci-auto-http-tree-v1`.

| HTTP path | Storage path | OCI lookup |
| --- | --- | --- |
| `/releases/` | empty path (root directory) | Resolve `team/releases:oci-auto-http-tree-v1` to root-index digest, then enumerate its directory pages. |
| `/releases/acme/tool%20v2.tar.gz` | `acme/tool v2.tar.gz` | Resolve root tag once; find `acme` directory descriptor, then `tool v2.tar.gz` file descriptor. Fetch its file manifest by digest, then stream its single layer blob by digest. |
| `/releases/a%25b/%E2%98%83` | `a%b/☃` | Decode each segment once; look up the exact UTF-8 names in the index. No path-derived tag is formed. |

`/releases/a%2Fb` is rejected because a decoded separator cannot be part of a component. `/releases/%2e%2e/secret` is rejected as a dot segment. `/releases/x//y`, malformed escapes, invalid UTF-8, and a component containing a backslash are rejected. A URL mount prefix never becomes part of the stored path.

## Alternatives considered and rejected

- **One OCI repository per file or directory:** rejected because path depth/name encoding would become repository-name policy, ordinary registry APIs do not provide portable hierarchical listing, and renames/deletes would touch many repositories.
- **One mutable OCI tag per path:** rejected because tags have restrictive syntax/length, make listing registry-specific or unbounded, and multi-tag writes cannot atomically publish a tree update.
- **Put the encoded path in one tag:** rejected due tag length/character limits and because URL paths can exceed tag limits.
- **Store the entire tree in one tar/blob:** rejected because every small update rewrites the full tree, directory listing requires fetching/parsing the whole archive, and replacement is not a bounded copy-on-write operation.
- **Use registry `_catalog` or tag-list APIs to discover paths:** rejected because they enumerate repositories/tags rather than a portable logical tree and commonly require permissions unrelated to reading a mounted path.
- **Use a local database as the index of record:** rejected because state must survive process replacement without a local durable store and be recoverable from OCI alone.
- **Allow unsynchronized concurrent writers:** rejected because the OCI Distribution API provides no portable tag compare-and-swap; pretending a read-before-write check is atomic risks lost updates.

## Consequences

The design makes tree reads snapshot-consistent and makes a single mutation visible through one root-tag publication. It uses standard OCI manifests, indexes, blobs, and digests while adding a small versioned convention for child names and file artifacts. The trade-offs are explicit: writers need exclusive coordination, registry tag reads must be read-after-write consistent, old cursors can expire after garbage collection, and third-party OCI tools will see implementation artifacts in the configured repository. A future incompatible storage format requires a new version/tag and an explicit migration; readers MUST NOT reinterpret an unknown format as version 1.
