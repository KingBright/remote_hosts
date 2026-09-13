# Token Efficiency Contract

Remote Hosts treats model context as a scarce execution resource. The durable machine record and the model-facing view are deliberately different: exact redacted output remains recoverable, while normal Agent responses carry only the information needed for the next decision.

## Design Principle

`raw durable state -> shared semantic compaction -> protocol-specific compact envelope -> Agent`

Both Remote Hosts implementations use the same `remote-hosts-token-output` crate for command classification and text compaction. `remote-hosts-code` keeps only Code-gateway envelope logic; `remote-hosts-mcp` keeps only durable operation/PTY/artifact grouping and cursor logic.

A filter may remove only known routine noise. Unknown text, failures and source locations survive. Exact redacted output remains available with `output_mode=full`; an Agent expands the existing result instead of rerunning the command.

## Historical Waste Patterns

| Pattern | Typical waste | Contract |
| --- | --- | --- |
| Passing test/build floods | Thousands of success/progress lines dominate context | Classify Cargo/Pytest and keep failures + summary; ~25k-token success fixture must return below ~200 tokens |
| Search/Git enumeration | Hundreds or thousands of near-identical lines | Bound `rg`/`grep`, `git log`, `git status`; exact text stays available in full output |
| Terminal repaint/repetition | ANSI, carriage-return progress and repeated heartbeat/status lines | Strip visual controls, keep final CR frame, fold exact consecutive repeats |
| MCP text + structured result duplication | Same large result is paid twice | Compact the structured result itself; text rendering is only a tiny decision summary |
| Success lifecycle bookkeeping | queue/timing/receipt metadata repeats after nothing is actionable | Successful compact results drop transport bookkeeping; pending results keep only actionable delay/block signals |
| Tiny output pages | Another tool call and envelope for every small page | Recognized Code reads consume larger raw pages; exact MCP operation reads consume up to 200 source chunks before one compact view |
| Large-artifact bypass | Normal output is compact, artifact reader reintroduces raw logs | Artifacts use the same classifier; recognized high-noise output can consume up to 1 MiB raw source per compact call while `next_offset` advances over source bytes |
| Repeated workspace/status polling | list/resolve/workspace/wait/result chains resend unchanged state | Agent Work Context is the normal snapshot/wait surface. Legacy workspace-state wait is not exposed by the default Agent profile |
| Rediscovery after timeout/context compression | devices/hosts/workspaces/operations are looked up again | Preserve Workspace, operation, PTY, transfer ids and sequence/offset cursors; no-change/timeout is not permission to replay or rediscover |
| Replay to recover omitted text | expensive commands are rerun only to see exact logs | Use `output_mode=full` on the existing durable result |
| Artifact preview duplication | compact result and metadata repeat the same log prefix | Agent artifact previews are bounded; exact artifact content is explicit and cursor-based |
| Fixed Skill/manual tax | installation, PTY, topology and release manuals enter every task | Hot Skill keeps invariants + workflow router; detailed references load only when needed |
| Repeated validation/builds | follow-up changes trigger the whole pipeline again | Use change-scoped verification and the single iteration report in `ITERATION-EFFICIENCY.md`; observe an existing long build rather than launching a duplicate |
| Uncertain write replay | a dropped receipt causes the mutation to be submitted again | Version checks, semantic idempotency and durable operation identity make “observe original first” the recovery path |

## Shared Semantic Profiles

The shared crate recognizes:

- `cargo test` / `cargo nextest`
- `cargo build` / `cargo check`
- `cargo clippy`
- `pytest`
- `git log`
- `git status`
- `rg` / `grep`
- generic or interactive output

Cargo/Pytest routine progress and passing-test lines collapse to summaries. Remaining diagnostic text and source locations are never head/tail sampled; the enclosing source cursor/page limits bound each response instead. Only positively identified Git/search enumeration records from an explicitly successful command may be sampled. Unknown formats, instructions and diagnostics survive in their original order, including text between omitted records. Running and failed enumerations are not sampled.

Classification recognizes the executable and exact subcommand, not keywords in arguments. Direct executable paths, simple environment assignments and Cargo toolchain selectors are supported. Compound scripts, pipelines, shell expansions, unknown wrappers and interactive commands deliberately use the generic profile; do not split dependency-sensitive scripts merely to force compression. The original MCP frontend classifies the complete stored shell script rather than a potentially truncated activity preview, and removes only its own anchored legacy summary header when full command metadata is unavailable.

Generic output is never semantically truncated by this layer; it receives only visual-equivalent cleanup and exact repetition folding. These rules are shared by the Code gateway and original MCP frontend.

PTY uses the generic profile deliberately. Prompts, menus and unique interactive text must survive.

## Cursor Contract

Compaction never invents a second source of truth.

- Code `terminal_read.cursor` advances over the durable raw log.
- MCP operation/PTY `next_sequence` is the last durable source sequence represented by the compact view.
- Artifact `next_offset` advances by source bytes, not compact output bytes.
- `source_count` / `source_chunk_count` record how much durable input produced the visible view.
- `compression.saved_tokens` estimates tokens removed from the model-facing representation.

A caller can switch to `output_mode=full` at the same raw cursor/sequence/offset without rerunning remote work.

## Fixed Context Contract

The default Agent tool profile is task-oriented rather than administrative. Agent Work Context replaces ordinary workspace-state polling. Admin/full profiles retain legacy/diagnostic tools.

`skills/remote-hosts-agent/SKILL.md` is the hot rule set. Installation, SSH error taxonomy, topology, MinIO, instance sync and detailed MCP procedures live under `skills/remote-hosts-agent/references/` and are read only for the relevant workflow.

When adding a new tool or rule, prefer a short invariant in the hot surface and a detailed cold reference. Do not duplicate a full procedure in both places.

## Regression Gates

1. A synthetic Cargo success log representing at least ~25k tokens compacts below ~200 tokens and reports at least ~24k estimated tokens saved.
2. Compiler/test failures preserve diagnostic text and source locations.
3. PTY compaction preserves unique prompts while folding redraw/repetition noise.
4. Successful, recognized Git/search enumeration remains bounded and advertises full-output recovery; unknown text, middle diagnostics, and all running/failed enumeration records survive.
5. Agent MCP operation output defaults to compact, advances the raw sequence, and `output_mode=full` recovers durable chunks.
6. Code and original MCP frontends consume the same shared classification/compaction implementation.
7. Strict Clippy, formatting and affected crate tests pass before release.
8. Keyword-bearing arguments and compound commands remain generic; complete MCP command metadata takes precedence over activity previews. Long diagnostic-only fixtures retain every unique error and source location.

## Boundary

This reduces terminal/MCP/context overhead, not model reasoning tokens or the total bill by the same percentage. Compression must not hide safety-relevant state, invent success, or replace exact evidence when exact evidence is required. If a compact view is ambiguous, expand the existing durable result.
