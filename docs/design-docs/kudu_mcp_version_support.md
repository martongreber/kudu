# Kudu MCP Server: Protocol Version Support (2025-06-18)

## Summary

The PoC server (`src/kudu/tools/tool_action_mcp.cc`) negotiates MCP protocol
version `2025-06-18` and accepts `2024-11-05` and `2025-03-26`. For the profile
it actually exposes -- tools capability over stdio, read-mostly, no auth --
the server is already a compliant `2025-06-18` server. Almost everything the
revision added is either optional (`MAY`) or belongs to transports and
subsystems the PoC deliberately does not expose (HTTP, OAuth, resources,
prompts, completion).

Accurate claim to make: "implements MCP 2025-06-18, tools capability over
stdio." That is true today.

## What 2025-06-18 introduced vs. our status

| Change | Applies to us (stdio, tools-only)? | Status |
|---|---|---|
| Remove JSON-RPC batching | Yes | Done -- we reject arrays as invalid |
| Structured tool output (outputSchema + structuredContent) | Optional, per-tool | Not implemented -- only real enhancement worth considering |
| OAuth Resource Server classification | HTTP/auth only | N/A |
| Client Resource Indicators (RFC 8707) | Client-side OAuth | N/A |
| Security best-practices page | Guidance | Nothing to emit on stdio |
| Elicitation (server prompts user mid-call) | Optional, needs client support | No value -- connection args are injected, no interactive prompts |
| Resource links in tool results | Optional content type | No value -- results are CLI text |
| MCP-Protocol-Version header on later requests | HTTP transport only | N/A (stdio) |
| Lifecycle SHOULD to MUST | Yes | Done -- version negotiation performs the MUST behavior |
| _meta on interface types | Optional | Not needed |
| context on CompletionRequest | Completions feature | N/A -- no completion |
| title display-name field | Optional, recommended | Not emitted -- trivial, worth adding |

## What is needed to honor the version statement

Strictly for compliance: nothing. The MUSTs are all satisfied today:

- Correct `initialize` result: protocolVersion + `capabilities.tools` +
  `serverInfo` (`BuildInitializeResponse`).
- Correct version negotiation: echo a supported version, else fall back
  (`NegotiateProtocolVersion`).
- `tools/list` descriptors (`BuildToolsListResponse` / `WriteMcpToolObject`).
- `tools/call` returning `content[]` + `isError` (`BuildToolResultResponse`).
- Batch rejection.
- Notifications ignored without a reply.

The features not implemented are all optional or belong to transports (HTTP)
and subsystems (auth, resources, prompts, completion) not exposed per the PRD.

## Optional polish (not obligation)

1. title on tools + serverInfo. Cheap and recommended by the revision. Tools
   currently emit only `name` + `description`; `serverInfo` emits `name` +
   `version`. Adding a human-friendly `title` (e.g. name `table_describe` ->
   title "Describe table") separates programmatic id from display name. Low
   risk, no behavior change.

2. Structured tool output. The only substantive gap and the highest-leverage
   feature for the agent use case. Many kudu tools already emit machine-readable
   JSON (`--format json`, `list_table_output_format=json`, ksck `json_compact`,
   `cluster gather` is JSON by construction). For those tools we could declare
   an `outputSchema` and return `structuredContent` alongside the text block, so
   the model consumes parsed data instead of re-parsing free text. Opt-in per
   tool and additive (still return serialized JSON in a text block for
   back-compat, which we already do). Real work: deciding which tools get
   schemas and maintaining them. Not required to honor the statement.

## Recommendation

- Keep the target at 2025-06-18 and phrase the claim precisely: "MCP
  2025-06-18, tools over stdio." Honored now.
- Add title to tool descriptors and serverInfo -- trivial, makes the
  "we track the revision's additions" claim literally true.
- Treat structured output as an optional follow-up scoped to the JSON-capable
  tools, only if consuming structured data is wanted.

## Cross-cutting note (from 2025-11-25 guidance)

Argument-validation failures in `HandleToolsCall` currently return JSON-RPC
`-32602` (invalid params). Later guidance suggests returning these as tool
errors (`isError=true`) instead, so the model sees them as recoverable tool
output rather than protocol errors. Optional, independent of version support.
