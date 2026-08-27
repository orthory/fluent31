# fluent31 guide

The usage documentation now lives in `docs/`, which is one document served
two ways. This file is a pointer, so links to it keep working.

## Where it went

| You are | Read |
|---|---|
| A person | [orthory.github.io/fluent31](https://orthory.github.io/fluent31/) — the same content, navigable, searchable |
| An agent or a script | [`docs/llms.txt`](docs/llms.txt) — the index, then the page you need under [`docs/p/`](docs/p/) |
| About to write fluent31 code | [SKILL.md](SKILL.md) first — the dense primer, and the assumptions from other databases that are wrong here |

`docs/index.html` is the source. `docs/llms.txt` and `docs/p/*.md` are
generated from it by `scripts/build-agent-docs.py`; a page is one file, so a
fetcher can address it.

## The rest of the shelf

| | |
|---|---|
| [WASM.md](WASM.md) | Module authoring manual and the normative host ABI. |
| [DESIGN.md](DESIGN.md) | The architecture as implemented, section by section. |
| [REPLICATION.md](REPLICATION.md) | The replica protocol. |

These stay where they are. They specify mechanism below the level the usage
docs describe, and the usage docs defer to them — as does the code, which
wins over both.

## Why this file is a pointer

There were two complete usage documents: this one and the site. They said
the same things in different words, and the site had pulled ahead — an
exhaustive WASM section, a relational translation guide, and roughly twice
the density of stated constraints. Keeping both meant writing every fact
twice and letting them drift apart in between. The site is now the single
source, and the markdown an agent reads is generated from it rather than
maintained beside it.
