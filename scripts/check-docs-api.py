#!/usr/bin/env python3
"""Fail when the documentation stops describing the code.

The usage docs are the anti-confabulation source for anyone -- human or
agent -- who has not read the crates. A method the docs invent, one they
never mention, a renamed GraphQL argument or a default that drifted are all
the same defect: prose that still reads true after it stopped being true.

Every check compares docs/index.html (and SKILL.md, and the specs where they
are the right home) against the sources, and reports rather than guesses:
what a check cannot resolve it says so about, instead of failing.

    scripts/check-docs-api.py          # report, exit 1 on any failure
"""
import re
import sys
from html.parser import HTMLParser
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SITE = ROOT / "docs/index.html"

# Bindings the docs use for a Db handle, and for an open transaction. A call
# on one of these is a claim about the public API; anything else is a local.
DB_RECEIVERS = ("db", "store")
TXN_RECEIVERS = ("txn", "tx")


class PreText(HTMLParser):
    """Text of every <pre> block, entities already decoded."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.depth, self.buf, self.blocks = 0, [], []

    def handle_starttag(self, tag, attrs):
        if tag == "pre":
            self.depth += 1

    def handle_endtag(self, tag):
        if tag == "pre" and self.depth:
            self.depth -= 1
            if not self.depth:
                self.blocks.append("".join(self.buf))
                self.buf = []

    def handle_data(self, data):
        if self.depth:
            self.buf.append(data)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def impl_methods(src: str, ty: str) -> set[str]:
    """`pub fn` names on one impl block -- not on its neighbours.

    db.rs carries both Db, which users hold, and DbInner, which is pub only
    so the sibling crates can reach it. Only the first is documentation's
    job, and a scan that misses the distinction reports the engine's plumbing
    as undocumented API.
    """
    out: set[str] = set()
    for m in re.finditer(rf"^impl(?:<[^>]*>)? {re.escape(ty)}\b", src, re.M):
        rest = src[m.end() :]
        nxt = re.search(r"^(?:impl|pub struct|struct|pub enum)\b", rest, re.M)
        body = rest[: nxt.start()] if nxt else rest
        out |= set(re.findall(r"^    pub (?:async )?fn (\w+)", body, re.M))
    return out


def graphql_roots(doc: str):
    """[(field, [argument names])] for each selection at the operation's top.

    Needs real depth tracking: an argument takes an object literal, and
    BytesInput is written {text: "..."}, so an inner object key is not an
    argument name and a nested selection is not a root field.
    """
    doc = re.sub(r"#[^\n]*", "", doc)
    out, i, brace, n = [], 0, 0, len(doc)
    while i < n:
        c = doc[i]
        if c == '"':
            i += 1
            while i < n and doc[i] != '"':
                i += 2 if doc[i] == "\\" else 1
            i += 1
            continue
        if c in "{}":
            brace += 1 if c == "{" else -1
            i += 1
            continue
        m = re.match(r"[A-Za-z_]\w*", doc[i:])
        if not m:
            i += 1
            continue
        word, j = m.group(0), i + m.end()
        k = j
        while k < n and doc[k] in " \t\n":
            k += 1
        args: list[str] = []
        if k < n and doc[k] == "(":
            depth, k2, inner = 1, k + 1, []
            while k2 < n and depth:
                ch = doc[k2]
                if ch == '"':
                    k2 += 1
                    while k2 < n and doc[k2] != '"':
                        k2 += 2 if doc[k2] == "\\" else 1
                elif ch in "({[":
                    depth += 1
                elif ch in ")}]":
                    depth -= 1
                elif depth == 1:
                    inner.append(ch)
                k2 += 1
            args = re.findall(r"([A-Za-z_]\w*)\s*:", "".join(inner))
            j = k2
        if brace == 1 and word not in ("query", "mutation", "subscription", "on"):
            out.append((word, args))
        i = j
    return out


def default_options() -> dict[str, float]:
    """Every numeric field of `Options::default()`, resolved to its value."""
    block = re.search(
        r"impl Default for Options \{.*?\n\}", read("crates/fluent31/src/config.rs"), re.S
    ).group(0)
    vals: dict[str, float] = {}
    for name, expr in re.findall(r"(\w+): ([^,\n]+),", block):
        e = expr.strip()
        shift = re.fullmatch(r"(\d+) << (\d+)", e)
        if shift:
            vals[name] = int(shift.group(1)) << int(shift.group(2))
        elif re.fullmatch(r"[\d_]+", e):
            vals[name] = int(e.replace("_", ""))
        elif re.fullmatch(r"\d+\.\d+", e):
            vals[name] = float(e)
    return vals


def spellings(n) -> set[str]:
    """Every form the docs might reasonably print a number in."""
    out = {str(n), f"{n:,}"}
    for unit, size in (("KiB", 1 << 10), ("MiB", 1 << 20), ("GiB", 1 << 30)):
        if isinstance(n, int) and n >= size and n % size == 0:
            q = n // size
            out |= {f"{q} {unit}", f"{q}&nbsp;{unit}", f"{q}{unit}"}
    if isinstance(n, int) and n >= 10**6 and n % 10**6 == 0:
        out |= {f"{n // 10**6} million"}
    return out


def named(name: str, *haystacks: str) -> bool:
    return any(re.search(rf"\b{re.escape(name)}\b", h) for h in haystacks)


def main() -> int:
    site_raw = SITE.read_text(encoding="utf-8")
    parser = PreText()
    parser.feed(site_raw)
    code = "\n".join(parser.blocks)
    skill = read("SKILL.md")
    specs = read("WASM.md") + read("DESIGN.md") + read("REPLICATION.md")

    db_src = read("crates/fluent31/src/db.rs")
    guest_src = read("crates/fluent-guest/src/lib.rs")

    fails: list[str] = []
    notes: list[str] = []

    # 1. Every call the docs make resolves to a real symbol. The two pools
    #    stay apart: a Txn-only method on a Db handle does not compile, so a
    #    merged pool would wave through the very confusion the docs prevent.
    db_api = impl_methods(db_src, "Db") | {"clone"}  # the handle is held as an Arc
    txn_api = impl_methods(read("crates/fluent31/src/txn.rs"), "Txn")
    guest_api = set(re.findall(r"^pub fn (\w+)", guest_src, re.M))
    for recv, call in set(re.findall(r"\b(\w+)\.(\w+)\(", code)):
        pool = db_api if recv in DB_RECEIVERS else txn_api if recv in TXN_RECEIVERS else None
        if pool is not None and call not in pool:
            ty = "Db" if recv in DB_RECEIVERS else "Txn"
            fails.append(f"docs call {recv}.{call}(), which impl {ty} does not define")
    for call in set(re.findall(r"fluent_guest::(\w+)\(", code)):
        if call not in guest_api:
            fails.append(f"docs call fluent_guest::{call}(), which the SDK does not export")

    # 2/3. Every public method is named somewhere a reader would find it.
    for name in sorted(impl_methods(db_src, "Db")):
        if not named(name, site_raw, skill, specs):
            fails.append(f"Db::{name} is public API named in no document")
    guest_pub = guest_api | set(re.findall(r"^    pub fn (\w+)", guest_src, re.M))
    guest_pub.discard("__entry")  # macro plumbing, never written by hand
    for name in sorted(guest_pub):
        if not named(name, site_raw, skill, specs):
            fails.append(f"fluent_guest::{name} is public API named in no document")

    # 4. GraphQL root fields and their arguments exist in the schema.
    gql_src = read("crates/fluent-graphql/src/builtins.rs") + read(
        "crates/fluent-graphql/src/subscriptions.rs"
    )
    fields: dict[str, set[str]] = {}
    for m in re.finditer(
        r'(?:Subscription)?Field::new\(\s*"(\w+)"(.*?)(?=(?:Subscription)?Field::new\(|\Z)',
        gql_src,
        re.S,
    ):
        fields.setdefault(m.group(1), set())
        fields[m.group(1)] |= set(re.findall(r'InputValue::new\(\s*"(\w+)"', m.group(2)))
    gql_blocks = [b for b in parser.blocks if re.match(r"\s*(query|mutation|subscription)\b", b)]
    gql_text = "\n".join(gql_blocks)
    for block in gql_blocks:
        for field, args in graphql_roots(block):
            if field not in fields:
                # A module descriptor mints its own root field. The docs must
                # still introduce it somewhere outside the query that uses it.
                elsewhere = site_raw.count(field) - gql_text.count(field)
                if elsewhere > 0:
                    notes.append(f"{field}: typed-module field, introduced elsewhere in the docs")
                else:
                    fails.append(f"GraphQL {field} is neither a built-in field nor introduced anywhere")
                continue
            for arg in args:
                if arg not in fields[field]:
                    fails.append(
                        f"GraphQL {field}({arg}:) is not an argument; it takes {sorted(fields[field])}"
                    )

    # 5. Documented defaults still match config.rs, beside their own name.
    hay = site_raw + skill
    for name, value in sorted(default_options().items()):
        windows = [
            hay[max(0, m.start() - 200) : m.end() + 200] for m in re.finditer(rf"\b{name}\b", hay)
        ]
        if not windows:
            fails.append(f"Options::{name} is documented nowhere")
        elif not any(s in w for w in windows for s in spellings(value)):
            fails.append(f"Options::{name} defaults to {value}, which appears near no mention of it")

    # 6. Command lines the docs print are the ones the binaries accept.
    for crate, path in (
        ("fluent-cli", "crates/fluent-cli/src/main.rs"),
        ("fluent-server", "crates/fluent-server/src/main.rs"),
    ):
        src = read(path)
        usage = re.search(rf"{re.escape(crate)} <[^\n]*", src)
        if not usage:
            fails.append(f"{crate} has no usage line to compare the docs against")
            continue
        line = usage.group(0)
        # Against the decoded block text, not the markup: the site highlights
        # each flag, so the raw HTML interleaves spans with the characters.
        if line not in code:
            fails.append(f"{crate} usage line in the docs differs from its own USAGE string")
        accepts = set(re.findall(r'"(--[a-z-]+)"', src))
        for flag in sorted(set(re.findall(r"--[a-z][a-z0-9-]*", line))):
            if flag not in accepts:
                fails.append(f"{crate} usage advertises {flag}, which its parser rejects")
        for flag in sorted(accepts - {"--help"}):
            if flag not in site_raw:
                fails.append(f"{crate} accepts {flag}, which the docs never mention")
    examples = {p.stem for p in (ROOT / "crates/fluent31/examples").glob("*.rs")}
    for name in set(re.findall(r"--example (\w+)", site_raw + read("README.md"))):
        if name not in examples:
            fails.append(f"docs run --example {name}, which does not exist")

    notes = sorted(set(notes))
    for note in notes:
        print(f"note  {note}")
    for f in fails:
        print(f"FAIL  {f}")
    print(f"\n{len(fails)} failures, {len(notes)} notes")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
