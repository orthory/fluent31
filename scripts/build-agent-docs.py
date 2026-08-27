#!/usr/bin/env python3
"""Render the docs site into agent-readable markdown.

`docs/index.html` is one page with hash routing, so every one of its pages
answers to the same URL and a fetcher cannot address any of them. This turns
each article into a real file under `docs/p/` and writes the `docs/llms.txt`
index that points at them.

    scripts/build-agent-docs.py            # write the files
    scripts/build-agent-docs.py --check    # fail if what is on disk is stale

The site is the source. Never hand-edit anything this writes.
"""
from __future__ import annotations

import argparse
import re
import sys
from html.parser import HTMLParser
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SITE = ROOT / "docs" / "index.html"
PAGES = ROOT / "docs" / "p"
INDEX = ROOT / "docs" / "llms.txt"
BASE = "https://orthory.github.io/fluent31"

VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input",
        "link", "meta", "param", "source", "track", "wbr"}


# --------------------------------------------------------------- tiny DOM
class Node:
    __slots__ = ("tag", "attrs", "kids", "text")

    def __init__(self, tag, attrs=None, text=None):
        self.tag, self.attrs, self.kids, self.text = tag, attrs or {}, [], text

    def cls(self):
        return self.attrs.get("class", "")

    def find_all(self, tag, cls=None):
        out = []
        for k in self.kids:
            if k.tag == tag and (cls is None or cls in k.cls().split()):
                out.append(k)
            out.extend(k.find_all(tag, cls))
        return out


class Tree(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.root = Node("#root")
        self.stack = [self.root]

    def handle_starttag(self, tag, attrs):
        node = Node(tag, dict(attrs))
        self.stack[-1].kids.append(node)
        if tag not in VOID:
            self.stack.append(node)

    def handle_startendtag(self, tag, attrs):
        self.stack[-1].kids.append(Node(tag, dict(attrs)))

    def handle_endtag(self, tag):
        for i in range(len(self.stack) - 1, 0, -1):
            if self.stack[i].tag == tag:
                del self.stack[i:]
                return

    def handle_data(self, data):
        self.stack[-1].kids.append(Node("#text", text=data))


def parse(source: str) -> Node:
    t = Tree()
    t.feed(source)
    return t.root


# ------------------------------------------------------------ conversion
def _site_slug(text: str) -> str:
    """The heading id docs/index.html mints for this text, character for
    character -- the join key between a site link and a generated file."""
    return re.sub(r"^-+|-+$", "", re.sub(r"[^a-z0-9]+", "-", text.lower()))


def plain(node: Node) -> str:
    """Text content, markup discarded."""
    if node.tag == "#text":
        return node.text
    return "".join(plain(k) for k in node.kids)


def md_anchor(text: str) -> str:
    """The fragment a markdown renderer derives from a heading."""
    return re.sub(r"[^\w\- ]", "", text.lower()).strip().replace(" ", "-")


def href(target: str, anchors: dict[str, str]) -> str:
    """Site hash link -> a path among the generated files.

    The site and a markdown renderer slugify a heading differently -- an
    apostrophe becomes a hyphen in one and vanishes in the other -- so the
    fragment is looked up by the site's heading id, never rewritten by hand.
    """
    if not target.startswith("#"):
        return target
    page, _, heading = target[1:].partition("/")
    if not heading:
        return f"{page}.md"
    return f"{page}.md#{anchors.get(heading, heading)}"


def emphasis(tag: str, inner: str) -> str:
    """The markdown wrapper for a phrasing tag; other tags pass their text."""
    if tag == "code":
        # A backtick inside the span needs a longer fence.
        fence = "`" * (max((len(m) for m in re.findall(r"`+", inner)), default=0) + 1)
        pad = " " if inner.startswith("`") or inner.endswith("`") else ""
        return f"{fence}{pad}{inner}{pad}{fence}"
    if tag in ("strong", "b"):
        return f"**{inner}**"
    if tag in ("em", "i"):
        return f"*{inner}*"
    return inner


def inline(node: Node, anchors: dict[str, str]) -> str:
    """Phrasing content -> markdown, whitespace preserved as authored."""
    if node.tag == "#text":
        return node.text
    inner = "".join(inline(k, anchors) for k in node.kids)
    if node.tag == "a":
        return f"[{inner}]({href(node.attrs.get('href', ''), anchors)})"
    if node.tag == "br":
        return "\n"
    return emphasis(node.tag, inner)


def squash(s: str) -> str:
    return re.sub(r"[ \t\n]+", " ", s).strip()


def desc(node: Node) -> str:
    """A lead line for the index: markup kept, links flattened to their text.

    The index sits a directory above the pages, so a link copied out of a
    lead would point at the wrong place. Only the text survives.
    """
    if node.tag == "#text":
        return node.text
    inner = "".join(desc(k) for k in node.kids)
    return emphasis(node.tag, inner)      # an <a> falls through to its text


def code_block(node: Node) -> str:
    """A .codeblock div: optional filename bar, then the <pre>."""
    bar = next((plain(k) for k in node.kids if "bar" in k.cls()), None)
    pre = next((k for k in node.kids if k.tag == "pre"), None)
    body = plain(pre).strip("\n") if pre is not None else ""
    head = f"<!-- {squash(bar)} -->\n" if bar else ""
    fence = "`" * max(3, max((len(m) for m in re.findall(r"`+", body)), default=0) + 1)
    return f"{head}{fence}\n{body}\n{fence}"


def table(node: Node, anchors: dict[str, str]) -> str:
    def cells(row, tag):
        return [squash(inline(c, anchors)).replace("|", "\\|")
                for c in row.kids if c.tag == tag]

    rows = node.find_all("tr")
    if not rows:
        return ""
    head = cells(rows[0], "th")
    body_rows = rows[1:] if head else rows
    width = max([len(head)] + [len(cells(r, "td")) for r in body_rows])
    if not head:
        head = [""] * width
    out = ["| " + " | ".join(head + [""] * (width - len(head))) + " |",
           "|" + "---|" * width]
    for r in body_rows:
        c = cells(r, "td")
        out.append("| " + " | ".join(c + [""] * (width - len(c))) + " |")
    return "\n".join(out)


def listing(node: Node, anchors: dict[str, str], depth: int = 0) -> str:
    out = []
    for i, li in enumerate([k for k in node.kids if k.tag == "li"], 1):
        marker = f"{i}." if node.tag == "ol" else "-"
        own = "".join(inline(k, anchors)
                      for k in li.kids if k.tag not in ("ul", "ol"))
        pad = "  " * depth
        out.append(f"{pad}{marker} {squash(own)}")
        for sub in [k for k in li.kids if k.tag in ("ul", "ol")]:
            out.append(listing(sub, anchors, depth + 1))
    return "\n".join(out)


def block(node: Node, anchors: dict[str, str]) -> str:
    """One block-level element -> its markdown, or '' to skip it."""
    cls = node.cls().split()
    if node.tag in ("h1", "h2", "h3", "h4"):
        return "#" * int(node.tag[1]) + " " + squash(inline(node, anchors))
    if node.tag == "p":
        if "eyebrow" in cls:
            return ""                      # navigational chrome, not content
        if "lead" in cls:
            return "> " + squash(inline(node, anchors))
        return squash(inline(node, anchors))
    if node.tag in ("ul", "ol"):
        return listing(node, anchors)
    if node.tag == "div":
        if "codeblock" in cls:
            return code_block(node)
        if "tablewrap" in cls:
            t = next((k for k in node.kids if k.tag == "table"), None)
            return table(t, anchors) if t is not None else ""
        if "note" in cls:
            p = next((k for k in node.kids if k.tag == "p"), None)
            if p is None:
                return ""
            lbl = next((k for k in p.kids if "lbl" in k.cls()), None)
            rest = "".join(inline(k, anchors) for k in p.kids if k is not lbl)
            tag = f"**{squash(plain(lbl))}** " if lbl is not None else ""
            return "> " + tag + squash(rest)
    if node.tag == "table":
        return table(node, anchors)
    return ""


def render(article: Node, pid: str, title: str, group: str,
           anchors: dict[str, str]) -> str:
    parts = [b for b in (block(k, anchors) for k in article.kids) if b]
    return "\n\n".join([
        f"<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->",
        f"<!-- Section: {group} · Human version: {BASE}/#{pid} -->",
        *parts,
        "---",
        f"fluent31 docs · [index](../llms.txt) · "
        f"[all pages]({BASE}/llms.txt) · this page is `{title}` in *{group}*",
    ]) + "\n"


# ------------------------------------------------------------------ build
def build() -> dict[str, str]:
    tree = parse(SITE.read_text(encoding="utf-8"))

    nav = next(n for n in tree.find_all("nav") if "nav" in n.cls())
    groups, order, titles = [], [], {}
    for kid in nav.kids:
        if kid.tag == "h5":
            groups.append((squash(plain(kid)), []))
        elif kid.tag == "a" and "data-nav" in kid.attrs:
            pid = kid.attrs["data-nav"]
            titles[pid] = squash(plain(kid))
            order.append(pid)
            groups[-1][1].append(pid)

    articles = {a.attrs["data-page"]: a
                for a in tree.find_all("article") if "data-page" in a.attrs}
    missing = set(order) ^ set(articles)
    if missing:
        sys.exit(f"nav and articles disagree: {sorted(missing)}")

    # Site heading id -> the fragment the generated markdown will carry.
    anchors = {f"{pid}--{_site_slug(plain(h))}": md_anchor(plain(h))
               for pid, art in articles.items()
               for h in art.find_all("h2") + art.find_all("h3")}

    group_of = {pid: g for g, ids in groups for pid in ids}
    out = {f"p/{pid}.md": render(articles[pid], pid, titles[pid],
                                 group_of[pid], anchors)
           for pid in order}

    lead = squash(desc(next(
        p for p in articles["introduction"].find_all("p") if "lead" in p.cls())))
    lines = [
        "# fluent31",
        "",
        f"> {lead}",
        "",
        "Every page of the documentation, one file each, in reading order.",
        "The HTML site at " + BASE + " is the same content for humans; its",
        "per-page URLs are fragments, so fetch the files below instead.",
        "",
        "Read `SKILL.md` first if you are about to write fluent31 code — it is",
        "the dense primer, and it names the assumptions carried over from other",
        "databases that are wrong here.",
        "",
        "## Primer",
        "",
        "- [SKILL.md](https://raw.githubusercontent.com/orthory/fluent31/master/SKILL.md): "
        "the model in twelve lines, exact signatures, every trap, and the priors that do not transfer.",
        "",
        "## Specs",
        "",
        "- [WASM.md](https://raw.githubusercontent.com/orthory/fluent31/master/WASM.md): "
        "module authoring manual and the normative host ABI.",
        "- [DESIGN.md](https://raw.githubusercontent.com/orthory/fluent31/master/DESIGN.md): "
        "the architecture as implemented, section by section.",
        "- [REPLICATION.md](https://raw.githubusercontent.com/orthory/fluent31/master/REPLICATION.md): "
        "the replica protocol.",
    ]
    for group, ids in groups:
        lines += ["", f"## {group}", ""]
        for pid in ids:
            lead_p = next((p for p in articles[pid].find_all("p")
                           if "lead" in p.cls()), None)
            summary = squash(desc(lead_p)) if lead_p is not None else ""
            lines.append(f"- [{titles[pid]}]({BASE}/p/{pid}.md)"
                         + (f": {summary}" if summary else ""))
    out["llms.txt"] = "\n".join(lines) + "\n"
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true",
                    help="exit non-zero if the generated files are stale")
    args = ap.parse_args()

    want = build()
    docs = ROOT / "docs"

    if args.check:
        have = {p.relative_to(docs).as_posix(): p.read_text(encoding="utf-8")
                for p in [INDEX, *sorted(PAGES.glob("*.md"))] if p.exists()}
        if have == want:
            print(f"agent docs up to date ({len(want)} files)")
            return 0
        for name in sorted(set(want) | set(have)):
            if name not in have:
                print(f"missing:  docs/{name}")
            elif name not in want:
                print(f"orphaned: docs/{name}")
            elif have[name] != want[name]:
                print(f"stale:    docs/{name}")
        print("\nrun: scripts/build-agent-docs.py")
        return 1

    PAGES.mkdir(parents=True, exist_ok=True)
    for stale in set(PAGES.glob("*.md")) - {docs / n for n in want}:
        stale.unlink()
    for name, text in want.items():
        (docs / name).write_text(text, encoding="utf-8")
    print(f"wrote {len(want)} files under docs/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
