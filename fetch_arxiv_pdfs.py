#!/usr/bin/env python3
"""Fetch all arXiv PDFs listed by a search URL.

Usage:
  python fetch_arxiv_pdfs.py "della Corte" "https://arxiv.org/search/math?searchtype=author&query=Della+Corte,+A"

Behavior:
  - Creates an output folder named {SANITIZED_NAME}_resources (uppercase, spaces -> underscores).
  - Discovers arXiv ids from the provided arXiv search results URL (handles pagination).
  - Fetches authoritative metadata via the arXiv Atom API.
  - Downloads each PDF and saves it with a recognizable filename:
      AUTHORS_title_in_lowercase.pdf
    where AUTHORS is derived from arXiv's author strings (sanitized), using the first 3 authors,
    and appending ETAL if there are more than 3.
  - Maintains an append-only metadata ledger at metadata.jsonl in the output folder.

This script uses only the Python standard library.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple


ATOM_NS = {
    "atom": "http://www.w3.org/2005/Atom",
}


_ARXIV_ABS_RE = re.compile(
    r"(?:https?://arxiv\.org)?/abs/"  # optional scheme+host
    r"(?P<id>(?:\d{4}\.\d{4,5})|(?:[a-z-]+/\d{7}))"  # new or legacy
    r"(?P<ver>v\d+)?",
    re.IGNORECASE,
)


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ascii_fold(s: str) -> str:
    return (
        unicodedata.normalize("NFKD", s)
        .encode("ascii", "ignore")
        .decode("ascii", "ignore")
    )


def sanitize_folder_name(name: str) -> str:
    s = ascii_fold(name).upper().strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Z0-9_]", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "RESOURCES"


def sanitize_title(title: str, *, max_len: int = 160) -> str:
    s = ascii_fold(title).lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    tokens = [t for t in s.split() if t]
    s = "_".join(tokens)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "untitled"
    if len(s) > max_len:
        s = s[:max_len].rstrip("_")
    return s


def sanitize_author(author: str) -> str:
    """Sanitize arXiv author string.

    - ASCII fold
    - Uppercase
    - Split into alnum tokens
    - Drop tokens with length == 2
    - Join with underscores
    """

    s = ascii_fold(author).upper()
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    tokens = [t for t in s.split() if t and len(t) != 2]
    s = "_".join(tokens)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "UNKNOWN"


def build_authors_prefix(authors: List[str]) -> str:
    sanitized = [sanitize_author(a) for a in authors]
    prefix_parts = sanitized[:3]
    if len(sanitized) > 3:
        prefix_parts.append("ETAL")
    return "_".join(prefix_parts) if prefix_parts else "UNKNOWN"


def normalize_arxiv_id(arxiv_id: str) -> str:
    arxiv_id = arxiv_id.strip()
    arxiv_id = re.sub(r"^https?://arxiv\.org/abs/", "", arxiv_id)
    arxiv_id = re.sub(r"v\d+$", "", arxiv_id)
    return arxiv_id


def arxiv_id_for_filename(arxiv_id: str) -> str:
    return normalize_arxiv_id(arxiv_id).replace("/", "_")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def request_bytes(url: str, *, timeout: int, user_agent: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def fetch_with_retries(
    url: str,
    *,
    timeout: int,
    user_agent: str,
    retries: int,
    base_backoff: float,
) -> bytes:
    last_err: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            return request_bytes(url, timeout=timeout, user_agent=user_agent)
        except urllib.error.HTTPError as e:
            last_err = e
            status = getattr(e, "code", None)
            if status in (404, 400, 401, 403):
                raise
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e

        if attempt < retries:
            sleep_s = base_backoff * (2 ** (attempt - 1))
            time.sleep(sleep_s)

    assert last_err is not None
    raise last_err


def extract_arxiv_ids_from_html(html: str) -> List[str]:
    ids: List[str] = []
    seen: set[str] = set()
    for m in _ARXIV_ABS_RE.finditer(html):
        arxiv_id = normalize_arxiv_id(m.group("id"))
        if arxiv_id and arxiv_id not in seen:
            ids.append(arxiv_id)
            seen.add(arxiv_id)
    return ids


def set_query_param(url: str, key: str, value: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    query[key] = [value]
    new_query = urllib.parse.urlencode(query, doseq=True)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, new_query, parsed.fragment))


def discover_arxiv_ids(
    url: str,
    *,
    timeout: int,
    user_agent: str,
    retries: int,
    page_size: int,
    delay: float,
    max_results: Optional[int],
) -> List[str]:
    all_ids: List[str] = []
    seen: set[str] = set()

    start = 0
    while True:
        page_url = set_query_param(set_query_param(url, "size", str(page_size)), "start", str(start))
        raw = fetch_with_retries(
            page_url,
            timeout=timeout,
            user_agent=user_agent,
            retries=retries,
            base_backoff=1.0,
        )
        html = raw.decode("utf-8", errors="replace")
        page_ids = extract_arxiv_ids_from_html(html)

        new_ids = [i for i in page_ids if i not in seen]
        if not new_ids:
            break

        for i in new_ids:
            all_ids.append(i)
            seen.add(i)
            if max_results is not None and len(all_ids) >= max_results:
                return all_ids[:max_results]

        start += page_size
        if delay:
            time.sleep(delay)

    return all_ids


@dataclass(frozen=True)
class ArxivItem:
    arxiv_id: str
    title: str
    authors: List[str]

    @property
    def pdf_url(self) -> str:
        return f"https://arxiv.org/pdf/{self.arxiv_id}.pdf"

    @property
    def abs_url(self) -> str:
        return f"https://arxiv.org/abs/{self.arxiv_id}"


def parse_atom_feed(xml_bytes: bytes) -> List[ArxivItem]:
    root = ET.fromstring(xml_bytes)
    items: List[ArxivItem] = []
    for entry in root.findall("atom:entry", ATOM_NS):
        entry_id = entry.findtext("atom:id", default="", namespaces=ATOM_NS).strip()
        arxiv_id = normalize_arxiv_id(entry_id)
        title = entry.findtext("atom:title", default="", namespaces=ATOM_NS)
        title = " ".join((title or "").split())
        authors = []
        for a in entry.findall("atom:author", ATOM_NS):
            nm = a.findtext("atom:name", default="", namespaces=ATOM_NS).strip()
            if nm:
                authors.append(nm)
        if arxiv_id:
            items.append(ArxivItem(arxiv_id=arxiv_id, title=title, authors=authors))
    return items


def fetch_metadata_for_ids(
    ids: List[str],
    *,
    timeout: int,
    user_agent: str,
    retries: int,
    batch_size: int,
    delay: float,
) -> Dict[str, ArxivItem]:
    out: Dict[str, ArxivItem] = {}
    for idx in range(0, len(ids), batch_size):
        batch = ids[idx : idx + batch_size]
        id_list = ",".join(batch)
        api_url = f"https://export.arxiv.org/api/query?id_list={urllib.parse.quote(id_list)}"
        xml_bytes = fetch_with_retries(
            api_url,
            timeout=timeout,
            user_agent=user_agent,
            retries=retries,
            base_backoff=1.0,
        )
        for item in parse_atom_feed(xml_bytes):
            out[item.arxiv_id] = item
        if delay:
            time.sleep(delay)
    return out


def load_jsonl_latest(path: str) -> Dict[str, dict]:
    """Load an append-only jsonl ledger keeping the last record per arxiv_id."""
    data: Dict[str, dict] = {}
    if not os.path.exists(path):
        return data
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            arxiv_id = normalize_arxiv_id(str(obj.get("arxiv_id", "")).strip())
            if arxiv_id:
                data[arxiv_id] = obj
    return data


def append_jsonl(path: str, obj: dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=True, sort_keys=True) + "\n")


def choose_filename(
    item: ArxivItem,
    *,
    out_dir: str,
    claimed: Dict[str, str],
    max_filename_len: int = 240,
) -> str:
    authors_prefix = build_authors_prefix(item.authors)
    title_part = sanitize_title(item.title)

    base = f"{authors_prefix}_{title_part}.pdf"
    safe_id = arxiv_id_for_filename(item.arxiv_id)

    # Enforce a soft max filename length by truncating title_part.
    if len(base) > max_filename_len:
        # Keep authors_prefix and .pdf; shrink title.
        overhead = len(authors_prefix) + 1 + len(".pdf")
        max_title = max(20, max_filename_len - overhead)
        title_part = sanitize_title(item.title, max_len=max_title)
        base = f"{authors_prefix}_{title_part}.pdf"

    path = os.path.join(out_dir, base)
    existing_claim = claimed.get(base)
    if existing_claim is not None and existing_claim != item.arxiv_id:
        return f"{authors_prefix}_{title_part}__{safe_id}.pdf"

    if os.path.exists(path):
        # If it already exists, disambiguate unless it is the same id we've recorded.
        # We can't know from the file alone, so disambiguate deterministically.
        return f"{authors_prefix}_{title_part}__{safe_id}.pdf"

    claimed[base] = item.arxiv_id
    return base


def download_pdf(
    url: str,
    dest_path: str,
    *,
    timeout: int,
    user_agent: str,
    retries: int,
    base_backoff: float,
) -> Tuple[int, str]:
    """Download a PDF to dest_path. Returns (bytes_written, sha256_hex)."""
    part_path = dest_path + ".part"
    last_err: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": user_agent})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                ctype = (resp.headers.get("Content-Type") or "").lower()
                # arXiv sometimes serves application/pdf; allow unknown.
                if ctype and "pdf" not in ctype and "octet-stream" not in ctype:
                    raise RuntimeError(f"Unexpected content-type: {ctype}")

                h = hashlib.sha256()
                total = 0
                with open(part_path, "wb") as out:
                    first = resp.read(5)
                    if not first.startswith(b"%PDF"):
                        raise RuntimeError("Response does not look like a PDF")
                    out.write(first)
                    h.update(first)
                    total += len(first)

                    while True:
                        chunk = resp.read(1024 * 64)
                        if not chunk:
                            break
                        out.write(chunk)
                        h.update(chunk)
                        total += len(chunk)

            os.replace(part_path, dest_path)
            return total, h.hexdigest()
        except urllib.error.HTTPError as e:
            last_err = e
            status = getattr(e, "code", None)
            if status in (404, 400, 401, 403):
                break
        except (urllib.error.URLError, TimeoutError, RuntimeError) as e:
            last_err = e

        try:
            if os.path.exists(part_path):
                os.remove(part_path)
        except OSError:
            pass

        if attempt < retries:
            time.sleep(base_backoff * (2 ** (attempt - 1)))

    assert last_err is not None
    raise last_err


def main() -> int:
    p = argparse.ArgumentParser(description="Fetch arXiv PDFs from a search URL")
    p.add_argument("name", help="Name used to create output folder")
    p.add_argument("url", help="arXiv search URL")
    p.add_argument("--delay", type=float, default=1.0, help="Delay between requests (seconds)")
    p.add_argument("--page-size", type=int, default=200, help="Search results page size")
    p.add_argument("--batch-size", type=int, default=50, help="arXiv API id_list batch size")
    p.add_argument("--max-results", type=int, default=None, help="Stop after N papers")
    p.add_argument("--timeout", type=int, default=30, help="HTTP timeout (seconds)")
    p.add_argument("--retries", type=int, default=3, help="Retries per request")
    args = p.parse_args()

    user_agent = "stalker-arxiv-fetch/1.0 (+https://arxiv.org)"

    out_dir = f"{sanitize_folder_name(args.name)}_resources"
    ensure_dir(out_dir)
    ledger_path = os.path.join(out_dir, "metadata.jsonl")

    existing = load_jsonl_latest(ledger_path)

    ids = discover_arxiv_ids(
        args.url,
        timeout=args.timeout,
        user_agent=user_agent,
        retries=args.retries,
        page_size=args.page_size,
        delay=args.delay,
        max_results=args.max_results,
    )
    if not ids:
        print("No arXiv ids found at the provided URL")
        return 2

    meta = fetch_metadata_for_ids(
        ids,
        timeout=args.timeout,
        user_agent=user_agent,
        retries=args.retries,
        batch_size=args.batch_size,
        delay=args.delay,
    )

    claimed: Dict[str, str] = {}
    # Pre-fill claimed from existing ledger to keep filenames stable.
    for arxiv_id, rec in existing.items():
        fn = rec.get("filename")
        if isinstance(fn, str) and fn:
            claimed[fn] = arxiv_id

    downloaded = 0
    skipped = 0
    failed = 0

    for arxiv_id in ids:
        item = meta.get(arxiv_id)
        if item is None:
            # If API didn't return it (rare), still record that we saw it.
            append_jsonl(
                ledger_path,
                {
                    "arxiv_id": arxiv_id,
                    "source_search_url": args.url,
                    "seen_at": now_iso(),
                    "status": "missing_metadata",
                },
            )
            failed += 1
            continue

        prev = existing.get(arxiv_id, {})
        prev_filename = prev.get("filename") if isinstance(prev, dict) else None
        if isinstance(prev_filename, str) and prev_filename:
            filename = prev_filename
        else:
            filename = choose_filename(item, out_dir=out_dir, claimed=claimed)
        dest_path = os.path.join(out_dir, filename)
        if prev.get("downloaded") is True and os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
            skipped += 1
            continue

        record_base = {
            "arxiv_id": item.arxiv_id,
            "abs_url": item.abs_url,
            "pdf_url": item.pdf_url,
            "title": item.title,
            "authors": item.authors,
            "filename": filename,
            "source_search_url": args.url,
        }
        append_jsonl(
            ledger_path,
            {
                **record_base,
                "started_at": now_iso(),
                "downloaded": False,
                "status": "downloading",
            },
        )

        try:
            nbytes, sha256_hex = download_pdf(
                item.pdf_url,
                dest_path,
                timeout=args.timeout,
                user_agent=user_agent,
                retries=args.retries,
                base_backoff=1.0,
            )
            append_jsonl(
                ledger_path,
                {
                    **record_base,
                    "downloaded": True,
                    "downloaded_at": now_iso(),
                    "bytes": nbytes,
                    "sha256": sha256_hex,
                    "status": "ok",
                },
            )
            downloaded += 1
        except Exception as e:  # noqa: BLE001
            append_jsonl(
                ledger_path,
                {
                    **record_base,
                    "downloaded": False,
                    "failed_at": now_iso(),
                    "status": "error",
                    "error": f"{type(e).__name__}: {e}",
                },
            )
            failed += 1

        if args.delay:
            time.sleep(args.delay)

    print(f"Output folder: {out_dir}")
    print(f"Discovered: {len(ids)}")
    print(f"Downloaded: {downloaded}")
    print(f"Skipped: {skipped}")
    print(f"Failed: {failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
