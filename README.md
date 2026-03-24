# arXiv PDF Fetcher

This repository contains a single, standalone Python script that:

- Takes two arguments: a `name` and an arXiv search `url`
- Discovers all arXiv records listed by that search (following pagination)
- Fetches authoritative metadata via the arXiv Atom API
- Downloads each corresponding PDF into a local subfolder
- Names files predictably using the author strings and title
- Writes an append-only metadata ledger so runs are resumable

## Requirements

- Python 3
- No third-party dependencies (standard library only)

## Usage

Basic:

```bash
python3 fetch_arxiv_pdfs.py "Some Name" "https://arxiv.org/search/math?searchtype=author&query=..."
```

Common options:

```bash
python3 fetch_arxiv_pdfs.py "Some Name" "https://arxiv.org/search/..." \
  --delay 1.0 \
  --page-size 200 \
  --batch-size 50 \
  --timeout 30 \
  --retries 3
```

Optional limit (useful for testing):

```bash
python3 fetch_arxiv_pdfs.py "Some Name" "https://arxiv.org/search/..." --max-results 5
```

## Output Layout

The script creates an output folder in the current working directory:

- `{SANITIZED_NAME}_resources`

Folder sanitization rules:

- Uppercase
- Spaces become underscores
- Characters outside `A-Z`, `0-9`, and `_` are removed

Inside the output folder:

- Downloaded PDFs
- `metadata.jsonl` (append-only ledger)

## How It Works

### 1) Discover arXiv IDs from the search URL

The provided `url` should be an arXiv search results page (e.g. `/search/...`).

The script:

- Requests the HTML for the search results
- Extracts arXiv identifiers by looking for `/abs/<id>` links
- Follows pagination by setting the `start` and `size` query parameters
- Stops when a page yields no new identifiers

Only the identifiers are taken from HTML. Titles and authors are not parsed from HTML.

### 2) Fetch metadata using the arXiv Atom API

For each discovered arXiv id, the script queries the Atom API:

- `https://export.arxiv.org/api/query?id_list=<comma-separated-ids>`

It parses the Atom XML and records:

- `arxiv_id` (versionless)
- `title`
- `authors` (as provided by arXiv)

This avoids relying on the search page structure for metadata.

### 3) Build predictable filenames

Each PDF is named using a sanitized author prefix and a sanitized title:

- `AUTHORS_title_in_lowercase.pdf`

Author prefix:

- Uses the first 3 authors
- Appends `ETAL` if there are more than 3 authors
- Each author string is sanitized from the Atom API value:
  - ASCII-fold (accents removed)
  - Uppercase
  - Split into alphanumeric tokens
  - Drop tokens of length 2 (for compactness)
  - Join tokens with underscores

Title segment:

- ASCII-fold (accents removed)
- Lowercase
- Non-alphanumeric characters become separators
- Tokens are joined with underscores
- Length is capped to keep filenames manageable

Collision handling:

- If two different records would produce the same filename, the script appends a unique id suffix:
  - `...__<arxiv_id>.pdf`

### 4) Download PDFs safely

- PDF URL is always:
  - `https://arxiv.org/pdf/<arxiv_id>.pdf`
- Downloads are streamed to `*.part` and atomically renamed to `*.pdf`
- The response is sanity-checked to look like a PDF (`%PDF` header)
- Retries/backoff and timeouts are used for robustness

### 5) Maintain a resumable ledger (`metadata.jsonl`)

In the output folder, `metadata.jsonl` is written as JSON Lines (one JSON object per line).
It is append-only and includes (among other fields):

- `arxiv_id`, `abs_url`, `pdf_url`
- `title`, `authors`
- `filename`
- `downloaded` status and timestamps
- `bytes` and `sha256` for successful downloads

On rerun:

- If the ledger indicates a record is downloaded and the file exists, it is skipped
- If a record already has a `filename` in the ledger, the same filename is reused

## Notes

- Be considerate with `--delay` to avoid hammering arXiv.
- If a search URL is very broad, consider using `--max-results` while testing.
