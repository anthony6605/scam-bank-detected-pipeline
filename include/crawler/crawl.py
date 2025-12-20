import argparse
import hashlib
import io
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Tuple, Set, List, Dict
from urllib.parse import urljoin, urlparse
from urllib import robotparser

import requests
import yaml
from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_date
import pdfplumber

USER_AGENT = "SCAM-BANK-DETECTED-PIPELINE/1.0 (educational project; contact: anhle23@augustana.edu)"


def norm_text(s: str) -> str:
    return " ".join((s or "").split())


def sha256_text(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def stable_doc_id(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()


def get_domain(url: str) -> str:
    return urlparse(url).netloc.lower()


@dataclass
class SourceConfig:
    name: str
    start_urls: List[str]
    allow_domains: List[str]
    article_url_contains: List[str]
    next_page_selector: str
    rate_limit_seconds: float
    max_list_pages: int


class RobotsCache:
    def __init__(self, session: requests.Session):
        self.session = session
        self.cache: Dict[str, robotparser.RobotFileParser] = {}

    def allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        scheme = parsed.scheme or "https"
        if not domain:
            return False

        if domain not in self.cache:
            rp = robotparser.RobotFileParser()
            robots_url = f"{scheme}://{domain}/robots.txt"
            try:
                resp = self.session.get(
                    robots_url,
                    timeout=15,
                    headers={"User-Agent": USER_AGENT},
                )
                rp.parse(resp.text.splitlines() if resp.status_code < 400 else [])
            except Exception:
                # If robots can't be fetched, don't block the whole run.
                rp.parse([])
            self.cache[domain] = rp

        return self.cache[domain].can_fetch(USER_AGENT, url)


def is_allowed_domain(url: str, allow_domains: List[str]) -> bool:
    host = get_domain(url)
    return any(host.endswith(d.lower()) for d in allow_domains)


def looks_like_target(url: str, contains_rules: List[str]) -> bool:
    u = url.lower()
    return any(rule.lower() in u for rule in contains_rules)


def extract_html_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ")
    return norm_text(text)


def extract_pdf_text(pdf_bytes: bytes) -> str:
    out = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            out.append(page.extract_text() or "")
    return norm_text(" ".join(out))


def best_effort_title(soup: BeautifulSoup) -> str:
    if soup.title and soup.title.get_text(strip=True):
        return norm_text(soup.title.get_text())
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return norm_text(h1.get_text())
    return ""


def best_effort_published_date(soup: BeautifulSoup) -> Optional[str]:
    candidates = [
        ("meta", {"property": "article:published_time"}),
        ("meta", {"name": "article:published_time"}),
        ("meta", {"name": "date"}),
        ("meta", {"name": "pubdate"}),
        ("meta", {"property": "og:updated_time"}),
    ]
    for tag, attrs in candidates:
        el = soup.find(tag, attrs)
        if el and el.get("content"):
            try:
                return parse_date(el["content"]).date().isoformat()
            except Exception:
                pass

    t = soup.find("time")
    if t and t.get("datetime"):
        try:
            return parse_date(t["datetime"]).date().isoformat()
        except Exception:
            pass

    return None


def discover_links_from_list_page(
    base_url: str, html: str, src: SourceConfig
) -> Tuple[Set[str], Optional[str]]:
    soup = BeautifulSoup(html, "lxml")

    links: Set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        full = urljoin(base_url, href)
        if not is_allowed_domain(full, src.allow_domains):
            continue
        if looks_like_target(full, src.article_url_contains):
            links.add(full)

    next_url: Optional[str] = None
    if src.next_page_selector:
        nxt = soup.select_one(src.next_page_selector)
        if nxt and nxt.get("href"):
            next_url = urljoin(base_url, nxt["href"])

    return links, next_url


def fetch(session: requests.Session, url: str, timeout: int = 30) -> requests.Response:
    return session.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})


def crawl_source(
    session: requests.Session,
    robots: RobotsCache,
    src: SourceConfig,
    out_file: str,
    max_docs: int,
    save_raw_dir: Optional[str],
) -> None:
    seen_urls: Set[str] = set()
    written = 0

    def write_record(rec: dict) -> None:
        nonlocal written
        with open(out_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        written += 1

    for start_url in src.start_urls:
        list_url: Optional[str] = start_url
        page_count = 0

        while list_url and page_count < src.max_list_pages and written < max_docs:
            page_count += 1

            if not robots.allowed(list_url):
                print(f"[{src.name}] robots disallow list: {list_url}")
                break

            time.sleep(src.rate_limit_seconds)
            r = fetch(session, list_url)

            if r.status_code >= 400:
                print(f"[{src.name}] list fetch failed {r.status_code}: {list_url}")
                break

            links, next_url = discover_links_from_list_page(list_url, r.text, src)
            print(f"[{src.name}] list page {page_count}: found {len(links)} candidate links")

            for url in sorted(links):
                if written >= max_docs:
                    return
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                if not robots.allowed(url):
                    continue

                time.sleep(src.rate_limit_seconds)
                resp = fetch(session, url)
                fetched_at = datetime.now(timezone.utc).isoformat()
                ctype = resp.headers.get("content-type", "").split(";")[0].strip().lower()

                title = ""
                published_date = None
                text = ""

                raw_base = None
                if save_raw_dir:
                    os.makedirs(save_raw_dir, exist_ok=True)
                    raw_base = os.path.join(save_raw_dir, stable_doc_id(url))

                try:
                    # HTML
                    if resp.status_code < 400 and ("text/html" in ctype or (ctype == "" and resp.text)):
                        if raw_base:
                            with open(raw_base + ".html", "w", encoding="utf-8") as f:
                                f.write(resp.text)

                        soup = BeautifulSoup(resp.text, "lxml")
                        title = best_effort_title(soup)
                        published_date = best_effort_published_date(soup)
                        text = extract_html_text(resp.text)

                        # Discover PDFs on the page as separate docs
                        for a in soup.select("a[href]"):
                            href = a.get("href")
                            if not href:
                                continue
                            pdf_url = urljoin(url, href)
                            if pdf_url.lower().endswith(".pdf") and is_allowed_domain(pdf_url, src.allow_domains):
                                if pdf_url not in seen_urls and robots.allowed(pdf_url):
                                    seen_urls.add(pdf_url)
                                    time.sleep(src.rate_limit_seconds)
                                    pdf_resp = fetch(session, pdf_url)
                                    if pdf_resp.status_code < 400:
                                        pdf_text = extract_pdf_text(pdf_resp.content)
                                        pdf_rec = {
                                            "doc_id": stable_doc_id(pdf_url),
                                            "source": src.name,
                                            "url": pdf_url,
                                            "fetched_at": datetime.now(timezone.utc).isoformat(),
                                            "status_code": pdf_resp.status_code,
                                            "content_type": "application/pdf",
                                            "title": "",
                                            "published_date": None,
                                            "text": pdf_text,
                                            "content_hash": sha256_text(pdf_text),
                                        }
                                        write_record(pdf_rec)

                    # PDF
                    elif resp.status_code < 400 and ("pdf" in ctype or url.lower().endswith(".pdf")):
                        pdf_bytes = resp.content
                        if raw_base:
                            with open(raw_base + ".pdf", "wb") as f:
                                f.write(pdf_bytes)
                        text = extract_pdf_text(pdf_bytes)

                    else:
                        text = ""

                except Exception as e:
                    print(f"[{src.name}] parse error: {url} -> {e}")
                    text = ""

                normalized = norm_text(text)
                record = {
                    "doc_id": stable_doc_id(url),
                    "source": src.name,
                    "url": url,
                    "fetched_at": fetched_at,
                    "status_code": resp.status_code,
                    "content_type": ctype,
                    "title": title,
                    "published_date": published_date,
                    "text": normalized,
                    "content_hash": sha256_text(normalized),
                }
                write_record(record)

            list_url = next_url


def load_sources(path: str) -> List[SourceConfig]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    sources = []
    for s in data["sources"]:
        sources.append(
            SourceConfig(
                name=s["name"],
                start_urls=s["start_urls"],
                allow_domains=s["allow_domains"],
                article_url_contains=s["article_url_contains"],
                next_page_selector=s.get("next_page_selector", "") or "",
                rate_limit_seconds=float(s.get("rate_limit_seconds", 2)),
                max_list_pages=int(s.get("max_list_pages", 3)),
            )
        )
    return sources


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", default="sources.yml")
    ap.add_argument("--out", default="docs.jsonl")
    ap.add_argument("--max_docs", type=int, default=200)
    ap.add_argument("--save_raw_dir", default="")  # set e.g. data/raw_pages to enable
    args = ap.parse_args()

    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    session = requests.Session()
    robots = RobotsCache(session)

    sources = load_sources(args.sources)
    for src in sources:
        print(f"=== Crawling source: {src.name} ===")
        crawl_source(
            session=session,
            robots=robots,
            src=src,
            out_file=args.out,
            max_docs=args.max_docs,
            save_raw_dir=(args.save_raw_dir if args.save_raw_dir else None),
        )

    print(f"Done. Output: {args.out}")


if __name__ == "__main__":
    main()
