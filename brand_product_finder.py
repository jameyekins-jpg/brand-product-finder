
import re
import io
import sys
import csv
import time
import json
import queue
import typing as t
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

import streamlit as st

st.set_page_config(page_title="Brand/Product Page Finder", layout="wide")

# ----------------------------
# Helpers
# ----------------------------

USER_AGENT = "Mozilla/5.0 (compatible; BrandProductFinder/1.0; +https://example.com)"
DEFAULT_TIMEOUT = 15

def fetch(url: str) -> t.Optional[str]:
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=DEFAULT_TIMEOUT)
        if resp.status_code == 200 and resp.headers.get("content-type", "").lower().startswith(("text/html", "application/xml", "text/xml")):
            return resp.text
        elif resp.status_code == 200:
            return resp.text
        else:
            return None
    except requests.RequestException:
        return None

def find_sitemaps(base_url: str) -> t.List[str]:
    base_url = base_url.rstrip("/")
    candidates = [
        base_url + "/sitemap.xml",
        base_url + "/sitemap_index.xml",
        base_url + "/sitemap1.xml",
        base_url + "/sitemap-index.xml",
        base_url + "/sitemap_index.xml.gz",
        base_url + "/sitemap.xml.gz",
        base_url + "/robots.txt",
    ]
    found = []
    for c in candidates:
        txt = fetch(c)
        if not txt:
            continue
        if c.endswith("robots.txt"):
            for line in txt.splitlines():
                if "sitemap" in line.lower():
                    parts = re.findall(r'(https?://[^\s]+)', line, flags=re.I)
                    for p in parts:
                        found.append(p.strip())
        else:
            found.append(c)
    return list(dict.fromkeys(found))

def parse_sitemap(xml_text: str) -> t.List[str]:
    # Handles simple sitemaps and sitemap indexes
    urls: t.List[str] = []
    try:
        soup = BeautifulSoup(xml_text, "xml")
        loc_tags = soup.find_all("loc")
        for tag in loc_tags:
            loc = tag.text.strip()
            if loc:
                urls.append(loc)
    except Exception:
        pass
    return urls

def is_same_site(url: str, base: str) -> bool:
    u = urlparse(url)
    b = urlparse(base)
    return (u.scheme, u.netloc) == (b.scheme, b.netloc)

def normalize_bases(inp: str) -> t.List[str]:
    parts = [p.strip() for p in inp.split(",") if p.strip()]
    norm = []
    for p in parts:
        if not p.startswith("http"):
            p = "https://" + p
        norm.append(p.rstrip("/"))
    return list(dict.fromkeys(norm))

def extract_visitable_links(html: str, base_url: str) -> t.List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("#") or href.lower().startswith("mailto:") or href.lower().startswith("tel:"):
            continue
        abs_url = urljoin(base_url + "/", href)
        if is_same_site(abs_url, base_url):
            links.add(abs_url.split("#")[0])
    return list(links)

def get_text_content(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    # Remove scripts/styles
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text)
    return text

@dataclass
class Product:
    brand: str
    name: str
    aliases: t.List[str] = field(default_factory=list)

    def patterns(self) -> t.List[re.Pattern]:
        # Build patterns for exact-ish name and aliases (case-insensitive).
        items = [self.name] + self.aliases
        pats = []
        for it in items:
            it = it.strip()
            if not it:
                continue
            # word-boundary-ish matching, allow punctuation/space between tokens
            token_regex = r"\b" + re.escape(it).replace(r"\ ", r"[ \u00A0\-_/]*") + r"\b"
            pats.append(re.compile(token_regex, flags=re.I))
        return pats

def parse_csv_products(file_bytes: bytes) -> t.List[Product]:
    decoded = file_bytes.decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(decoded))
    prods: t.List[Product] = []
    for row in reader:
        brand = (row.get("brand") or row.get("Brand") or "").strip()
        name = (row.get("product") or row.get("Product") or row.get("name") or "").strip()
        aliases_raw = (row.get("aliases") or row.get("Aliases") or "").strip()
        aliases = [a.strip() for a in aliases_raw.split("|") if a.strip()] if aliases_raw else []
        if brand and name:
            prods.append(Product(brand=brand, name=name, aliases=aliases))
    return prods

def parse_pasted_products(pasted: str) -> t.List[Product]:
    # Expect lines like: Brand,Product,alias1|alias2
    prods: t.List[Product] = []
    for line in pasted.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 2:
            brand, name = parts[0], parts[1]
            aliases = []
            if len(parts) >= 3:
                aliases = [a.strip() for a in parts[2].split("|") if a.strip()]
            prods.append(Product(brand=brand, name=name, aliases=aliases))
    return prods

def rate_limited_get(url: str, cache: dict, delay_s: float) -> t.Optional[str]:
    if url in cache:
        return cache[url]
    txt = fetch(url)
    if delay_s > 0:
        time.sleep(delay_s)
    cache[url] = txt
    return txt

# ----------------------------
# UI
# ----------------------------

st.title("🔎 Brand & Product Page Finder")
st.write("Find where your products are mentioned across your site(s), grouped by brand.")

with st.expander("How it works / Tips", expanded=False):
    st.markdown("""
- Enter one or more site base URLs (comma-separated). Example: `https://technomeow.com, https://technobark.com`
- Add products via **CSV upload** (columns: `brand,product,aliases`) or paste as lines `Brand,Product,alias1|alias2`.
- The app will try to read your **sitemap(s)** for URLs. If none found, it will **crawl** starting at your homepage up to a limit.
- It scans page text for brand/product **exact-ish** matches (aliases help for variants).
- Export your results to **CSV**.
    """)

colA, colB = st.columns([2,1])

with colA:
    sites_input = st.text_input("Your site base URL(s) (comma-separated)", placeholder="https://technomeow.com, https://technobark.com")
with colB:
    crawl_limit = st.number_input("Max pages per site (if no sitemap)", min_value=50, max_value=5000, value=500, step=50)

uploaded = st.file_uploader("Upload CSV of products (brand,product,aliases)", type=["csv"], accept_multiple_files=False)
pasted = st.text_area("Or paste products (one per line: Brand,Product,alias1|alias2)", height=150, placeholder="Tractive,Tractive GPS Cat Tracker,Tractive Cat Collar|Tractive Tracker\nPetSafe,Smart Feed Automatic Feeder,SmartFeed|Smart Feeder")

colC, colD, colE = st.columns([1,1,1])
with colC:
    delay_s = st.number_input("Delay between requests (seconds)", min_value=0.0, max_value=5.0, value=0.2, step=0.1)
with colD:
    require_brand_match = st.checkbox("Require brand & product on page", value=False, help="If checked, only count a page if BOTH the brand name and product name appear.")
with colE:
    search_case_sensitive = st.checkbox("Case sensitive search", value=False)

run = st.button("Run Scan")

# ----------------------------
# Main logic
# ----------------------------
if run:
    bases = normalize_bases(sites_input) if sites_input.strip() else []
    if not bases:
        st.error("Please enter at least one site base URL.")
        st.stop()

    products: t.List[Product] = []
    if uploaded:
        try:
            products = parse_csv_products(uploaded.getvalue())
        except Exception as e:
            st.error(f"Could not parse CSV: {e}")
            st.stop()
    if not uploaded and pasted.strip():
        products = parse_pasted_products(pasted)

    if not products:
        st.error("Please provide at least one product (CSV or pasted lines).")
        st.stop()

    # Precompile brand/product patterns
    brand_patterns: dict[str, re.Pattern] = {}
    for p in products:
        if p.brand not in brand_patterns:
            flags = 0 if search_case_sensitive else re.I
            brand_patterns[p.brand] = re.compile(r"\b" + re.escape(p.brand) + r"\b", flags=flags)

    st.info("Starting scan… This may take a few minutes depending on your site size.")

    # Gather URLs per site from sitemaps; fallback to crawl.
    site_urls: dict[str, t.List[str]] = {}
    for base in bases:
        st.write(f"**Indexing:** {base}")
        urls: t.List[str] = []
        sitemaps = find_sitemaps(base)
        sm_urls: t.Set[str] = set()

        if sitemaps:
            st.write(f"• Found sitemaps: {', '.join(sitemaps)}")
            for sm in sitemaps:
                txt = fetch(sm)
                if not txt:
                    continue
                for u in parse_sitemap(txt):
                    if u.endswith(".xml") or u.endswith(".xml.gz"):
                        # It's a child sitemap; fetch it too
                        txt2 = fetch(u)
                        if not txt2:
                            continue
                        for u2 in parse_sitemap(txt2):
                            if is_same_site(u2, base):
                                sm_urls.add(u2.split("#")[0])
                    else:
                        if is_same_site(u, base):
                            sm_urls.add(u.split("#")[0])
            urls = list(sorted(sm_urls))
            st.write(f"• URLs from sitemap: {len(urls)}")
        else:
            st.write("• No sitemap found; crawling up to limit…")
            visited = set()
            q = queue.Queue()
            q.put(base + "/")
            while not q.empty() and len(visited) < crawl_limit:
                u = q.get()
                if u in visited:
                    continue
                visited.add(u)
                html = fetch(u)
                if not html:
                    continue
                for link in extract_visitable_links(html, base):
                    if link not in visited and is_same_site(link, base):
                        q.put(link)
            urls = list(sorted(visited))
            st.write(f"• Crawled URLs: {len(urls)}")

        site_urls[base] = urls

    # Scan pages
    results: t.List[dict] = []
    cache: dict[str, t.Optional[str]] = {}
    total_pages = sum(len(v) for v in site_urls.values())
    prog = st.progress(0.0, text="Scanning pages…")
    seen_pairs = set()

    # Prepare product patterns once
    product_patterns: dict[tuple, t.List[re.Pattern]] = {}
    for p in products:
        product_patterns[(p.brand, p.name)] = p.patterns()

    processed = 0
    for base, urls in site_urls.items():
        for url in urls:
            html = rate_limited_get(url, cache, delay_s)
            processed += 1
            prog.progress(min(processed/ max(total_pages,1), 1.0), text=f"Scanning {processed}/{total_pages}")

            if not html:
                continue
            text = get_text_content(html)

            for p in products:
                # Brand check
                brand_ok = True
                if require_brand_match:
                    bp = brand_patterns.get(p.brand)
                    brand_ok = bool(bp.search(text)) if bp else False
                # Product check
                pats = product_patterns[(p.brand, p.name)]
                prod_ok = any(pt.search(text) for pt in pats)

                if prod_ok and brand_ok:
                    key = (p.brand, p.name, url)
                    if key in seen_pairs:
                        continue
                    seen_pairs.add(key)
                    results.append({
                        "brand": p.brand,
                        "product": p.name,
                        "url": url,
                    })

    if not results:
        st.warning("No matches found. Try adding aliases, unchecking 'Require brand & product', or verifying sitemap/crawl limits.")
    else:
        st.success(f"Found {len(results)} product mentions.")
        # Build a grouped display
        import pandas as pd
        df = pd.DataFrame(results).sort_values(["brand", "product", "url"])
        st.dataframe(df, use_container_width=True)
        # Download
        out = io.StringIO()
        df.to_csv(out, index=False)
        st.download_button("Download CSV", out.getvalue(), file_name="brand_product_pages.csv", mime="text/csv")

        # Also show a pivot-like summary: product -> count of pages
        summary = (
            df.groupby(["brand", "product"])["url"]
            .nunique()
            .reset_index(name="pages_found")
            .sort_values(["brand", "product"])
        )
        st.markdown("**Summary (unique pages per product):**")
        st.dataframe(summary, use_container_width=True)
        out2 = io.StringIO()
        summary.to_csv(out2, index=False)
        st.download_button("Download Summary CSV", out2.getvalue(), file_name="brand_product_summary.csv", mime="text/csv")

st.markdown("---")
st.caption("Tip: Add common aliases (e.g., 'Litter-Robot 4|Litter Robot 4|LR4') to improve matching.")
