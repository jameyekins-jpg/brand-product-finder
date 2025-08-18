
import re
import io
import csv
import time
import queue
import typing as t
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

import streamlit as st
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(page_title="Brand/Product Page Finder", layout="wide")

# ----------------------------
# Helpers
# ----------------------------

USER_AGENT = "Mozilla/5.0 (compatible; BrandProductFinder/1.3; +https://example.com)"
DEFAULT_TIMEOUT = 15

def fetch(url: str) -> t.Optional[str]:
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=DEFAULT_TIMEOUT)
        if resp.status_code == 200:
            return resp.text
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
    urls: t.List[str] = []
    try:
        soup = BeautifulSoup(xml_text, "xml")
        for tag in soup.find_all("loc"):
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
        if href.startswith("#") or href.lower().startswith(("mailto:", "tel:")):
            continue
        abs_url = urljoin(base_url + "/", href)
        if is_same_site(abs_url, base_url):
            links.add(abs_url.split("#")[0])
    return list(links)

def get_text_content(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return text

def flexible_token_regex(s: str) -> str:
    # Treat spaces, hyphens, underscores, slashes as interchangeable
    return re.escape(s).replace(r"\ ", r"[ \u00A0\-\_/]*")

@dataclass
class Product:
    brand: str
    name: str  # empty string means brand-only search
    aliases: t.List[str] = field(default_factory=list)
    brand_only: bool = False

    def product_patterns(self, case_sensitive: bool) -> t.List[re.Pattern]:
        if self.brand_only:
            return []
        items = [self.name] + self.aliases
        flags = 0 if case_sensitive else re.I
        return [re.compile(r"\b" + flexible_token_regex(it) + r"\b", flags=flags) for it in items if it]

def compile_brand_pattern(brand: str, case_sensitive: bool) -> re.Pattern:
    flags = 0 if case_sensitive else re.I
    return re.compile(r"\b" + flexible_token_regex(brand) + r"\b", flags=flags)

def parse_csv_products(file_bytes: bytes) -> t.List[Product]:
    decoded = file_bytes.decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(decoded))
    prods: t.List[Product] = []
    for row in reader:
        brand = (row.get("brand") or row.get("Brand") or "").strip() or "Unknown"
        name = (row.get("product") or row.get("Product") or row.get("name") or "").strip()
        aliases_raw = (row.get("aliases") or row.get("Aliases") or "").strip()
        aliases = [a.strip() for a in aliases_raw.split("|") if a.strip()] if aliases_raw else []
        if name:
            prods.append(Product(brand=brand, name=name, aliases=aliases, brand_only=False))
        else:
            prods.append(Product(brand=brand, name="", aliases=[], brand_only=True))
    return prods

def parse_pasted_products(pasted: str) -> t.List[Product]:
    # Accepts Brand,Product,Alias1|Alias2  | Brand,Product  | Brand,  | Product
    prods: t.List[Product] = []
    for raw in pasted.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 2:
            brand, name = parts[0] or "Unknown", parts[1]
            if name:
                aliases = [a.strip() for a in parts[2].split("|")] if len(parts) >=3 and parts[2] else []
                prods.append(Product(brand=brand, name=name, aliases=aliases, brand_only=False))
            else:
                prods.append(Product(brand=brand, name="", aliases=[], brand_only=True))
        else:
            prods.append(Product(brand="Unknown", name=line, aliases=[], brand_only=False))
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
st.write("Find where your brands and products are mentioned across your site(s). Use **brand-only** lines to list every page that mentions a brand.")

with st.expander("How it works & Input Examples", expanded=False):
    st.markdown("""
**What this does**
- Reads your **sitemap(s)** (or crawls if none found)
- Scans pages for **brand and/or product** mentions
- Outputs **Product → URLs** and a **summary CSV**

**Add products (two options)**

**Option 1 — Upload CSV** (columns must be exactly):
```
brand,product,aliases
```
- **brand** → Company name (e.g., Whisker, PetSafe, Tractive)
- **product** → Product name (e.g., Litter-Robot 4). Leave blank to search by **brand-only**.
- **aliases** *(optional)* → Alternate names, separated by `|` (e.g., `Litter Robot 4|LR4`)

**Option 2 — Paste lines (one per line)**
Formats accepted:
```
Brand,Product,Alias1|Alias2
Brand,Product
Brand,          # Brand-only (find any page with this brand)
Product         # Product-only (brand will be set to 'Unknown')
```
Examples:
```
Whisker,Litter-Robot 4,Litter Robot 4|LR4
PetSafe,                         # brand-only search for PetSafe
Tractive,Tractive GPS Cat Tracker,Tractive Cat Collar|Tractive Tracker
Litter-Robot 4                   # product-only
```
""")

colA, colB = st.columns([2,1])
with colA:
    sites_input = st.text_input("Your site base URL(s) (comma-separated)", placeholder="https://technomeow.com, https://technobark.com")
with colB:
    crawl_limit = st.number_input("Max pages per site (if no sitemap)", min_value=50, max_value=10000, value=800, step=50)

uploaded = st.file_uploader("Upload CSV of products (brand,product,aliases)", type=["csv"], accept_multiple_files=False)

pasted_ph = (
"Whisker,Litter-Robot 4,Litter Robot 4|LR4\n"
"PetSafe,                         # Brand-only\n"
"Tractive,Tractive GPS Cat Tracker,Tractive Cat Collar|Tractive Tracker\n"
"Litter-Robot 4                   # Product-only"
)
pasted = st.text_area("Or paste products (one per line: Brand,Product,alias1|alias2)", height=180, placeholder=pasted_ph)

# *** Move TIP directly under the paste box ***
st.markdown("**Tip:** Add common aliases (e.g., `Litter-Robot 4|Litter Robot 4|LR4`) to improve matching.")

colC, colD, colE = st.columns([1,1,1])
with colC:
    delay_s = st.number_input("Delay between requests (seconds)", min_value=0.0, max_value=2.0, value=0.1, step=0.1,
                              help="Lower = faster but heavier on your server. Try 0.0–0.2 for speed.")
with colD:
    max_workers = st.slider("Parallel requests", min_value=1, max_value=16, value=8, help="Higher = faster. If your server slows down, lower this.")
with colE:
    stop_after = st.number_input("Stop after N pages per product (0 = no limit)", min_value=0, max_value=1000, value=0)

colF, colG = st.columns([2,1])
with colF:
    exclude_patterns = st.text_input("Exclude URL patterns (comma-separated)", placeholder="/tag/, /category/, /page/, /feed/")
with colG:
    require_brand_match = st.checkbox("Require brand & product on page", value=False, help="Only count a page if BOTH appear. (Ignored for brand-only lines.)")

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
        st.error("Please provide at least one product or brand line (CSV or pasted lines).")
        st.stop()

    # Compile patterns
    brand_patterns: dict[str, re.Pattern] = {}
    for p in products:
        if p.brand not in brand_patterns:
            brand_patterns[p.brand] = compile_brand_pattern(p.brand, search_case_sensitive)

    st.info("Indexing your site(s)…")

    # Gather URLs per site
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

        # Apply exclusions
        patterns = [p.strip() for p in (exclude_patterns or "").split(",") if p.strip()]
        if patterns:
            before = len(urls)
            urls = [u for u in urls if not any(pat in u for pat in patterns)]
            st.write(f"• Excluding patterns {patterns}: removed {before - len(urls)} URLs")

        site_urls[base] = urls

    # Prepare product patterns once
    product_patterns: dict[tuple, t.List[re.Pattern]] = {}
    for p in products:
        product_patterns[(p.brand, p.name, p.brand_only)] = p.product_patterns(search_case_sensitive)

    st.info("Scanning pages…")
    all_urls = [u for urls in site_urls.values() for u in urls]
    cache: dict[str, t.Optional[str]] = {}
    results: t.List[dict] = []
    seen_pairs = set()
    per_product_counts: dict[tuple, int] = {}

    def scan_url(url: str):
        html = rate_limited_get(url, cache, delay_s)
        if not html:
            return []
        text = get_text_content(html)

        found = []
        for p in products:
            key_pp = (p.brand, p.name, p.brand_only)
            if p.brand_only:
                bp = brand_patterns.get(p.brand)
                if bp and bp.search(text):
                    key = (p.brand, "(any)", url)
                    if key not in seen_pairs:
                        found.append({"brand": p.brand, "product": "(any)", "url": url})
                continue

            brand_ok = True
            if require_brand_match:
                bp = brand_patterns.get(p.brand)
                brand_ok = bool(bp.search(text)) if bp else False

            pats = product_patterns[key_pp]
            prod_ok = any(pt.search(text) for pt in pats)

            if prod_ok and brand_ok:
                key = (p.brand, p.name, url)
                if key not in seen_pairs:
                    found.append({"brand": p.brand, "product": p.name, "url": url})
        return found

    # Run in parallel
    progress = st.progress(0.0, text=f"0 / {len(all_urls)}")
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(scan_url, url) for url in all_urls]
        for fut in as_completed(futures):
            out = fut.result()
            if out:
                for row in out:
                    key = (row["brand"], row["product"], row["url"])
                    if key not in seen_pairs:
                        seen_pairs.add(key)
                        results.append(row)
                        key_pp = (row["brand"], row["product"])
                        per_product_counts[key_pp] = per_product_counts.get(key_pp, 0) + 1
            completed += 1
            progress.progress(min(completed / max(len(all_urls), 1), 1.0), text=f"{completed} / {len(all_urls)}")

    if not results:
        st.warning("No matches found. For brand-only, add a line like 'PetSafe,'. For product searches, add aliases or uncheck 'Require brand & product'.")
    else:
        st.success(f"Found {len(results)} mentions.")
        df = pd.DataFrame(results).sort_values(["brand", "product", "url"])
        st.dataframe(df, use_container_width=True)
        out = io.StringIO()
        df.to_csv(out, index=False)
        st.download_button("Download CSV", out.getvalue(), file_name="brand_product_pages.csv", mime="text/csv")

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
st.caption("Use brand-only lines like 'PetSafe,' to list every page that mentions that brand.")
