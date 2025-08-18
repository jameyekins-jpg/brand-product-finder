
import re
import io
import csv
import json
import time
import queue
import typing as t
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

import streamlit as st
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

st.set_page_config(page_title="Brand/Product Page Finder", layout="wide")

# ----------------------------
# Helpers
# ----------------------------

USER_AGENT = "Mozilla/5.0 (compatible; BrandProductFinder/1.6; +https://example.com)"
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
    # spaces/hyphens/underscores/slashes interchangeable
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
    # Accepts: Brand,Product,Alias1|Alias2  | Brand,Product  | Brand,  | Product
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

# --- Product autodetection ---

def jsonld_products(html: str) -> t.List[tuple[str,str]]:
    """Return list of (brand, product_name) from JSON-LD Product nodes if present."""
    out = []
    try:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all("script", {"type":"application/ld+json"}):
            try:
                data = json.loads(tag.string or "")
            except Exception:
                continue
            nodes = data if isinstance(data, list) else [data]
            for n in nodes:
                if isinstance(n, dict):
                    tval = n.get("@type")
                    types = [tval] if isinstance(tval, str) else (tval or [])
                    if "Product" in types or (isinstance(types, list) and any(t for t in types if isinstance(t,str) and t.lower()=="product")):
                        name = (n.get("name") or "").strip()
                        b = n.get("brand")
                        bname = ""
                        if isinstance(b, dict):
                            bname = (b.get("name") or "").strip()
                        elif isinstance(b, str):
                            bname = b.strip()
                        if name:
                            out.append((bname or "Unknown", name))
    except Exception:
        pass
    return out

GENERIC_ENDINGS = re.compile(r"(?:\s+(?:review|guide|how to|best|top|vs\.?|comparison|overview|202\d|20\d{2}))+$", re.I)

def clean_product_phrase(brand: str, phrase: str) -> str:
    s = phrase.strip(" -–—:|.,)™® ")
    s = re.sub(GENERIC_ENDINGS, "", s).strip(" -–—:|.,)™® ")
    # remove leading brand
    if s.lower().startswith(brand.lower() + " "):
        s = s[len(brand)+1:].strip()
    return s

def detect_products_from_text(text: str, brand: str, max_per_page: int = 3) -> t.List[str]:
    """Heuristics to pull product names that co-occur with the brand in raw text."""
    out = []
    # Pattern 1: "Brand Product Name ..."
    p1 = re.compile(rf"\b{re.escape(brand)}(?:'s)?\s+([A-Z0-9][\w\-]*(?:\s+[A-Z0-9][\w\-]*){{0,7}})", re.I)
    # Pattern 2: "Product Name by Brand"
    p2 = re.compile(rf"([A-Z][\w\-]*(?:\s+[A-Z0-9][\w\-]*){{0,7}})\s+(?:by|from)\s+{re.escape(brand)}\b", re.I)

    for m in p1.finditer(text):
        phrase = clean_product_phrase(brand, m.group(1))
        if len(phrase.split()) >= 2:
            out.append(phrase)
    for m in p2.finditer(text):
        phrase = clean_product_phrase(brand, m.group(1))
        if len(phrase.split()) >= 2:
            out.append(phrase)

    # De-dup while keeping order
    seen = set()
    res = []
    for p in out:
        key = p.lower()
        if key not in seen:
            seen.add(key)
            res.append(p)
        if len(res) >= max_per_page:
            break
    return res

def title_guess(html: str) -> str:
    try:
        soup = BeautifulSoup(html, "html.parser")
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            return og["content"].strip()
        tw = soup.find("meta", attrs={"name":"twitter:title"})
        if tw and tw.get("content"):
            return tw["content"].strip()
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            return h1.get_text(strip=True)
        return ""
    except Exception:
        return ""

# ----------------------------
# UI
# ----------------------------

st.title("🔎 Brand & Product Page Finder")
st.write("Find where your brands and products are mentioned across your site(s). Use **brand-only** lines to list every page that mentions a brand.")

with st.expander("How it works & Input Examples", expanded=False):
    st.markdown("""
**Option 1 — Upload CSV** (columns must be exactly):
```
brand,product,aliases
```
- **brand** → Company name (e.g., Whisker, PetSafe, Tractive)
- **product** → Product name (e.g., Litter-Robot 4). Leave blank to search by **brand-only**.
- **aliases** *(optional)* → Alternate names, separated by `|` (e.g., `Litter Robot 4|LR4`)

**Option 2 — Paste lines (one per line)**
Accepted:
```
Brand,Product,Alias1|Alias2
Brand,Product
Brand,          # Brand-only (find any page with this brand)
Product         # Product-only (brand auto-detected if possible)
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
st.markdown("**Tip:** Add common aliases (e.g., `Litter-Robot 4|Litter Robot 4|LR4`) to improve matching.")

col1, col2, col3 = st.columns([1,1,1])
with col1:
    delay_s = st.number_input("Delay between requests (seconds)", min_value=0.0, max_value=2.0, value=0.1, step=0.1)
with col2:
    max_workers = st.slider("Parallel requests", min_value=1, max_value=16, value=8)
with col3:
    stop_after = st.number_input("Stop after N pages per product (0 = no limit)", min_value=0, max_value=1000, value=0)

col4, col5 = st.columns([2,1])
with col4:
    exclude_patterns = st.text_input("Exclude URL patterns (comma-separated)", placeholder="/tag/, /category/, /page/, /feed/")
with col5:
    require_brand_match = st.checkbox("Require brand & product on page", value=False)

search_case_sensitive = st.checkbox("Case sensitive search", value=False)
auto_detect = st.checkbox("Auto-detect product names from JSON-LD/title/text when possible", value=True)
max_names = st.slider("Max detected product names per page", 1, 5, 3)
run = st.button("Run Scan")

# ----------------------------
# Main logic
# ----------------------------
if run:
    try:
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

        # Build brand patterns and a catalog of product patterns by brand
        brand_patterns: dict[str, re.Pattern] = {}
        for p in products:
            if p.brand not in brand_patterns:
                brand_patterns[p.brand] = compile_brand_pattern(p.brand, search_case_sensitive)

        catalog_patterns: dict[str, list[tuple[str, re.Pattern]]] = {}
        for p in products:
            if not p.brand_only and p.name:
                for pat in p.product_patterns(search_case_sensitive):
                    catalog_patterns.setdefault(p.brand, []).append((p.name, pat))

        st.info("Indexing your site(s)…")

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

            patterns = [p.strip() for p in (exclude_patterns or "").split(",") if p.strip()]
            if patterns:
                before = len(urls)
                urls = [u for u in urls if not any(pat in u for pat in patterns)]
                st.write(f"• Excluding patterns {patterns}: removed {before - len(urls)} URLs")

            site_urls[base] = urls

        st.info("Scanning pages…")
        all_urls = [u for urls in site_urls.values() for u in urls]

        results: t.List[dict] = []
        seen = set()

        def scan_url(url: str):
            html = fetch(url)
            if not html:
                return []
            text = get_text_content(html)

            rows = []

            # Structured/semantic signals
            jd_pairs = jsonld_products(html) if auto_detect else []
            title = title_guess(html) if auto_detect else ""

            for p in products:
                # Brand-only
                if p.brand_only:
                    bp = brand_patterns.get(p.brand)
                    if bp and bp.search(text):
                        hit_products = set()
                        # Catalog matches
                        for pname, pat in catalog_patterns.get(p.brand, []):
                            if pat.search(text):
                                hit_products.add(pname)
                        # JSON-LD brand products
                        if not hit_products and jd_pairs:
                            for bname, pname in jd_pairs:
                                if bp.search(bname or "") and pname:
                                    hit_products.add(pname.strip())
                        # Title fallback
                        if not hit_products and title and bp.search(title):
                            hit_products.add(clean_product_phrase(p.brand, title))
                        # Text heuristics (NEW)
                        if auto_detect and len(hit_products) == 0:
                            for ph in detect_products_from_text(text, p.brand, max_per_page=max_names):
                                hit_products.add(ph)

                        if not hit_products:
                            rows.append({"brand": p.brand, "product": "(any)", "url": url})
                        else:
                            for pname in sorted(hit_products):
                                rows.append({"brand": p.brand, "product": pname, "url": url})
                    continue

                # Product search (brand + product, or product-only)
                pats = [pat for pat in catalog_patterns.get(p.brand, []) if pat[0] == p.name]
                if not pats:
                    pats = [(p.name, pat) for pat in p.product_patterns(search_case_sensitive)]
                product_hit = any(pat.search(text) for _name, pat in pats)

                if product_hit:
                    out_brand = p.brand
                    if (out_brand == "Unknown" or not require_brand_match) and auto_detect:
                        for bname, pname in jd_pairs:
                            if pname and any(pat.search(pname) for _n, pat in pats):
                                out_brand = bname or out_brand
                                break
                    if require_brand_match:
                        bp = brand_patterns.get(p.brand)
                        if bp and not bp.search(text):
                            continue
                    rows.append({"brand": out_brand or p.brand, "product": p.name, "url": url})

            return rows

        processed = 0
        progress = st.progress(0.0, text=f"0 / {len(all_urls)}")
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for out in ex.map(scan_url, all_urls, chunksize=1):
                for row in out:
                    key = (row["brand"], row["product"], row["url"])
                    if key not in seen:
                        seen.add(key)
                        results.append(row)
                processed += 1
                progress.progress(min(processed / max(len(all_urls), 1), 1.0), text=f"{processed} / {len(all_urls)}")

        if not results:
            st.warning("No matches found. For brand-only, add a line like 'PetSafe,'. For product-only, paste just the product name. Add aliases to catch variants.")
        else:
            st.success(f"Found {len(results)} mentions.")
            df = pd.DataFrame(results).sort_values(["brand", "product", "url"])

            st.markdown("### Results — grouped by Brand → Product → URLs")
            for (brand, product), sub in df.groupby(["brand", "product"]):
                with st.expander(f"{brand} › {product}  ({len(sub)} page{'s' if len(sub)!=1 else ''})", expanded=False):
                    st.dataframe(sub[["url"]], use_container_width=True, hide_index=True)

            st.markdown("### Full table")
            st.dataframe(df, use_container_width=True)
            out = io.StringIO(); df.to_csv(out, index=False)
            st.download_button("Download CSV", out.getvalue(), file_name="brand_product_pages.csv", mime="text/csv")

            summary = (
                df.groupby(["brand", "product"])["url"]
                .nunique()
                .reset_index(name="pages_found")
                .sort_values(["brand", "product"])
            )
            st.markdown("**Summary (unique pages per product):**")
            st.dataframe(summary, use_container_width=True)
            out2 = io.StringIO(); summary.to_csv(out2, index=False)
            st.download_button("Download Summary CSV", out2.getvalue(), file_name="brand_product_summary.csv", mime="text/csv")

    except Exception as e:
        st.error("The app hit an error while running.")
        st.exception(e)

st.markdown("---")
st.caption("Use brand-only lines like 'PetSafe,' to list every page that mentions that brand. Product-only lines are supported too.")
