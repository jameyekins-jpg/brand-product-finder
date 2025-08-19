
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

BUILD_VERSION = "2.4.0"

st.set_page_config(page_title="Brand/Product Page Finder", layout="wide")

# ----------------------------
# Helpers
# ----------------------------

USER_AGENT = "Mozilla/5.0 (compatible; BrandProductFinder/" + BUILD_VERSION + "; +https://example.com)"
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

from dataclasses import dataclass, field
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

# --- Product autodetection & cleanup ---

def jsonld_products(html: str) -> t.List[tuple[str,str]]:
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
                    is_product = False
                    if isinstance(types, list):
                        for _t in types:
                            if isinstance(_t, str) and _t.lower() == "product":
                                is_product = True
                                break
                    elif isinstance(types, str) and types.lower() == "product":
                        is_product = True
                    if is_product:
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

STOPWORDS = {"and","or","with","for","to","the","a","an","in","on","of","by"}
GENERIC_BAD_TOKENS = {"guide","beginners","beginner","best","top","vs","comparison","compare","review","reviews","let","peace",
                      "how","why","what","actually","really","lowest","entrance","ultimate","complete","2020","2021","2022","2023","2024","2025"}

def clean_phrase_tokens(s: str) -> str:
    s = s.strip(" -–—:|.,)™®(")
    tokens = s.split()
    if not tokens:
        return ""
    if tokens[0].isdigit():
        return ""
    kept = []
    for tok in tokens:
        low = tok.lower()
        if low in GENERIC_BAD_TOKENS or low in STOPWORDS:
            break
        if tok.isalpha() and tok.islower():
            break
        if any(ch in tok for ch in [",",";","/"]):
            break
        kept.append(tok)
    return " ".join(kept).strip(" -–—:|.,)™®(")

def title_guess(html: str) -> str:
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return ""
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return og["content"].strip()
    tw = soup.find("meta", attrs={"name":"twitter:title"})
    if tw and tw.get("content"):
        return tw["content"].strip()
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)
    title = soup.find("title")
    if title and title.get_text(strip=True):
        return title.get_text(strip=True)
    return ""

# Full implementation (text-based)
def detect_products_from_text(text: str, brand: str, max_per_page: int, require_brand_in_name: bool,
                              ignore_words: set[str], other_brands: set[str]) -> t.List[str]:
    out = []
    for m in re.finditer(rf"\b{re.escape(brand)}(?:'s)?\s+([A-Z][\w\-]*(?:\s+[A-Z0-9][\w\-]*){{0,6}})", text, flags=re.I):
        phrase = clean_phrase_tokens(m.group(1))
        if phrase:
            out.append(phrase)
    for m in re.finditer(rf"([A-Z][\w\-]*(?:\s+[A-Z0-9][\w\-]*){{0,6}})\s+(?:by|from)\s+{re.escape(brand)}\b", text, flags=re.I):
        phrase = clean_phrase_tokens(m.group(1))
        if phrase:
            out.append(phrase)
    seen=set(); res=[]
    for p in out:
        pl = p.lower()
        if any(ob in pl for ob in other_brands) or any(w in pl for w in ignore_words):
            continue
        if require_brand_in_name and brand.lower() not in pl:
            p = f"{brand} {p}"; pl = p.lower()
        if len(p.split()) < 2:
            continue
        if pl not in seen:
            seen.add(pl); res.append(p)
        if len(res) >= max_per_page:
            break
    return res

def detect_products_from_html(html: str, brand: str, max_per_page: int, require_brand_in_name: bool,
                              ignore_words: set[str], other_brands: set[str]) -> t.List[str]:
    soup = BeautifulSoup(html, "html.parser")
    candidates = []
    for tag in soup.find_all(["h1","h2","h3","h4","strong","b","li","a"]):
        txt = tag.get_text(" ", strip=True)
        if txt:
            candidates.append(txt)
    out = []
    pat_prefix = re.compile(rf"\b{re.escape(brand)}(?:'s)?\s+([A-Z][\w\-]*(?:\s+[A-Z0-9][\w\-]*){{0,6}})", re.I)
    pat_suffix = re.compile(rf"([A-Z][\w\-]*(?:\s+[A-Z0-9][\w\-]*){{0,6}})\s+(?:by|from)\s+{re.escape(brand)}\b", re.I)
    for text in candidates:
        for m in pat_prefix.finditer(text):
            phrase = clean_phrase_tokens(m.group(1))
            if phrase:
                out.append(phrase)
        for m in pat_suffix.finditer(text):
            phrase = clean_phrase_tokens(m.group(1))
            if phrase:
                out.append(phrase)
    seen=set(); res=[]
    for p in out:
        pl = p.lower()
        if any(ob in pl for ob in other_brands) or any(w in pl for w in ignore_words):
            continue
        if require_brand_in_name and brand.lower() not in pl:
            p = f"{brand} {p}"; pl = p.lower()
        if len(p.split()) < 2:
            continue
        if pl not in seen:
            seen.add(pl); res.append(p)
        if len(res) >= max_per_page:
            break
    return res

VARIANT_TOKENS = [
    r"wi[\s\-]?fi", "wifi", r"with\s+wi[\s\-]?fi", r"wi[\s\-]?fi\s+enabled", r"app[\s\-]?controlled", r"with\s+app",
    r"bluetooth", r"wireless"
]
def product_canonical_display(brand: str, name: str) -> str:
    s = name
    for vt in VARIANT_TOKENS:
        s = re.sub(rf"\b{vt}\b", "", s, flags=re.I)
    s = re.sub(r"\s{2,}", " ", s).strip(" -–—:|.,)™®(").strip()
    if s.lower().startswith(brand.lower() + " " + brand.lower()):
        s = s[len(brand)+1:]
    return s
def product_canonical_key(brand: str, name: str) -> str:
    s = product_canonical_display(brand, name).lower()
    s = re.sub(rf"\b{re.escape(brand.lower())}\b", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def _dedupe_within_url(rows: list[dict]) -> list[dict]:
    """
    Given rows [{'brand','product','url','title'}], collapse variants within the same brand+URL.
    Strategy:
      - Build a normalized key: lowercase, remove brand word, strip non-alnum.
      - Prefer the longest, most specific name per normalized key.
      - Additionally, if one name is a substring of another (after removing brand), keep the longer.
    """
    from collections import defaultdict
    grouped = defaultdict(list)
    for r in rows:
        grouped[(r["brand"].lower(), r["url"])].append(r)

    out = []
    for (brand_l, url), items in grouped.items():
        # First pass: choose the "best" per normalized key
        buckets = {}
        for it in items:
            name = it["product"]
            # normalize
            norm = re.sub(rf"\b{re.escape(brand_l)}\b", "", name.lower())
            norm = re.sub(r"[^a-z0-9 ]+", " ", norm)
            norm = re.sub(r"\s+", " ", norm).strip()
            key = re.sub(r"\s+", "", norm)
            # pick best by length (more specific)
            prev = buckets.get(key)
            if prev is None or len(name) > len(prev["product"]):
                buckets[key] = it

        # Second pass: if one surviving name is a substring of another, keep the longer
        keep = list(buckets.values())
        keep_names = [re.sub(rf"\b{re.escape(brand_l)}\b", "", k["product"].lower()).strip() for k in keep]
        final = []
        for i, it in enumerate(keep):
            ni = keep_names[i]
            longer_exists = any((i != j) and (ni in keep_names[j]) and (len(keep[j]["product"]) > len(it["product"])) for j in range(len(keep)))
            if not longer_exists:
                final.append(it)

        out.extend(final)
    return out
st.title("🔎 Brand & Product Page Finder")
st.caption("Build " + BUILD_VERSION + " — improved brand-only detection (headings/links/lists + text).")

keep_minutes = st.slider("Keep results visible for (minutes)", 1, 30, 10)

if "last_results" in st.session_state:
    rec = st.session_state["last_results"]
    age = time.time() - rec["ts"]
    if age < rec.get("ttl", 600):
        with st.expander(f"Last results (kept for {int((rec.get('ttl',600)-age)//60)+1} more min)", expanded=False):
            df_cached = pd.DataFrame(rec["rows"])
            if not df_cached.empty:
                df_cached = df_cached.rename(columns={"product":"Product Name","title":"Blog Post Name","url":"URL","brand":"Brand"})
                df_cached = df_cached[["Brand","Product Name","Blog Post Name","URL"]]
                st.dataframe(df_cached, use_container_width=True)
                out = io.StringIO(); df_cached.to_csv(out, index=False)
                st.download_button("Download full results (CSV)", out.getvalue(), file_name="brand_product_pages.csv", mime="text/csv", key="cached_full")
            else:
                st.write("No cached rows.")

with st.expander("How it works & Input Examples", expanded=False):
    st.markdown("""
**Paste or upload products**. One line per item:
```
Brand,Product,Alias1|Alias2
Brand,Product
Brand,          # brand-only
Product         # product-only
```
Examples:
```
Whisker,Litter-Robot 4,Litter Robot 4|LR4
PetSafe,
Tractive,Tractive GPS Cat Tracker,Tractive Cat Collar|Tractive Tracker
Litter-Robot 4
```
""")

colA, colB = st.columns([2,1])
with colA:
    sites_input = st.text_input("Your site base URL(s) (comma-separated)", placeholder="https://technomeow.com, https://technobark.com")
with colB:
    crawl_limit = st.number_input("Max pages per site (if no sitemap)", min_value=50, max_value=10000, value=800, step=50)

uploaded = st.file_uploader("Upload CSV of products (columns: brand,product,aliases)", type=["csv"], accept_multiple_files=False)

pasted_ph = (
"Whisker,Litter-Robot 4,Litter Robot 4|LR4\n"
"PetSafe,                         # Brand-only\n"
"Tractive,Tractive GPS Cat Tracker,Tractive Cat Collar|Tractive Tracker\n"
"Litter-Robot 4                   # Product-only"
)
pasted = st.text_area("Or paste products (one per line)", height=180, placeholder=pasted_ph)
treat_single_as_brand = st.checkbox("Treat single-field pasted lines as brands (recommended)", value=True)
st.markdown("**Tip:** Add common aliases (e.g., `Litter-Robot 4|Litter Robot 4|LR4`) to improve matching.")

col1, col2, col3 = st.columns([1,1,1])
with col1:
    delay_s = st.number_input("Delay between requests (seconds)", min_value=0.0, max_value=2.0, value=0.1, step=0.1)
with col2:
    max_workers = st.slider("Parallel requests", min_value=1, max_value=16, value=8)
with col3:
    stop_after = st.number_input("Stop after N pages per product (0 = no limit)", min_value=0, max_value=1000, value=0)

common_excludes_checked = st.checkbox("Exclude common paths (/tag/, /category/, /page/, /feed/, /reviews/, /author/)", value=True)
custom_excludes = st.text_input("Additional URL patterns to exclude (comma-separated)", value="")

col4, col5 = st.columns([2,1])
with col4:
    require_brand_match = st.checkbox("Require brand & product on page", value=False)
    collapse_variants = st.checkbox("Collapse near‑duplicate product names (e.g., Wi‑Fi/WiFi variants)", value=True)
dedupe_per_url = st.checkbox("De‑duplicate variants on the same page (keep most specific name)", value=True)
with col5:
    search_case_sensitive = st.checkbox("Case sensitive search", value=False)

strict_mode = st.checkbox("Strict brand proximity (reduce wrong products)", value=True)
require_brand_in_name = st.checkbox("Product name must include brand word (brand-only mode)", value=True)
ignore_words_text = st.text_input("Ignore product names containing these words (comma-separated)",
                                  value="guide,beginners,review,best,top,vs,comparison,peace,actually,lowest,entrance")
other_brands_text = st.text_input("Other brands/words to ignore (comma-separated)", value="Pet Snowy,CatLink,Satellai")
auto_detect = st.checkbox("Auto-detect product names from JSON-LD/title/text when possible", value=True)
max_names = st.slider("Max detected product names per page", 1, 20, 12)  # default higher
run = st.button("Run Scan")

# ----------------------------
# Parsing helpers
# ----------------------------

@dataclass
class _ProductPattern:
    brand: str
    name: str
    brand_only: bool
    pats: t.List[re.Pattern]

def product_patterns(p: Product, case_sensitive: bool) -> _ProductPattern:
    flags = 0 if case_sensitive else re.I
    if p.brand_only:
        return _ProductPattern(p.brand, p.name, True, [])
    items = [p.name] + p.aliases
    pats = [re.compile(r"\b" + flexible_token_regex(it) + r"\b", flags=flags) for it in items if it]
    return _ProductPattern(p.brand, p.name, False, pats)

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

def parse_pasted_products(pasted: str, treat_single_as_brand: bool) -> t.List[Product]:
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
            if treat_single_as_brand:
                prods.append(Product(brand=line, name="", aliases=[], brand_only=True))
            else:
                prods.append(Product(brand="Unknown", name=line, aliases=[], brand_only=False))
    return prods

def build_canonical_map(catalog_patterns: dict[str, list[tuple[str, re.Pattern]]]) -> dict[str, list[tuple[str, re.Pattern]]]:
    return catalog_patterns

def canonicalize_phrase(brand: str, phrase: str, canon_map: dict[str, list[tuple[str, re.Pattern]]], collapse_variants: bool) -> str:
    for cname, pat in canon_map.get(brand, []):
        if pat.search(phrase):
            return cname
    if collapse_variants:
        return product_canonical_display(brand, phrase)
    return phrase

# ----------------------------
# Main logic & output
# ----------------------------
def display_results(df: pd.DataFrame):
    st.markdown("### Full table")
    df_out = df.rename(columns={"brand":"Brand","product":"Product Name","title":"Blog Post Name","url":"URL"})
    df_out = df_out[["Brand","Product Name","Blog Post Name","URL"]]
    st.dataframe(df_out, use_container_width=True)
    out = io.StringIO(); df_out.to_csv(out, index=False)
    st.download_button("Download full table (CSV)", out.getvalue(), file_name="brand_product_pages.csv", mime="text/csv", key="full")

    summary = (
        df.groupby(["brand", "product"])["url"]
        .nunique()
        .reset_index(name="pages_found")
        .sort_values(["brand", "product"])
    )
    with st.expander("Summary (unique pages per product)", expanded=False):
        st.dataframe(summary, use_container_width=True)

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
            products = parse_pasted_products(pasted, treat_single_as_brand)
        if not products:
            st.error("Please provide at least one product or brand line (CSV or pasted lines).")
            st.stop()

        ignore_words = {w.strip().lower() for w in ignore_words_text.split(",") if w.strip()}
        other_brands = {w.strip().lower() for w in other_brands_text.split(",") if w.strip()}

        brand_patterns: dict[str, re.Pattern] = {}
        for p in products:
            if p.brand not in brand_patterns:
                brand_patterns[p.brand] = compile_brand_pattern(p.brand, search_case_sensitive)

        catalog_patterns: dict[str, list[tuple[str, re.Pattern]]] = {}
        compiled_products: list[_ProductPattern] = []
        for p in products:
            pp = product_patterns(p, search_case_sensitive)
            compiled_products.append(pp)
            if not p.brand_only and p.name:
                for pat in pp.pats:
                    catalog_patterns.setdefault(p.brand, []).append((p.name, pat))

        canon_map = build_canonical_map(catalog_patterns)

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

            patterns = []
            if common_excludes_checked:
                patterns.extend(["/tag/", "/category/", "/page/", "/feed/", "/reviews/", "/author/"])
            if custom_excludes:
                patterns.extend([p.strip() for p in custom_excludes.split(",") if p.strip()])
            if patterns:
                before = len(urls)
                urls = [u for u in urls if not any(p in u for p in patterns)]
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
            page_title = title_guess(html)

            rows = []
            jd_pairs = jsonld_products(html) if auto_detect else []

            for pp in compiled_products:
                if pp.brand_only:
                    bp = brand_patterns.get(pp.brand)
                    if bp and bp.search(text):
                        hit_products = set()
                        for pname, pat in catalog_patterns.get(pp.brand, []):
                            if pat.search(text):
                                hit_products.add(pname)
                        for bname, pname in jd_pairs:
                            if (bname or "").lower() == pp.brand.lower():
                                hit_products.add(canonicalize_phrase(pp.brand, pname.strip(), canon_map, collapse_variants))
                        for ph in detect_products_from_html(html, pp.brand, max_per_page=max_names,
                                                            require_brand_in_name=require_brand_in_name,
                                                            ignore_words=ignore_words, other_brands=other_brands):
                            hit_products.add(canonicalize_phrase(pp.brand, ph, canon_map, collapse_variants))
                        for ph in detect_products_from_text(text, pp.brand, max_per_page=max_names,
                                                            require_brand_in_name=require_brand_in_name,
                                                            ignore_words=ignore_words, other_brands=other_brands):
                            hit_products.add(canonicalize_phrase(pp.brand, ph, canon_map, collapse_variants))

                        for pname in sorted(hit_products):
                            rows.append({"brand": pp.brand, "product": pname, "url": url, "title": page_title})
                    continue

                prod_ok = any(pat.search(text) for pat in pp.pats)
                if prod_ok:
                    out_brand = pp.brand
                    if (out_brand == "Unknown" or not require_brand_match) and jd_pairs:
                        for bname, pname in jd_pairs:
                            if pname and any(pat.search(pname) for pat in pp.pats):
                                out_brand = bname or out_brand
                                break
                    if require_brand_match:
                        bp = brand_patterns.get(pp.brand)
                        if bp and not bp.search(text):
                            continue
                    canonical_name = canonicalize_phrase(out_brand or pp.brand, pp.name, canon_map, collapse_variants)
                    rows.append({"brand": out_brand or pp.brand, "product": canonical_name, "url": url, "title": page_title})
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

        if collapse_variants and results:
            compact = []
            seen_keys = set()
            for r in results:
                key = (r["brand"].lower(), product_canonical_key(r["brand"], r["product"]), r["url"])
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                r = dict(r)
                r["product"] = product_canonical_display(r["brand"], r["product"])
                compact.append(r)
            results = compact

        # Per-URL dedupe (keep most specific name on same page)
        if results and dedupe_per_url:
            results = _dedupe_within_url(results)

        if not results:
            st.warning("No product matches found. Tips: add aliases; keep 'Strict brand proximity' on; include product-only lines to target specific models.")
        else:
            df = pd.DataFrame(results).sort_values(["brand", "product", "url"])
            st.session_state["last_results"] = {"rows": df.to_dict(orient="records"), "ts": time.time(), "ttl": keep_minutes*60}
            st.success(f"Found {len(df)} product-page matches — results will be saved here for {keep_minutes} minutes unless you run another scan.")
            display_results(df)

    except Exception as e:
        st.error("The app hit an error while running.")
        st.exception(e)

st.markdown("---")
st.caption("Brand-only lines now use **headings, links, list items, and full-text** to extract product names. CSV = Full table.")
