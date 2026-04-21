import asyncio
import json
import re
from urllib.parse import urlparse
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

MAX_LISTINGS_PER_SITE = 8
MAX_CONCURRENT_SITES = 3  # mag wat hoger, we doen alleen lijstpagina's

SCRAPER_CONFIG = [
    {"id": "ladresse_tournon",    "url": "https://www.ladresse.com/agence/l-adresse-tournon-d-agenais/266/acheter?sort=date-desc", "base": "https://www.ladresse.com",                          "pattern": r"/annonce/"},
    {"id": "beauxvillages",       "url": "https://beauxvillages.com/en/latest-properties?hotsheet=1",                              "base": "https://beauxvillages.com",                         "pattern": r"/property/"},
    {"id": "lot_immoco",          "url": "https://www.lot-immoco.net/a-vendre/1",                                                  "base": "https://www.lot-immoco.net",                        "pattern": r"^/[0-9]+-.*\.html$"},
    {"id": "pouget",              "url": "https://www.agencespouget.com/vente/1",                                                  "base": "https://www.agencespouget.com",                     "pattern": r"/vente/"},
    {"id": "human",               "url": "https://www.human-immobilier.fr/achat-maison-tarn-et-garonne?og=0&sort=date-desc",       "base": "https://www.human-immobilier.fr",                   "pattern": r"/annonce-"},
    {"id": "xavier",              "url": "https://xavierimmobilier.fr/gb/15-properties-up-to-350000",                             "base": "https://xavierimmobilier.fr",                       "pattern": r"/[0-9]+-"},
    {"id": "wheeler",             "url": "https://wheeler-property.com/for-sale/",                                                 "base": "https://wheeler-property.com",                      "pattern": r"/properties/"},
    {"id": "mouly",               "url": "https://www.mouly-immobilier.com/recherche/",                                            "base": "https://www.mouly-immobilier.com",                  "pattern": r"/[0-9]+-"},
]

def fix_url(url, base):
    if not url:
        return None
    if not url.startswith("http"):
        return base.rstrip("/") + "/" + url.lstrip("/")
    return url

# ------------------------------
# PRICE EXTRACTION
# ------------------------------
def extract_price(text):
    if not text:
        return "N/A"
    matches = re.findall(r"(\d[\d\s\.]{3,})\s*€", text)
    if not matches:
        return "N/A"
    return f"{matches[-1].strip()} €"

# ------------------------------
# IMAGE FILTERING
# ------------------------------
def is_bad_image(src):
    src = src.lower()
    bad = ["logo", "icon", "svg", "placeholder", "sprite", "loader", "blank", "ondulation"]
    return any(b in src for b in bad)

def extract_image(card, base):
    img = card.find("img", attrs={"data-src": True})
    if img and img.get("data-src") and not is_bad_image(img["data-src"]):
        return fix_url(img["data-src"], base)

    img = card.find("img", src=True)
    if img and img.get("src") and not is_bad_image(img["src"]):
        return fix_url(img["src"], base)

    img = card.find("img", srcset=True)
    if img and img.get("srcset"):
        first = img["srcset"].split(",")[0].split()[0]
        if not is_bad_image(first):
            return fix_url(first, base)

    divs = card.find_all("div", style=True)
    for d in divs:
        style = d.get("style", "")
        m = re.search(r'url\((.*?)\)', style)
        if m:
            src = m.group(1).strip('"\'')
            if not is_bad_image(src):
                return fix_url(src, base)

    return None

# ------------------------------
# TITLE EXTRACTION
# ------------------------------
def extract_title(card, a):
    for tag in ["h1", "h2", "h3"]:
        h = card.find(tag)
        if h and h.get_text(strip=True):
            return h.get_text(strip=True)

    for tag in ["strong", "b"]:
        h = card.find(tag)
        if h and h.get_text(strip=True):
            return h.get_text(strip=True)

    t = a.get_text(" ", strip=True)
    if t and len(t.split()) > 2:
        return t

    txt = card.get_text(" ", strip=True)
    if txt and len(txt.split()) > 3:
        return txt[:120]

    return "Onbekend"

# ------------------------------
# LISTING VALIDATION
# ------------------------------
def looks_like_listing(card_text):
    return bool(re.search(r"\d[\d\s\.]{3,}\s*€", card_text))

# ------------------------------
# CARD DETECTION (verbeterd)
# ------------------------------
def find_card_from_anchor(a):
    """
    Klim omhoog tot een 'echte' kaart:
    - liefst <article>, <li>, <section>
    - anders een grotere <div> met genoeg tekst
    """
    node = a
    best_div = None

    for _ in range(8):
        if not node.parent:
            break
        node = node.parent

        if node.name in ("article", "li", "section"):
            return node

        if node.name == "div":
            text_len = len(node.get_text(" ", strip=True))
            if text_len > 80:  # heuristiek: grotere container
                best_div = node

    return best_div or a.parent

# ------------------------------
# SCRAPE LIST PAGE
# ------------------------------
async def scrape_list_page(browser, config):
    print(f"--- Start: {config['id']} ---")

    context = await browser.new_context(
        viewport={"width": 1280, "height": 800},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    )
    page = await context.new_page()

    await page.goto(config["url"], wait_until="domcontentloaded", timeout=25000)

    for _ in range(4):
        await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        await asyncio.sleep(1)

    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")

    pattern = re.compile(config["pattern"], re.I)

    anchors = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if pattern.search(href):
            anchors.append((a, fix_url(href, config["base"])))

    seen = set()
    unique = []
    for a, href in anchors:
        if href not in seen:
            seen.add(href)
            unique.append((a, href))

    listings = []
    for a, href in unique:
        card = find_card_from_anchor(a)
        card_text = card.get_text(" ", strip=True) if card else ""

        if not looks_like_listing(card_text):
            continue

        prijs = extract_price(card_text)
        foto = extract_image(card, config["base"]) or "N/A"
        titel = extract_title(card, a)

        listings.append({
            "Bron": config["id"],
            "Titel": titel,
            "Prijs": prijs,
            "Foto": foto,
            "URL": href
        })

        if len(listings) >= MAX_LISTINGS_PER_SITE:
            break

    print(f"  {config['id']}: {len(listings)} listings gevonden")
    await context.close()
    return listings

# ------------------------------
# MAIN
# ------------------------------
async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )

        sem = asyncio.Semaphore(MAX_CONCURRENT_SITES)

        async def sem_task(conf):
            async with sem:
                return await scrape_list_page(browser, conf)

        all_results = await asyncio.gather(*[sem_task(c) for c in SCRAPER_CONFIG])
        flat = [item for sub in all_results for item in sub]

        with open("data.json", "w", encoding="utf-8") as f:
            json.dump(flat, f, ensure_ascii=False, indent=2)

        print(f"\n✅ Klaar! Totaal {len(flat)} panden gescraped.")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
