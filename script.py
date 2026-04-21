import asyncio
import json
import re
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup

MAX_LISTINGS_PER_SITE = 8
MAX_CONCURRENT_SITES = 3

SCRAPER_CONFIG = [
    {
        "id": "ladresse_tournon",
        "url": "https://www.ladresse.com/agence/l-adresse-tournon-d-agenais/266/acheter?sort=date-desc",
        "base": "https://www.ladresse.com",
        "pattern": r"/annonce/",
    },
    {
        "id": "beauxvillages",
        "url": "https://beauxvillages.com/en/latest-properties?hotsheet=1",
        "base": "https://beauxvillages.com",
        "pattern": r"/property/",
    },
    {
        "id": "lot_immoco",
        "url": "https://www.lot-immoco.net/a-vendre/1",
        "base": "https://www.lot-immoco.net",
        "pattern": r"\.html$",
        "card_selector": "div.panel.panel-default",
    },
    {
        "id": "pouget",
        "url": "https://www.agencespouget.com/vente/1",
        "base": "https://www.agencespouget.com",
        "pattern": r"/vente/",
    },
    {
        "id": "human",
        "url": "https://www.human-immobilier.fr/achat-maison-tarn-et-garonne?og=0&sort=date-desc",
        "base": "https://www.human-immobilier.fr",
        "pattern": r"/annonce-",
    },
    {
        "id": "letuc",
        "url": "https://www.letuc.com/recherche/",
        "base": "https://www.letuc.com",
        "pattern": r"/t[0-9]+/",
    },
    {
        "id": "nestenn",
        "url": "https://immobilier-villeneuve-sur-lot.nestenn.com/achat-immobilier",
        "base": "https://immobilier-villeneuve-sur-lot.nestenn.com",
        "pattern": r"ref-",
    },
    {
        "id": "century21",
        "url": "https://www.century21-bg-villeneuve.com/annonces/achat/",
        "base": "https://www.century21-bg-villeneuve.com",
        "pattern": r"/detail/",
    },
    {
        "id": "lot_et_garonne",
        "url": "https://www.lot-et-garonne-immobilier.com/bien-a-acheter.html",
        "base": "https://www.lot-et-garonne-immobilier.com",
        "pattern": r"\.html$",
    },
    {
        "id": "valadie",
        "url": "https://valadie-immobilier.com/fr/biens/a_vendre/1",
        "base": "https://valadie-immobilier.com",
        "pattern": r"/fiche/",
    },
    {
        "id": "villereal",
        "url": "https://www.immobilier-villereal.com/fr/nos-biens-tous",
        "base": "https://www.immobilier-villereal.com",
        "pattern": r"/property/",
    },
    {
        "id": "quercygascogne",
        "url": "https://www.quercygascogne.fr/ventes/1",
        "base": "https://www.quercygascogne.fr",
        "pattern": r"/[0-9]+-",
        "card_selector": "article.property",
    },
    {
        "id": "xavier",
        "url": "https://xavierimmobilier.fr/gb/15-properties-up-to-350000",
        "base": "https://xavierimmobilier.fr",
        "pattern": r"/[0-9]+-",
    },
    {
        "id": "wheeler",
        "url": "https://wheeler-property.com/for-sale/",
        "base": "https://wheeler-property.com",
        "pattern": r"/properties/",
        "card_selector": "div.e-con-inner",
    },
    {
        "id": "mouly",
        "url": "https://www.mouly-immobilier.com/recherche/",
        "base": "https://www.mouly-immobilier.com",
        "pattern": r"/[0-9]+-",
        "card_selector": "article.property-listing-v2__container",
    },
    {
        "id": "soleil47",
        "url": "https://www.soleil-immobilier-47.com/vente/1",
        "base": "https://www.soleil-immobilier-47.com",
        "pattern": r"/[0-9]+-",
    },
    {
        "id": "marin",
        "url": "https://www.immobilier-marin.com/vente/1",
        "base": "https://www.immobilier-marin.com",
        "pattern": r"/[0-9]+-",
    },
    {
        "id": "factor",
        "url": "https://www.factorimmo.com/nouveautes/1",
        "base": "https://www.factorimmo.com",
        "pattern": r"/[0-9]+-",
    },
    {
        "id": "pousset",
        "url": "https://www.immobilier-pousset.fr/vente/1",
        "base": "https://www.immobilier-pousset.fr",
        "pattern": r"/vente/",
    },
    {
        "id": "arobase",
        "url": "https://www.arobaseimmobilier.fr/vente/1",
        "base": "https://www.arobaseimmobilier.fr",
        "pattern": r"/[0-9]+-",
    },
    {
        "id": "immo46",
        "url": "https://www.immo46.com/fr/a-vendre",
        "base": "https://www.immo46.com",
        "pattern": r",P[0-9]",
    },
    {
        "id": "pleinsud",
        "url": "https://www.pleinsudimmo.fr/nos-biens-immobiliers",
        "base": "https://www.pleinsudimmo.fr",
        "pattern": r"\.html$",
    },
    {
        "id": "signature_agenaise",
        "url": "https://www.la-signature-agenaise.fr/fr/vente?orderBy=2",
        "base": "https://www.la-signature-agenaise.fr",
        "pattern": r"/fr/vente/",
    },
    {
        "id": "maisondelimmobilier",
        "url": "https://www.maisondelimmobilier.com/catalog/advanced_search_result.php?C_28=Vente",
        "base": "https://www.maisondelimmobilier.com",
        "pattern": r"fiches",
    },
    {
        "id": "guy_hoquet",
        "url": "https://www.guy-hoquet.com/biens/result#1&f20=46_c2,47_c2,82_c2",
        "base": "https://www.guy-hoquet.com",
        "pattern": r"/bien/",
    },
    {
        "id": "eleonor",
        "url": "https://www.agence-eleonor.fr/fr/vente",
        "base": "https://www.agence-eleonor.fr",
        "pattern": r"/fr/propriete/",
    },
    {
        "id": "ledil",
        "url": "https://ledil.immo/recherche/tous-types/47+46+82?",
        "base": "https://ledil.immo",
        "pattern": r"/bien/",
    },
    {
        "id": "charles_loftie",
        "url": "https://charles-loftie-immo.com/fr/recherche",
        "base": "https://charles-loftie-immo.com",
        "pattern": r"/fr/selection",
    },
    {
        "id": "prada_prestige",
        "url": "https://prada-prestige-immo.fr/notre-selection",
        "base": "https://prada-prestige-immo.fr",
        "pattern": r"detail",
    },
    {
        "id": "orpi",
        "url": "https://www.orpi.com/recherche/buy?sort=date-down",
        "base": "https://www.orpi.com",
        "pattern": r"annonce-vente",
    },
]

# ---------------------------------------------------------
# URL FIX
# ---------------------------------------------------------
def fix_url(url, base):
    if not url:
        return None
    url = url.strip()
    if url.startswith("//"):
        return "https:" + url
    if not url.startswith("http"):
        return base.rstrip("/") + "/" + url.lstrip("/")
    return url


# ---------------------------------------------------------
# PRICE EXTRACTION (verbeterd)
# ---------------------------------------------------------
def extract_price(text):
    if not text:
        return "N/A"
    # pakt alle stukken met "€"
    matches = re.findall(r"([\d\.\,\s ]+)\s*€", text)
    if not matches:
        return "N/A"

    raw = matches[-1]  # laatste prijsfragment
    # Neem alleen de laatste ~12 chars om rare prefixes te strippen (2650115553 335 000 € -> 3 335 000)
    raw = raw[-12:]
    # Hou alleen cijfers, spaties, punt, komma, smalle spatie
    raw = re.sub(r"[^\d\.\,\s ]", "", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    if not raw:
        return "N/A"
    return raw + " €"


# ---------------------------------------------------------
# IMAGE FILTERING
# ---------------------------------------------------------
def is_bad_image(src):
    src = src.lower()
    bad = ["logo", "icon", "svg", "placeholder", "sprite", "loader", "blank"]
    return any(b in src for b in bad)


def extract_image(card, base):
    # data-src
    img = card.find("img", attrs={"data-src": True})
    if img and img.get("data-src") and not is_bad_image(img["data-src"]):
        return fix_url(img["data-src"], base)

    # src
    img = card.find("img", src=True)
    if img and img.get("src") and not is_bad_image(img["src"]):
        return fix_url(img["src"], base)

    # srcset
    img = card.find("img", srcset=True)
    if img and img.get("srcset"):
        first = img["srcset"].split(",")[0].split()[0]
        if not is_bad_image(first):
            return fix_url(first, base)

    # background-image
    for d in card.find_all("div", style=True):
        m = re.search(r'url\((.*?)\)', d.get("style", ""))
        if m:
            src = m.group(1).strip('"\'')
            if not is_bad_image(src):
                return fix_url(src, base)

    return None


# ---------------------------------------------------------
# GENERIC TITLE
# ---------------------------------------------------------
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


# ---------------------------------------------------------
# LISTING VALIDATION
# ---------------------------------------------------------
def looks_like_listing(text):
    return bool(re.search(r"[\d\.\,\s ]+€", text))


# ---------------------------------------------------------
# SITE-SPECIFIEKE PARSERS
# ---------------------------------------------------------

def parse_lot_immoco(card, base):
    a = card.select_one("h1 a")
    url = fix_url(a["href"], base) if a else None
    titel = a.get_text(strip=True) if a else "Onbekend"

    # soms is itemprop=price netjes, maar we normaliseren toch via extract_price
    price_el = card
    prijs = extract_price(price_el.get_text(" ", strip=True))

    img = card.select_one("div.panel-heading img")
    foto = fix_url(img["src"], base) if img else extract_image(card, base) or "N/A"

    return {
        "Titel": titel,
        "Prijs": prijs,
        "Foto": foto,
        "URL": url,
    }


def parse_quercygascogne(card, base):
    a = card.select_one("a.property__link")
    url = fix_url(a["href"], base) if a else None

    title_el = card.select_one(".property__title h2 span")
    titel = title_el.get_text(strip=True) if title_el else "Onbekend"

    prijs = extract_price(card.get_text(" ", strip=True))

    img = card.select_one(".property__visual img")
    foto = fix_url(img.get("data-src") or img.get("src"), base) if img else extract_image(card, base) or "N/A"

    return {
        "Titel": titel,
        "Prijs": prijs,
        "Foto": foto,
        "URL": url,
    }


def parse_mouly(card, base):
    a = card.select_one("a.item__title")
    url = fix_url(a["href"], base) if a else None

    title_el = card.select_one(".title__content-2")
    titel = title_el.get_text(strip=True) if title_el else "Onbekend"

    prijs = extract_price(card.get_text(" ", strip=True))

    img = card.select_one(".decorate__img")
    foto = fix_url(img["src"], base) if img else extract_image(card, base) or "N/A"

    return {
        "Titel": titel,
        "Prijs": prijs,
        "Foto": foto,
        "URL": url,
    }


def parse_wheeler(card, base):
    a = card.select_one("a.jet-listing-dynamic-link__link")
    url = fix_url(a["href"], base) if a else None

    title_el = card.select_one(".wmc-listing-title .jet-listing-dynamic-link__label")
    titel = title_el.get_text(strip=True) if title_el else "Onbekend"

    prijs = extract_price(card.get_text(" ", strip=True))

    img = card.select_one("img")
    foto = fix_url(img["src"], base) if img else extract_image(card, base) or "N/A"

    return {
        "Titel": titel,
        "Prijs": prijs,
        "Foto": foto,
        "URL": url,
    }


def parse_eleonor(card, base):
    # kaart is de <a class="container"> of een parent
    a = card if card.name == "a" else card.find("a", href=True)
    url = fix_url(a["href"], base) if a else None

    img = card.select_one("img.propertiesPicture")
    foto = fix_url(img["src"], base) if img else extract_image(card, base) or "N/A"

    title_el = card.select_one(".title")
    subtitle_el = card.select_one(".subtitle")

    titel = ""
    if title_el:
        titel += title_el.get_text(" ", strip=True)
    if subtitle_el:
        if titel:
            titel += " – "
        titel += subtitle_el.get_text(" ", strip=True)
    titel = titel.strip() if titel else "Onbekend"

    price_el = card.select_one(".price")
    prijs = extract_price(price_el.get_text(" ", strip=True) if price_el else card.get_text(" ", strip=True))

    # Plaats uit title (bijv. "Maison, Puy-l'Évêque")
    plaats = "Onbekend"
    if title_el:
        t = title_el.get_text(" ", strip=True)
        parts = [p.strip() for p in t.split(",") if p.strip()]
        if len(parts) > 1:
            plaats = parts[-1]

    return {
        "Titel": titel,
        "Prijs": prijs,
        "Foto": foto,
        "URL": url,
        "Plaats": plaats,
    }


SITE_PARSERS = {
    "lot_immoco": parse_lot_immoco,
    "quercygascogne": parse_quercygascogne,
    "mouly": parse_mouly,
    "wheeler": parse_wheeler,
    "eleonor": parse_eleonor,
}


# ---------------------------------------------------------
# CARD DETECTION
# ---------------------------------------------------------
def find_card_for_anchor(soup, a, config):
    sel = config.get("card_selector")
    if sel:
        for card in soup.select(sel):
            if card.find("a", href=a.get("href")):
                return card

    # fallback: climb
    node = a
    for _ in range(8):
        if not node.parent:
            break
        node = node.parent
        if node.name in ("article", "li", "section", "div"):
            return node
    return a.parent


# ---------------------------------------------------------
# NORMALISATIE VAN EEN LISTING
# ---------------------------------------------------------
def normalize_listing(site_id, href, card, base, a):
    text = card.get_text(" ", strip=True)

    parser = SITE_PARSERS.get(site_id)
    if parser:
        parsed = parser(card, base)
    else:
        parsed = {
            "Titel": extract_title(card, a),
            "Prijs": extract_price(text),
            "Foto": extract_image(card, base) or "N/A",
            "URL": href,
        }

    # Defaults
    listing = {
        "Bron": site_id,
        "Titel": parsed.get("Titel", "Onbekend"),
        "Plaats": parsed.get("Plaats", "Onbekend"),
        "Nieuw?": parsed.get("Nieuw?", "Nee"),
        "Prijs": parsed.get("Prijs", "N/A"),
        "m2 Binnen": parsed.get("m2 Binnen", "N/A"),
        "m2 Buiten": parsed.get("m2 Buiten", "N/A"),
        "Slaapkamers": parsed.get("Slaapkamers", "N/A"),
        "URL": parsed.get("URL", href),
        "Foto": parsed.get("Foto", extract_image(card, base) or "N/A"),
    }

    return listing


# ---------------------------------------------------------
# SCRAPE LIST PAGE
# ---------------------------------------------------------
async def scrape_list_page(browser, config):
    site_id = config["id"]
    print(f"--- Start: {site_id} ---")

    context = await browser.new_context(
        viewport={"width": 1400, "height": 900},
        user_agent="Mozilla/5.0",
    )
    page = await context.new_page()

    try:
        await page.goto(config["url"], wait_until="domcontentloaded", timeout=60000)
    except PlaywrightTimeoutError:
        print(f"  {site_id}: timeout bij goto, overslaan.")
        await context.close()
        return []

    # simpele scroll om lazy‑load te triggeren
    for _ in range(4):
        await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        await asyncio.sleep(1)

    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")

    pattern = re.compile(config["pattern"], re.I)

    anchors = []
    for a in soup.find_all("a", href=True):
        if pattern.search(a["href"]):
            anchors.append((a, fix_url(a["href"], config["base"])))

    seen = set()
    unique = []
    for a, href in anchors:
        if href not in seen:
            seen.add(href)
            unique.append((a, href))

    listings = []
    for a, href in unique:
        card = find_card_for_anchor(soup, a, config)
        if not card:
            continue

        text = card.get_text(" ", strip=True)
        if not looks_like_listing(text):
            continue

        listing = normalize_listing(site_id, href, card, config["base"], a)
        listings.append(listing)

        if len(listings) >= MAX_LISTINGS_PER_SITE:
            break

    print(f"  {site_id}: {len(listings)} listings")
    await context.close()
    return listings


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        sem = asyncio.Semaphore(MAX_CONCURRENT_SITES)

        async def run(conf):
            async with sem:
                return await scrape_list_page(browser, conf)

        results = await asyncio.gather(*[run(c) for c in SCRAPER_CONFIG])
        flat = [x for sub in results for x in sub]

        with open("data.json", "w", encoding="utf-8") as f:
            json.dump(flat, f, ensure_ascii=False, indent=2)

        print(f"\n✅ Klaar! {len(flat)} panden gescraped.")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
