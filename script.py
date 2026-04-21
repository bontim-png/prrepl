import asyncio
import json
import re
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

MAX_LISTINGS_PER_SITE = 6
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
        "pattern": r"/fr/vente/",
        "card_selector": "a.container",
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
# PRICE EXTRACTION
# ---------------------------------------------------------
def extract_price(text):
    if not text:
        return "N/A"
    # haal alle prijsfragmenten met € eruit
    matches = re.findall(r"([\d\.\,\s ]+)\s*€", text)
    if not matches:
        return "N/A"
    raw = matches[-1]
    # verwijder rare leading blokken zoals "2650115553 335 000"
    # pak laatste blok met minimaal 3 cijfers
    parts = re.split(r"\s+", raw.replace(" ", " ").strip())
    candidate = None
    for p in reversed(parts):
        digits = re.sub(r"[^\d]", "", p)
        if len(digits) >= 3:
            candidate = p
            break
    if not candidate:
        candidate = raw.replace(" ", " ").strip()
    # vervang komma door spatie als duizendtallen, punt door spatie
    candidate = candidate.replace(" ", " ")
    candidate = candidate.replace(".", " ")
    candidate = candidate.replace(",", " ")
    candidate = re.sub(r"\s+", " ", candidate).strip()
    return candidate + " €"


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
# GENERIC TITLE (alleen intern, voor plaats-detectie)
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
        return txt[:200]

    return ""


# ---------------------------------------------------------
# LISTING VALIDATION
# ---------------------------------------------------------
def looks_like_listing(text):
    return bool(re.search(r"[\d\.\,\s ]+€", text))


# ---------------------------------------------------------
# DETECT "NIEUW?"
# ---------------------------------------------------------
def detect_nieuw(text):
    if not text:
        return "Nee"
    t = text.lower()
    keywords = [
        "nouveau", "nouveauté", "exclusif", "exclusivité",
        "new", "new listing", "just listed", "recently added",
        "nieuw", "net binnen", "pas toegevoegd",
        "juste arrivé", "fresh on the market",
    ]
    return "Ja" if any(k in t for k in keywords) else "Nee"


# ---------------------------------------------------------
# DETECT PLAATS
# ---------------------------------------------------------
def detect_place_from_title_or_text(text):
    if not text:
        return "Onbekend"

    # simpele heuristiek: zoek iets als "Maison, Puy-l'Évêque"
    m = re.search(r",\s*([^,–\-|]+)$", text.strip())
    if m:
        plaats = m.group(1).strip()
        if 2 <= len(plaats) <= 60:
            return plaats

    # zoek hoofdletter-woorden met streepjes (Montaigu-de-Quercy)
    m = re.search(r"\b([A-ZÉÈÎÏÂÀÇ][\w\'\-éèêëàâîïôöùûüç]+(?:-[A-ZÉÈÎÏÂÀÇ][\w\'\-éèêëàâîïôöùûüç]+)*)", text)
    if m:
        return m.group(1).strip()

    return "Onbekend"


def detect_place_from_url(url):
    if not url:
        return None
    # pak laatste segment dat geen id is
    path = url.split("?")[0]
    parts = [p for p in path.split("/") if p]
    if not parts:
        return None
    # neem laatste niet-numerieke slug
    for p in reversed(parts):
        if not re.fullmatch(r"\d+", p):
            slug = p
            break
    else:
        return None
    slug = slug.replace("-", " ")
    slug = re.sub(r"\d+", "", slug).strip()
    if not slug:
        return None
    # capitalise eerste letter van elk woord
    return " ".join(w.capitalize() for w in slug.split())


# ---------------------------------------------------------
# DETECT M2 / SLAAPKAMERS
# ---------------------------------------------------------
def detect_m2_binnen(text):
    if not text:
        return "N/A"
    # zoek iets als "90 m²", "105 m2"
    m = re.search(r"(\d{2,4})\s*(m²|m2|sqm)", text, re.I)
    if m:
        return m.group(1)
    # fallback: "surface habitable 120 m²"
    m = re.search(r"surface.*?(\d{2,4})\s*(m²|m2|sqm)", text, re.I)
    if m:
        return m.group(1)
    return "N/A"


def detect_m2_buiten(text):
    if not text:
        return "N/A"
    # zoek naar terrain / land / plot
    m = re.search(r"(terrain|land|plot|jardin|parcelle)[^0-9]{0,20}(\d{3,6})\s*(m²|m2)", text, re.I)
    if m:
        return m.group(2)
    return "N/A"


def detect_slaapkamers(text):
    if not text:
        return "N/A"
    # 3 chambres, 4 ch, 5 bedrooms, 2 bed
    m = re.search(r"(\d+)\s*(chambres|chambre|ch\b)", text, re.I)
    if m:
        return m.group(1)
    m = re.search(r"(\d+)\s*(bedrooms|bedroom|bed\b)", text, re.I)
    if m:
        return m.group(1)
    return "N/A"


# ---------------------------------------------------------
# SITE-SPECIFIEKE PARSERS (geven ruwe data terug)
# ---------------------------------------------------------

def parse_lot_immoco(card, base):
    a = card.select_one("h1 a")
    url = fix_url(a["href"], base) if a else None

    titel = a.get_text(strip=True) if a else ""
    price_el = card.select_one("span[itemprop='price']")
    prijs_text = price_el.get("content") + " €" if price_el else card.get_text(" ", strip=True)

    img = card.select_one("div.panel-heading img")
    foto = fix_url(img["src"], base) if img else extract_image(card, base)

    text = card.get_text(" ", strip=True)
    return {
        "url": url,
        "title": titel,
        "text": text,
        "price_text": prijs_text,
        "photo": foto,
    }


def parse_quercygascogne(card, base):
    a = card.select_one("a.property__link")
    url = fix_url(a["href"], base) if a else None

    title_el = card.select_one(".property__title h2 span")
    titel = title_el.get_text(strip=True) if title_el else ""

    price_el = card.select_one(".property__price span")
    prijs_text = price_el.get_text(strip=True) if price_el else card.get_text(" ", strip=True)

    img = card.select_one(".property__visual img")
    foto = fix_url(img.get("data-src") or img.get("src"), base) if img else extract_image(card, base)

    text = card.get_text(" ", strip=True)
    return {
        "url": url,
        "title": titel,
        "text": text,
        "price_text": prijs_text,
        "photo": foto,
    }


def parse_mouly(card, base):
    a = card.select_one("a.item__title")
    url = fix_url(a["href"], base) if a else None

    title_el = card.select_one(".title__content-2")
    titel = title_el.get_text(strip=True) if title_el else ""

    price_el = card.select_one(".item__price .__price-value")
    prijs_text = price_el.get_text(strip=True) if price_el else card.get_text(" ", strip=True)

    img = card.select_one(".decorate__img")
    foto = fix_url(img["src"], base) if img else extract_image(card, base)

    text = card.get_text(" ", strip=True)
    return {
        "url": url,
        "title": titel,
        "text": text,
        "price_text": prijs_text,
        "photo": foto,
    }


def parse_wheeler(card, base):
    a = card.select_one("a.jet-listing-dynamic-link__link")
    url = fix_url(a["href"], base) if a else None

    title_el = card.select_one(".wmc-listing-title .jet-listing-dynamic-link__label")
    titel = title_el.get_text(strip=True) if title_el else ""

    price_el = card.select_one(".jet-listing-dynamic-field__content")
    prijs_text = price_el.get_text(" ", strip=True) if price_el else card.get_text(" ", strip=True)

    img = card.select_one("img")
    foto = fix_url(img["src"], base) if img else extract_image(card, base)

    text = card.get_text(" ", strip=True)
    return {
        "url": url,
        "title": titel,
        "text": text,
        "price_text": prijs_text,
        "photo": foto,
    }


def parse_eleonor(card, base):
    a = card if card.name == "a" else card.find("a", href=True)
    url = fix_url(a["href"], base) if a else None

    img = card.select_one("img.propertiesPicture")
    foto = fix_url(img["src"], base) if img else extract_image(card, base)

    title_el = card.select_one(".title")
    subtitle_el = card.select_one(".subtitle")

    titel = ""
    if title_el:
        titel += title_el.get_text(" ", strip=True)
    if subtitle_el:
        if titel:
            titel += " – "
        titel += subtitle_el.get_text(" ", strip=True)

    price_el = card.select_one(".price")
    prijs_text = price_el.get_text(" ", strip=True) if price_el else card.get_text(" ", strip=True)

    text = card.get_text(" ", strip=True)
    return {
        "url": url,
        "title": titel,
        "text": text,
        "price_text": prijs_text,
        "photo": foto,
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
# NORMALISATIE NAAR JOUW STRUCTUUR
# ---------------------------------------------------------
def normalize_listing(site_id, raw):
    url = raw.get("url")
    title = raw.get("title") or ""
    text = raw.get("text") or ""
    price_text = raw.get("price_text") or text
    photo = raw.get("photo")

    full_text = " ".join([title, text]).strip()

    prijs = extract_price(price_text)
    nieuw = detect_nieuw(full_text)

    plaats = detect_place_from_title_or_text(title)
    if plaats == "Onbekend":
        from_text = detect_place_from_title_or_text(text)
        if from_text != "Onbekend":
            plaats = from_text
    if plaats == "Onbekend":
        from_url = detect_place_from_url(url)
        if from_url:
            plaats = from_url

    m2_binnen = detect_m2_binnen(full_text)
    m2_buiten = detect_m2_buiten(full_text)
    slaapkamers = detect_slaapkamers(full_text)

    return {
        "Bron": site_id,
        "Plaats": plaats,
        "Nieuw?": nieuw,
        "Prijs": prijs,
        "m2 Binnen": m2_binnen,
        "m2 Buiten": m2_buiten,
        "Slaapkamers": slaapkamers,
        "URL": url or "",
        "Foto": photo or "N/A",
    }


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
        await page.goto(config["url"], wait_until="domcontentloaded", timeout=30000)
    except Exception as e:
        print(f"  {site_id}: goto failed: {e}")
        await context.close()
        return []

    # probeer rustig te scrollen, maar breek bij navigatie
    for _ in range(4):
        try:
            await page.mouse.wheel(0, 2000)
            await asyncio.sleep(1)
        except Exception:
            break

    try:
        html = await page.content()
    except Exception as e:
        print(f"  {site_id}: content failed: {e}")
        await context.close()
        return []

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
    parser = SITE_PARSERS.get(site_id)

    for a, href in unique:
        card = find_card_for_anchor(soup, a, config)
        if not card:
            continue

        text = card.get_text(" ", strip=True)
        if not looks_like_listing(text):
            continue

        if parser:
            raw = parser(card, config["base"])
        else:
            title = extract_title(card, a)
            foto = extract_image(card, config["base"]) or "N/A"
            raw = {
                "url": href,
                "title": title,
                "text": text,
                "price_text": text,
                "photo": foto,
            }

        raw["url"] = raw.get("url") or href
        normalized = normalize_listing(site_id, raw)
        listings.append(normalized)

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
