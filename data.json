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
        "url": "https://www.immobilier-marin.com/recherche/",
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
# IMAGE FILTERING
# ---------------------------------------------------------
def is_bad_image(src):
    src = src.lower()
    bad = ["logo", "icon", "svg", "placeholder", "sprite", "loader", "blank"]
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

    for d in card.find_all("div", style=True):
        m = re.search(r'url\((.*?)\)', d.get("style", ""))
        if m:
            src = m.group(1).strip('"\'')
            if not is_bad_image(src):
                return fix_url(src, base)

    return None

# ---------------------------------------------------------
# GENERIC TITLE (alleen nog nodig voor Plaats-fallback)
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

    return ""

# ---------------------------------------------------------
# LISTING VALIDATION
# ---------------------------------------------------------
def looks_like_listing(text):
    return bool(re.search(r"[\d\.\,\s ]+€", text)) or "chambre" in text.lower() or "pièce" in text.lower()

# ---------------------------------------------------------
# SITE-SPECIFIEKE PARSERS (zoals je al had)
# ---------------------------------------------------------
def parse_lot_immoco(card, base):
    a = card.select_one("h1 a")
    url = fix_url(a["href"], base) if a else None
    titel = a.get_text(strip=True) if a else ""
    price_el = card.select_one("span[itemprop='price']")
    prijs = price_el.get("content") + " €" if price_el else "N/A"
    img = card.select_one("div.panel-heading img")
    foto = fix_url(img["src"], base) if img else "N/A"
    return {"Titel": titel, "PrijsRaw": prijs, "Foto": foto, "URL": url}

def parse_quercygascogne(card, base):
    a = card.select_one("a.property__link")
    url = fix_url(a["href"], base) if a else None
    title_el = card.select_one(".property__title h2 span")
    titel = title_el.get_text(strip=True) if title_el else ""
    price_el = card.select_one(".property__price span")
    prijs = price_el.get_text(strip=True) if price_el else "N/A"
    img = card.select_one(".property__visual img")
    foto = fix_url(img.get("data-src") or img.get("src"), base) if img else "N/A"
    return {"Titel": titel, "PrijsRaw": prijs, "Foto": foto, "URL": url}

def parse_mouly(card, base):
    a = card.select_one("a.item__title")
    url = fix_url(a["href"], base) if a else None
    title_el = card.select_one(".title__content-2")
    titel = title_el.get_text(strip=True) if title_el else ""
    price_el = card.select_one(".item__price .__price-value")
    prijs = price_el.get_text(strip=True) if price_el else "N/A"
    img = card.select_one(".decorate__img")
    foto = fix_url(img["src"], base) if img else "N/A"
    return {"Titel": titel, "PrijsRaw": prijs, "Foto": foto, "URL": url}

def parse_wheeler(card, base):
    a = card.select_one("a.jet-listing-dynamic-link__link")
    url = fix_url(a["href"], base) if a else None
    title_el = card.select_one(".wmc-listing-title .jet-listing-dynamic-link__label")
    titel = title_el.get_text(strip=True) if title_el else ""
    price_el = card.select_one(".jet-listing-dynamic-field__content")
    prijs = price_el.get_text(" ", strip=True) if price_el else "N/A"
    img = card.select_one("img")
    foto = fix_url(img["src"], base) if img else "N/A"
    return {"Titel": titel, "PrijsRaw": prijs, "Foto": foto, "URL": url}

def parse_eleonor(card, base):
    a = card if card.name == "a" else card.find("a", href=True)
    url = fix_url(a["href"], base) if a else None
    img = card.select_one("img.propertiesPicture")
    foto = fix_url(img["src"], base) if img else "N/A"
    title_el = card.select_one(".title")
    subtitle_el = card.select_one(".subtitle")
    titel = ""
    if title_el:
        titel += title_el.get_text(" ", strip=True)
    if subtitle_el:
        titel += " – " + subtitle_el.get_text(" ", strip=True)
    titel = titel.strip()
    price_el = card.select_one(".price")
    prijs = price_el.get_text(" ", strip=True) if price_el else "N/A"
    return {"Titel": titel, "PrijsRaw": prijs, "Foto": foto, "URL": url}

SITE_PARSERS = {
    "lot_immoco": parse_lot_immoco,
    "quercygascogne": parse_quercygascogne,
    "mouly": parse_mouly,
    "wheeler": parse_wheeler,
    "eleonor": parse_eleonor,
}

# ---------------------------------------------------------
# PLAATS-DETECTIE
# ---------------------------------------------------------
FORBIDDEN_PLACE_WORDS = {
    "maison", "appartement", "immeuble", "terrain", "propriete", "propriété",
    "villa", "ensemble", "magnifique", "charmante", "charmant", "belle", "grand",
    "estimation", "country", "propriete", "propriété", "chateau", "château",
    "exclusif", "exclusifs", "exclusivité", "exclusivite",
    "garage", "jardin", "piscine", "dependances", "dépendances", "vue", "boisé",
    "boise", "maisonnette", "studio"
}

def normalize_word(w: str) -> str:
    w = w.strip(" ,.;:!?\t\n").lower()
    w = w.replace("’", "'")
    w = w.replace("é", "e").replace("è", "e").replace("ê", "e")
    w = w.replace("à", "a").replace("â", "a")
    w = w.replace("ô", "o").replace("ù", "u").replace("û", "u")
    return w

def is_valid_place_word(w: str) -> bool:
    if not w:
        return False
    w_norm = normalize_word(w)
    if not w_norm:
        return False
    if w_norm in FORBIDDEN_PLACE_WORDS:
        return False
    if any(c.isdigit() for c in w_norm):
        return False
    if len(w_norm) < 3:
        return False
    return True

def format_place(words):
    return " ".join(w.capitalize() for w in words)

def detect_place_from_url(url: str | None) -> str | None:
    if not url:
        return None
    parts = url.split("/")
    for p in reversed(parts):
        if "-" in p and not p.isdigit():
            raw = p.split("-")
            words = [w for w in raw if is_valid_place_word(w)]
            if words:
                return format_place(words)
    return None

def detect_place_from_title(title: str | None) -> str | None:
    if not title:
        return None
    parts = re.split(r"[,\-–|/]", title)
    for p in parts:
        words = [w for w in p.split() if is_valid_place_word(w)]
        if words:
            return format_place(words)
    return None

def detect_place_from_text(text: str | None) -> str | None:
    if not text:
        return None
    words = [w for w in text.split() if is_valid_place_word(w)]
    if words:
        return words[0].capitalize()
    return None

def detect_place(url, title, text):
    return (
        detect_place_from_url(url)
        or detect_place_from_title(title)
        or detect_place_from_text(text)
        or "Onbekend"
    )

# ---------------------------------------------------------
# PRIJS-DETECTIE
# ---------------------------------------------------------
def extract_price(text: str | None) -> str:
    if not text:
        return "N/A"
    # alle getallen gevolgd door €
    matches = re.findall(r"(\d[\d\s \.]*\d)\s*€", text)
    if not matches:
        return "N/A"
    # neem de EERSTE (niet de laatste) om "000 €" te vermijden
    price = matches[0]
    price = price.replace(" ", " ").replace(" ", "").replace(".", "")
    # herformat: 285000 -> "285 000 €"
    try:
        n = int(price)
        return f"{n:,}".replace(",", " ") + " €"
    except ValueError:
        return matches[0].strip() + " €"

# ---------------------------------------------------------
# m2 BINNEN / BUITEN
# ---------------------------------------------------------
OUTSIDE_HINTS = ["terrain", "jardin", "parcelle", "parc", "land", "plot", "boise", "boisé", "hectares", "ha"]

def extract_m2(text: str | None):
    if not text:
        return ("N/A", "N/A")
    tokens = text.split()
    inside = None
    outside = None

    for i, tok in enumerate(tokens):
        m = re.match(r"(\d+)\s*(m²|m2)", tok.replace("mÂ²", "m²"))
        if not m:
            # soms "197.00" en dan "m²" apart
            if i + 1 < len(tokens) and re.match(r"m²|m2", tokens[i + 1]):
                m = re.match(r"(\d+)", tok)
            else:
                continue
        if not m:
            continue
        val = m.group(1)
        # context: woorden ervoor
        window_start = max(0, i - 5)
        context = " ".join(tokens[window_start:i]).lower()
        is_outside = any(h in context for h in OUTSIDE_HINTS)
        if is_outside:
            if outside is None:
                outside = val
        else:
            if inside is None:
                inside = val

    return (inside or "N/A", outside or "N/A")

# ---------------------------------------------------------
# SLAAPKAMERS
# ---------------------------------------------------------
def extract_bedrooms(text: str | None) -> str:
    if not text:
        return "N/A"
    m = re.search(r"(\d+)\s*(chambres?|ch\.|chb|bedrooms?|bed)", text, re.I)
    if m:
        return m.group(1)
    return "N/A"

# ---------------------------------------------------------
# NIEUW?
# ---------------------------------------------------------
def detect_new(text: str | None) -> str:
    if not text:
        return "Nee"
    t = text.lower()
    if any(w in t for w in ["nouveaute", "nouveauté", "new", "recent", "récent"]):
        return "Ja"
    if "exclusiv" in t:
        # jij mag dit aanpassen naar wens; nu telt exclusif als "nieuw"
        return "Ja"
    return "Nee"

# ---------------------------------------------------------
# CARD DETECTION
# ---------------------------------------------------------
def find_card_for_anchor(soup, a, config):
    sel = config.get("card_selector")
    if sel:
        for card in soup.select(sel):
            if card.find("a", href=a.get("href")):
                return card

    node = a
    for _ in range(8):
        if not node.parent:
            break
        node = node.parent
        if node.name in ("article", "li", "section", "div"):
            return node
    return a.parent

# ---------------------------------------------------------
# SCRAPE LIST PAGE
# ---------------------------------------------------------
async def scrape_list_page(browser, config):
    site_id = config["id"]
    print(f"--- Start: {site_id} ---")

    context = await browser.new_context(
        viewport={"width": 1400, "height": 900},
        user_agent="Mozilla/5.0"
    )
    page = await context.new_page()

    await page.goto(config["url"], wait_until="domcontentloaded", timeout=30000)

    # veilige scroll: navigation changes → try/except
    for _ in range(4):
        try:
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            await asyncio.sleep(1)
        except Exception:
            break

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

        parser = SITE_PARSERS.get(site_id)
        if parser:
            parsed = parser(card, config["base"])
            titel = parsed.get("Titel", "") or ""
            prijs_raw = parsed.get("PrijsRaw", "") or text
            foto = parsed.get("Foto") or extract_image(card, config["base"]) or "N/A"
            url = parsed.get("URL") or href
        else:
            titel = extract_title(card, a)
            prijs_raw = text
            foto = extract_image(card, config["base"]) or "N/A"
            url = href

        prijs = extract_price(prijs_raw or text)
        m2_binnen, m2_buiten = extract_m2(text)
        slaapkamers = extract_bedrooms(text)
        nieuw = detect_new(text + " " + titel)

        plaats = detect_place(url, titel, text)

        data = {
            "Bron": site_id,
            "Plaats": plaats,
            "Nieuw?": nieuw,
            "Prijs": prijs,
            "m2 Binnen": m2_binnen,
            "m2 Buiten": m2_buiten,
            "Slaapkamers": slaapkamers,
            "URL": url,
            "Foto": foto,
        }

        listings.append(data)
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
