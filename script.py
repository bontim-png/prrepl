import asyncio
import json
import re
from urllib.parse import urlparse
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

MAX_LISTINGS_PER_SITE = 8
MAX_CONCURRENT_SITES = 3  # mag wat hoger, we doen alleen lijstpagina's

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
        # Elementor listing: we pakken de hele e-con-inner als card
        "card_selector": "div.e-con-inner",
    },
    {
        "id": "mouly",
        "url": "https://www.mouly-immobilier.com/recherche/",
        "base": "https://www.mouly-immobilier.com",
        "pattern": r"/[0-9]+-",
        # Mouly listing: article.property-listing-v2__container
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
# FIND CARD FOR ANCHOR
# ------------------------------
def find_card_for_anchor(soup, a, config):
    # 1) site-specifieke selector
    sel = config.get("card_selector")
    if sel:
        # neem de dichtstbijzijnde ancestor die aan de selector voldoet
        parent = a
        for _ in range(12):
            if not parent:
                break
            if getattr(parent, "select_one", None):
                # check of deze parent zelf matcht
                if parent.name and parent.get("class"):
                    # simpele check via CSS selector op het hele document
                    # maar we willen alleen deze parent testen
                    # daarom: vergelijk classes handmatig
                    # (we gaan ervan uit dat sel een simpele class selector is)
                    pass
            parent = parent.parent

        # eenvoudiger: direct via soup.find_all en kijken of a erin zit
        for card in soup.select(sel):
            if card.find("a", href=a.get("href")):
                return card

    # 2) generiek: omhoog klimmen
    card = a
    for _ in range(6):
        if not card.parent:
            break
        card = card.parent
        if card.name in ("article", "li", "div", "section"):
            break
    return card


# ------------------------------
# SCRAPE LIST PAGE
# ------------------------------
async def scrape_list_page(browser, config):
    print(f"--- Start: {config['id']} ---")

    context = await browser.new_context(
        viewport={"width": 1280, "height": 800},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
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
        card = find_card_for_anchor(soup, a, config)
        if not card:
            continue

        card_text = card.get_text(" ", strip=True)

        if not looks_like_listing(card_text):
            continue

        prijs = extract_price(card_text)
        foto = extract_image(card, config["base"]) or "N/A"
        titel = extract_title(card, a)

        listings.append(
            {
                "Bron": config["id"],
                "Titel": titel,
                "Prijs": prijs,
                "Foto": foto,
                "URL": href,
            }
        )

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
            args=["--no-sandbox", "--disable-dev-shm-usage"],
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
