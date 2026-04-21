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
    {"id": "lot_immoco",          "url": "https://www.lot-immoco.net/a-vendre/1",                                                  "base": "https://www.lot-immoco.net",                        "pattern": r"\.html$"},
    {"id": "pouget",              "url": "https://www.agencespouget.com/vente/1",                                                  "base": "https://www.agencespouget.com",                     "pattern": r"/vente/"},
    {"id": "human",               "url": "https://www.human-immobilier.fr/achat-maison-tarn-et-garonne?og=0&sort=date-desc",       "base": "https://www.human-immobilier.fr",                   "pattern": r"/annonce-"},
    {"id": "letuc",               "url": "https://www.letuc.com/recherche/",                                                       "base": "https://www.letuc.com",                             "pattern": r"/t[0-9]+/"},
    {"id": "nestenn",             "url": "https://immobilier-villeneuve-sur-lot.nestenn.com/achat-immobilier",                     "base": "https://immobilier-villeneuve-sur-lot.nestenn.com",  "pattern": r"ref-"},
    {"id": "century21",           "url": "https://www.century21-bg-villeneuve.com/annonces/achat/",                                "base": "https://www.century21-bg-villeneuve.com",            "pattern": r"/detail/"},
    {"id": "lot_et_garonne",      "url": "https://www.lot-et-garonne-immobilier.com/bien-a-acheter.html",                         "base": "https://www.lot-et-garonne-immobilier.com",          "pattern": r"\.html$"},
    {"id": "valadie",             "url": "https://valadie-immobilier.com/fr/biens/a_vendre/1",                                    "base": "https://valadie-immobilier.com",                    "pattern": r"/fiche/"},
    {"id": "villereal",           "url": "https://www.immobilier-villereal.com/fr/nos-biens-tous",                                 "base": "https://www.immobilier-villereal.com",               "pattern": r"/property/"},
    {"id": "quercygascogne",      "url": "https://www.quercygascogne.fr/ventes/1",                                                 "base": "https://www.quercygascogne.fr",                     "pattern": r"/[0-9]+-"},
    {"id": "xavier",              "url": "https://xavierimmobilier.fr/gb/15-properties-up-to-350000",                             "base": "https://xavierimmobilier.fr",                       "pattern": r"/[0-9]+-"},
    {"id": "wheeler",             "url": "https://wheeler-property.com/for-sale/",                                                 "base": "https://wheeler-property.com",                      "pattern": r"/properties/"},
    {"id": "mouly",               "url": "https://www.mouly-immobilier.com/recherche/",                                            "base": "https://www.mouly-immobilier.com",                  "pattern": r"/[0-9]+-"},
    {"id": "soleil47",            "url": "https://www.soleil-immobilier-47.com/vente/1",                                          "base": "https://www.soleil-immobilier-47.com",              "pattern": r"/[0-9]+-"},
    {"id": "marin",               "url": "https://www.immobilier-marin.com/vente/1",                                              "base": "https://www.immobilier-marin.com",                  "pattern": r"/[0-9]+-"},
    {"id": "factor",              "url": "https://www.factorimmo.com/nouveautes/1",                                                "base": "https://www.factorimmo.com",                        "pattern": r"/[0-9]+-"},
    {"id": "pousset",             "url": "https://www.immobilier-pousset.fr/vente/1",                                             "base": "https://www.immobilier-pousset.fr",                 "pattern": r"/vente/"},
    {"id": "arobase",             "url": "https://www.arobaseimmobilier.fr/vente/1",                                              "base": "https://www.arobaseimmobilier.fr",                  "pattern": r"/[0-9]+-"},
    {"id": "immo46",              "url": "https://www.immo46.com/fr/a-vendre",                                                    "base": "https://www.immo46.com",                            "pattern": r",P[0-9]"},
    {"id": "pleinsud",            "url": "https://www.pleinsudimmo.fr/nos-biens-immobiliers",                                     "base": "https://www.pleinsudimmo.fr",                       "pattern": r"\.html$"},
    {"id": "signature_agenaise",  "url": "https://www.la-signature-agenaise.fr/fr/vente?orderBy=2",                              "base": "https://www.la-signature-agenaise.fr",              "pattern": r"/fr/vente/"},
    {"id": "maisondelimmobilier", "url": "https://www.maisondelimmobilier.com/catalog/advanced_search_result.php?C_28=Vente",     "base": "https://www.maisondelimmobilier.com",               "pattern": r"fiches"},
    {"id": "guy_hoquet",          "url": "https://www.guy-hoquet.com/biens/result#1&f20=46_c2,47_c2,82_c2",                      "base": "https://www.guy-hoquet.com",                        "pattern": r"/bien/"},
    {"id": "eleonor",             "url": "https://www.agence-eleonor.fr/fr/vente",                                                "base": "https://www.agence-eleonor.fr",                     "pattern": r"/fr/vente/"},
    {"id": "ledil",               "url": "https://ledil.immo/recherche/tous-types/47+46+82?",                                     "base": "https://ledil.immo",                                "pattern": r"/bien/"},
    {"id": "charles_loftie",      "url": "https://charles-loftie-immo.com/fr/recherche",                                         "base": "https://charles-loftie-immo.com",                   "pattern": r"/fr/selection"},
    {"id": "prada_prestige",      "url": "https://prada-prestige-immo.fr/notre-selection",                                        "base": "https://prada-prestige-immo.fr",                    "pattern": r"detail"},
    {"id": "orpi",                "url": "https://www.orpi.com/recherche/buy?sort=date-down",                                     "base": "https://www.orpi.com",                              "pattern": r"annonce-vente"},
]

def fix_url(url, base_url):
    if not url:
        return None
    if not url.startswith("http"):
        url = base_url.rstrip("/") + "/" + url.lstrip("/")
    parsed = urlparse(url)
    if not parsed.path:
        return url
    return url

def extract_price(text: str) -> str:
    if not text:
        return "N/A"
    m = re.search(r"(\d[\d\s\.]{3,})\s*€", text)
    if not m:
        m = re.search(r"€\s*(\d[\d\s\.]{3,})", text)
    if not m:
        return "N/A"
    raw = m.group(0)
    return raw.strip()

def extract_image_from_card(card, base_url):
    # 1. data-src
    img = card.find("img", attrs={"data-src": True})
    if img and img.get("data-src"):
        return fix_url(img["data-src"], base_url)
    # 2. src
    img = card.find("img", src=True)
    if img and img.get("src"):
        return fix_url(img["src"], base_url)
    # 3. srcset (pak eerste)
    img = card.find("img", srcset=True)
    if img and img.get("srcset"):
        first = img["srcset"].split(",")[0].split()[0]
        return fix_url(first, base_url)
    return None

async def scrape_list_page(browser, config):
    print(f"--- Start: {config['id']} ---")

    context = await browser.new_context(
        viewport={"width": 1280, "height": 800},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    page = await context.new_page()
    page.set_default_timeout(25000)
    page.set_default_navigation_timeout(25000)

    results = []

    try:
        await page.goto(config["url"], wait_until="domcontentloaded", timeout=25000)

        # simpele lazy-load scroll
        for _ in range(4):
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            await asyncio.sleep(1.0)

        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")

        pattern = re.compile(config["pattern"], re.I)

        # alle anchors met matching href in DOM-volgorde
        anchors = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if pattern.search(href):
                full = fix_url(href, config["base"])
                anchors.append((a, full))

        # dedupe op URL, behoud volgorde
        seen = set()
        unique = []
        for a, href in anchors:
            if href not in seen:
                seen.add(href)
                unique.append((a, href))

        unique = unique[:MAX_LISTINGS_PER_SITE]
        print(f"  {config['id']}: {len(unique)} listings gevonden")

        for a, href in unique:
            # probeer een "card" te vinden rond de link
            card = a
            for _ in range(4):
                if card.parent is None:
                    break
                card = card.parent
                if card.name in ("article", "li", "div", "section"):
                    break

            card_text = card.get_text(" ", strip=True) if card else a.get_text(" ", strip=True)
            prijs = extract_price(card_text)

            foto = extract_image_from_card(card, config["base"]) if card else None
            if not foto:
                # fallback: zoek img direct onder de link
                img = a.find("img")
                if img and img.get("src"):
                    foto = fix_url(img["src"], config["base"])

            titel = a.get_text(" ", strip=True)
            if not titel and card:
                titel = card_text[:120]

            results.append({
                "Bron": config["id"],
                "Titel": titel or "Onbekend",
                "Prijs": prijs,
                "Foto": foto or "N/A",
                "URL": href,
            })

    except Exception as e:
        print(f"  ! Fout bij {config['id']}: {e}")

    await context.close()
    return results

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
