import asyncio
import re
import json
from urllib.parse import urlparse
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
# ─────────────────────────────────────────
# CONFIGURATIE
# ─────────────────────────────────────────
SCRAPER_CONFIG = [
    {"id": "ladresse_tournon",    "url": "https://www.ladresse.com/agence/l-adresse-tournon-d-agenais/266/acheter?sort=date-desc", "base": "https://www.ladresse.com",                          "pattern": r"/achat/"},
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

MAX_LISTINGS_PER_SITE = 10
BATCH_SIZE = 3


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def fix_url(url: str) -> str:
    """Zorgt ervoor dat de URL eindigt op een slash als dat nodig is."""
    parsed = urlparse(url)
    # Alleen toevoegen als er geen extensie is (zoals .html of .php)
    if not parsed.path.endswith('/') and not re.search(r'\.[a-z0-9]{2,4}$', parsed.path, re.I):
        return url + '/'
    return url

def make_absolute(src: str, base_url: str) -> str:
    if not src: return "N/A"
    if src.startswith("http"): return src
    if src.startswith("//"): return "https:" + src
    if src.startswith("/"):
        parsed = urlparse(base_url)
        return f"{parsed.scheme}://{parsed.netloc}{src}"
    return base_url.rstrip("/") + "/" + src

def extract_main_image(soup: BeautifulSoup, page_url: str) -> str:
    # 1. Check og:image
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        return make_absolute(og["content"], page_url)

    # 2. Uitgebreide lijst met selectors voor foto's (inclusief Franse thema's)
    for selector in [
        "div.gallery", "div.slider", "div.carousel", "div.swiper-slide",
        "div.photo", "div.visuel", "div.main-photo", "div.bien-photo",
        "img.main-img", "img.property-image", "div.thumb", "figure img"
    ]:
        img = soup.select_one(selector)
        if img:
            # Check src, dan data-src (voor lazy loading)
            src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
            if src and re.search(r"\.(jpg|jpeg|png|webp)", src, re.I):
                return make_absolute(src, page_url)

    return "N/A"

# ─────────────────────────────────────────
# DETAIL PAGINA
# ─────────────────────────────────────────

async def scrape_details(context, url: str, site_id: str) -> dict | None:
    url = fix_url(url) # URL Fix toegepast
    try:
        page = await context.new_page()
        # Gebruik networkidle om te zorgen dat scripts (en prijzen) geladen zijn
        await page.goto(url, wait_until="networkidle", timeout=30000)
        
        # SCROLL ACTIE: Dit triggert lazy loading van foto's en prijzen
        await page.evaluate("window.scrollTo(0, 500)")
        await asyncio.sleep(1.5) # Wacht even op de render

        content = await page.content()
        await page.close()

        soup = BeautifulSoup(content, "html.parser")
        # Strip overtollige spaties voor betere regex match
        text = " ".join(soup.get_text().split())

        # Verbeterde Prijs Regex: pakt "285 000 €", "285.000€" en "285000 €"
        prijs = re.search(r"(\d[\d\s.,]*)\s?€", text)
        
        binnen = re.search(r"(\d+)\s?m²?\s?habitable|surface\s?[:\-]\s?(\d+)\s?m²?|(\d+)\s?m²?\s?de\s?surface", text, re.I)
        buiten = re.search(r"terrain\s?[:\-]\s?(\d+[\s.]?\d*)\s?m²?|(\d+[\s.]?\d*)\s?m²?\s?de\s?terrain", text, re.I)
        kamers = re.search(r"(\d+)\s?chambre", text, re.I)
        nieuw  = bool(re.search(r"nouveau|nouveauté|new|récent|exclusivité", text, re.I))

        # Titel/Plaatsbepaling
        og_title = soup.find("meta", property="og:title")
        h1 = soup.find("h1")
        plaats = "Onbekend"
        if og_title and og_title.get("content"):
            plaats = og_title["content"][:80]
        elif h1:
            plaats = h1.get_text(strip=True)[:80]

        foto = extract_main_image(soup, url)

        return {
            "Bron": site_id,
            "Plaats": plaats,
            "Nieuw?": "JA" if nieuw else "Nee",
            "Prijs": prijs.group(0).replace(" ", "").strip() if prijs else "N/A",
            "m2 Binnen": next((g for g in binnen.groups() if g), "N/A") if binnen else "N/A",
            "m2 Buiten": next((g for g in buiten.groups() if g), "N/A") if buiten else "N/A",
            "Slaapkamers": kamers.group(1) if kamers else "N/A",
            "Foto": foto,
            "URL": url,
        }

    except Exception as e:
        print(f"  ✗ Detail-fout ({site_id} | {url}): {e}")
        return None


# ─────────────────────────────────────────
# LIJSTPAGINA
# ─────────────────────────────────────────

async def scrape_site(browser, config: dict) -> list:
    site_id = config["id"]
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    )
    page = await context.new_page()
    listings = []

    try:
        print(f"▶ {site_id}...")
        await page.goto(config["url"], wait_until="networkidle", timeout=60000)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
        await asyncio.sleep(2)

        content = await page.content()
        soup = BeautifulSoup(content, "html.parser")

        seen = set()
        link_urls = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not re.search(config["pattern"], href, re.I):
                continue
            full = href if href.startswith("http") else config["base"] + href
            full = full.rstrip("/")
            if full not in seen:
                seen.add(full)
                link_urls.append(full)

        print(f"  → {len(link_urls)} links, bezoek max {MAX_LISTINGS_PER_SITE}...")

        for url in link_urls[:MAX_LISTINGS_PER_SITE]:
            detail = await scrape_details(context, url, site_id)
            if detail:
                listings.append(detail)

        print(f"  ✓ {len(listings)} listings opgeslagen")

    except Exception as e:
        print(f"  ✗ Fout bij {site_id}: {e}")

    finally:
        await page.close()
        await context.close()

    return listings


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        all_data = []

        for i in range(0, len(SCRAPER_CONFIG), BATCH_SIZE):
            batch = SCRAPER_CONFIG[i:i + BATCH_SIZE]
            tasks = [scrape_site(browser, cfg) for cfg in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, list):
                    all_data.extend(res)
                else:
                    print(f"  ⚠ Batch-fout: {res}")

        await browser.close()

        with open("data.json", "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)

        foto_count = sum(1 for d in all_data if d.get("Foto") != "N/A")
        print(f"\n✅ Klaar! {len(all_data)} listings | {foto_count} foto's → data.json")


if __name__ == "__main__":
    asyncio.run(main())
