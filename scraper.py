import asyncio
import re
import json
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# --- CONFIGURATIE (Zelfde als voorheen) ---
SCRAPER_CONFIG = [
    {"id": "ladresse_tournon",   "url": "https://www.ladresse.com/agence/l-adresse-tournon-d-agenais/266/acheter?sort=date-desc",  "base": "https://www.ladresse.com", "pattern": r"/achat/"},
    {"id": "beauxvillages",      "url": "https://beauxvillages.com/en/latest-properties?hotsheet=1",                      "base": "https://beauxvillages.com", "pattern": r"/property/"},
    {"id": "lot_immoco",         "url": "https://www.lot-immoco.net/a-vendre/1",                                                   "base": "https://www.lot-immoco.net", "pattern": r"\.html$"},
    {"id": "pouget",             "url": "https://www.agencespouget.com/nos-biens",                                                 "base": "https://www.agencespouget.com", "pattern": r"/vente/"},
    {"id": "human",              "url": "https://www.human-immobilier.fr/achat-maison-tarn-et-garonne?og=0&sort=date-desc",        "base": "https://www.human-immobilier.fr", "pattern": r"/annonce-"},
    {"id": "letuc",              "url": "https://www.letuc.com/recherche/",                                                        "base": "https://www.letuc.com", "pattern": r"/t[0-9]+/"},
    {"id": "nestenn",            "url": "https://immobilier-villeneuve-sur-lot.nestenn.com/achat-immobilier",                      "base": "https://immobilier-villeneuve-sur-lot.nestenn.com", "pattern": r"ref-"},
    {"id": "century21",          "url": "https://www.century21-bg-villeneuve.com/annonces/achat/",                                 "base": "https://www.century21-bg-villeneuve.com", "pattern": r"/detail/"},
    {"id": "lot_et_garonne",     "url": "https://www.lot-et-garonne-immobilier.com/bien-a-acheter.html",                          "base": "https://www.lot-et-garonne-immobilier.com", "pattern": r"\.html$"},
    {"id": "valadie",            "url": "https://valadie-immobilier.com/fr/biens/a_vendre/1",                                     "base": "https://valadie-immobilier.com", "pattern": r"/fiche/"},
    {"id": "villereal",          "url": "https://www.immobilier-villereal.com/fr/nos-biens-tous",                                  "base": "https://www.immobilier-villereal.com", "pattern": r"/property/"},
    {"id": "quercygascogne",     "url": "https://www.quercygascogne.fr/ventes/1",                                                  "base": "https://www.quercygascogne.fr", "pattern": r"/[0-9]+-"},
    {"id": "xavier",             "url": "https://xavierimmobilier.fr/gb/15-properties-up-to-350000",                              "base": "https://xavierimmobilier.fr", "pattern": r"/[0-9]+-"},
    {"id": "wheeler",            "url": "https://wheeler-property.com/for-sale/",                                                  "base": "https://wheeler-property.com", "pattern": r"/properties/"},
    {"id": "mouly",              "url": "https://www.mouly-immobilier.com/recherche/",                                             "base": "https://www.mouly-immobilier.com", "pattern": r"/[0-9]+-"},
    {"id": "soleil47",           "url": "https://www.soleil-immobilier-47.com/vente/1",                                           "base": "https://www.soleil-immobilier-47.com", "pattern": r"/[0-9]+-"},
    {"id": "marin",              "url": "https://www.immobilier-marin.com/vente/1",                                               "base": "https://www.immobilier-marin.com", "pattern": r"/[0-9]+-"},
    {"id": "factor",             "url": "https://www.factorimmo.com/nouveautes/1",                                                 "base": "https://www.factorimmo.com", "pattern": r"/[0-9]+-"},
    {"id": "pousset",            "url": "https://www.immobilier-pousset.fr/vente/1",                                              "base": "https://www.immobilier-pousset.fr", "pattern": r"/vente/"},
    {"id": "arobase",            "url": "https://www.arobaseimmobilier.fr/vente/1",                                               "base": "https://www.arobaseimmobilier.fr", "pattern": r"/[0-9]+-"},
    {"id": "immo46",             "url": "https://www.immo46.com/fr/a-vendre",                                                     "base": "https://www.immo46.com", "pattern": r",P[0-9]"},
    {"id": "pleinsud",           "url": "https://www.pleinsudimmo.fr/nos-biens-immobiliers",                                      "base": "https://www.pleinsudimmo.fr", "pattern": r"\.html$"},
    {"id": "signature_agenaise", "url": "https://www.la-signature-agenaise.fr/fr/vente?orderBy=2",                               "base": "https://www.la-signature-agenaise.fr", "pattern": r"/fr/vente/"},
    {"id": "maisondelimmobilier","url": "https://www.maisondelimmobilier.com/catalog/advanced_search_result.php?C_28=Vente",      "base": "https://www.maisondelimmobilier.com", "pattern": r"fiches"},
    {"id": "guy_hoquet",         "url": "https://www.guy-hoquet.com/biens/result#1&f20=46_c2,47_c2,82_c2",                      "base": "https://www.guy-hoquet.com", "pattern": r"/bien/"},
    {"id": "eleonor",            "url": "https://www.agence-eleonor.fr/fr/vente",                                                  "base": "https://www.agence-eleonor.fr", "pattern": r"/fr/vente/"},
    {"id": "ledil",              "url": "https://ledil.immo/recherche/tous-types/47+46+82?",                                      "base": "https://ledil.immo", "pattern": r"/bien/"},
    {"id": "charles_loftie",     "url": "https://charles-loftie-immo.com/fr/recherche",                                          "base": "https://charles-loftie-immo.com", "pattern": r"/fr/selection"},
    {"id": "prada_prestige",     "url": "https://prada-prestige-immo.fr/notre-selection",                                         "base": "https://prada-prestige-immo.fr", "pattern": r"detail"},
    {"id": "orpi",               "url": "https://www.orpi.com/recherche/buy?sort=date-down",                                      "base": "https://www.orpi.com", "pattern": r"annonce-vente"},
]

async def scrape_site(browser, config):
    site_id = config["id"]
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    )
    page = await context.new_page()
    results = []

    try:
        await page.goto(config["url"], wait_until="networkidle", timeout=60000)
        await asyncio.sleep(2)
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        links = soup.find_all('a', href=True)

        for a in links:
            if len(results) >= 10:
                break

            href = a['href']
            if not re.search(config["pattern"], href, re.IGNORECASE):
                continue

            full_url = href if href.startswith('http') else config["base"] + href

            # -----------------------------
            # PRIJS DETECTIE
            # -----------------------------
            raw_text = a.get_text(" ", strip=True)
            parent_text = a.parent.get_text(" ", strip=True) if a.parent else ""
            combined = raw_text + " " + parent_text

            price_match = re.search(r"(\d[\d\s\.]*\d)\s*€", combined)
            prijs = price_match.group(0) if price_match else "N/A"

            # -----------------------------
            # FOTO DETECTIE
            # -----------------------------
            foto = None

            # 1) img binnen de link
            img = a.find("img")
            if img and img.get("src"):
                foto = img["src"]

            # 2) img in de parent
            if not foto and a.parent:
                img = a.parent.find("img")
                if img and img.get("src"):
                    foto = img["src"]

            # 3) fallback: zoek eerste img in de buurt
            if not foto:
                img = soup.find("img")
                if img and img.get("src"):
                    foto = img["src"]

            # 4) absolute URL maken
            if foto and not foto.startswith("http"):
                foto = config["base"].rstrip("/") + "/" + foto.lstrip("/")

            entry = {
                "Bron": site_id,
                "Plaats": "Onbekend",
                "Nieuw?": "Nee",
                "Prijs": prijs,
                "m2 Binnen": "N/A",
                "m2 Buiten": "N/A",
                "Slaapkamers": "N/A",
                "URL": full_url,
                "Foto": foto or "N/A"
            }

            if entry not in results:
                results.append(entry)

        print(f"✓ {site_id}: {len(results)} gevonden.")

    except Exception as e:
        print(f"✗ Fout bij {site_id}: {e}")

    finally:
        await page.close()
        await context.close()

    return results


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        all_data = []
        
        # Batching om GitHub runners niet te overbelasten
        batch_size = 3
        for i in range(0, len(SCRAPER_CONFIG), batch_size):
            batch = SCRAPER_CONFIG[i:i + batch_size]
            tasks = [scrape_site(browser, config) for config in batch]
            batch_results = await asyncio.gather(*tasks)
            for res in batch_results:
                all_data.extend(res)
        
        await browser.close()
        
        # Opslaan naar data.json
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        print(f"\nKlaar! data.json is gegenereerd met {len(all_data)} huizen.")

if __name__ == "__main__":
    asyncio.run(main())
