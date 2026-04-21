import asyncio
import json
import re
from urllib.parse import urlparse
from playwright.async_api import async_playwright
from playwright_stealth import stealth_async
from bs4 import BeautifulSoup

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
MAX_LISTINGS_PER_SITE = 8
MAX_CONCURRENT_SITES = 2   # VEILIG voor GitHub Actions
MAX_RETRIES = 3

# ← PLAATS HIER DE SCRAPER_CONFIG MET SELECTORS DIE IK JE GAF

SCRAPER_CONFIG = [
    {
        "id": "ladresse_tournon",
        "url": "https://www.ladresse.com/agence/l-adresse-tournon-d-agenais/266/acheter?sort=date-desc",
        "base": "https://www.ladresse.com",
        "pattern": r"/achat/",
        "selectors": {
            "listing_container": "div.property-item",
            "link": "a.property-item__link",
            "price": ".property-item__price",
            "image": "img"
        }
    },
    {
        "id": "beauxvillages",
        "url": "https://beauxvillages.com/en/latest-properties?hotsheet=1",
        "base": "https://beauxvillages.com",
        "pattern": r"/property/",
        "selectors": {
            "listing_container": "div.hotsheet-item",
            "link": "a",
            "price": ".price",
            "image": "img"
        }
    },
    {
        "id": "lot_immoco",
        "url": "https://www.lot-immoco.net/a-vendre/1",
        "base": "https://www.lot-immoco.net",
        "pattern": r"\.html$",
        "selectors": {
            "listing_container": "div.annonce",
            "link": "a",
            "price": ".prix",
            "image": "img"
        }
    },
    {
        "id": "pouget",
        "url": "https://www.agencespouget.com/vente/1",
        "base": "https://www.agencespouget.com",
        "pattern": r"/vente/",
        "selectors": {
            "listing_container": ".bien",
            "link": "a",
            "price": ".prix",
            "image": "img"
        }
    },
    {
        "id": "human",
        "url": "https://www.human-immobilier.fr/achat-maison-tarn-et-garonne?og=0&sort=date-desc",
        "base": "https://www.human-immobilier.fr",
        "pattern": r"/annonce-",
        "selectors": {
            "listing_container": ".annonce",
            "link": "a",
            "price": ".price",
            "image": "img"
        }
    },
    {
        "id": "letuc",
        "url": "https://www.letuc.com/recherche/",
        "base": "https://www.letuc.com",
        "pattern": r"/t[0-9]+/",
        "selectors": {
            "listing_container": ".bien",
            "link": "a",
            "price": ".prix",
            "image": "img"
        }
    },
    {
        "id": "nestenn",
        "url": "https://immobilier-villeneuve-sur-lot.nestenn.com/achat-immobilier",
        "base": "https://immobilier-villeneuve-sur-lot.nestenn.com",
        "pattern": r"ref-",
        "selectors": {
            "listing_container": ".item",
            "link": "a",
            "price": ".price",
            "image": "img"
        }
    },
    {
        "id": "century21",
        "url": "https://www.century21-bg-villeneuve.com/annonces/achat/",
        "base": "https://www.century21-bg-villeneuve.com",
        "pattern": r"/detail/",
        "selectors": {
            "listing_container": ".annonce",
            "link": "a",
            "price": ".price",
            "image": "img"
        }
    },
    {
        "id": "lot_et_garonne",
        "url": "https://www.lot-et-garonne-immobilier.com/bien-a-acheter.html",
        "base": "https://www.lot-et-garonne-immobilier.com",
        "pattern": r"\.html$",
        "selectors": {
            "listing_container": ".bien",
            "link": "a",
            "price": ".prix",
            "image": "img"
        }
    },
    {
        "id": "valadie",
        "url": "https://valadie-immobilier.com/fr/biens/a_vendre/1",
        "base": "https://valadie-immobilier.com",
        "pattern": r"/fiche/",
        "selectors": {
            "listing_container": ".property",
            "link": "a",
            "price": ".price",
            "image": "img"
        }
    },
    {
        "id": "villereal",
        "url": "https://www.immobilier-villereal.com/fr/nos-biens-tous",
        "base": "https://www.immobilier-villereal.com",
        "pattern": r"/property/",
        "selectors": {
            "listing_container": ".property",
            "link": "a",
            "price": ".price",
            "image": "img"
        }
    },
    {
        "id": "quercygascogne",
        "url": "https://www.quercygascogne.fr/ventes/1",
        "base": "https://www.quercygascogne.fr",
        "pattern": r"/[0-9]+-",
        "selectors": {
            "listing_container": ".bien",
            "link": "a",
            "price": ".prix",
            "image": "img"
        }
    },
    {
        "id": "xavier",
        "url": "https://xavierimmobilier.fr/gb/15-properties-up-to-350000",
        "base": "https://xavierimmobilier.fr",
        "pattern": r"/[0-9]+-",
        "selectors": {
            "listing_container": ".product",
            "link": "a",
            "price": ".price",
            "image": "img"
        }
    },
    {
        "id": "wheeler",
        "url": "https://wheeler-property.com/for-sale/",
        "base": "https://wheeler-property.com",
        "pattern": r"/properties/",
        "selectors": {
            "listing_container": ".property-card",
            "link": "a",
            "price": ".price",
            "image": "img"
        }
    },
    {
        "id": "mouly",
        "url": "https://www.mouly-immobilier.com/recherche/",
        "base": "https://www.mouly-immobilier.com",
        "pattern": r"/[0-9]+-",
        "selectors": {
            "listing_container": ".bien",
            "link": "a",
            "price": ".prix",
            "image": "img"
        }
    },
    {
        "id": "soleil47",
        "url": "https://www.soleil-immobilier-47.com/vente/1",
        "base": "https://www.soleil-immobilier-47.com",
        "pattern": r"/[0-9]+-",
        "selectors": {
            "listing_container": ".bien",
            "link": "a",
            "price": ".prix",
            "image": "img"
        }
    },
    {
        "id": "marin",
        "url": "https://www.immobilier-marin.com/vente/1",
        "base": "https://www.immobilier-marin.com",
        "pattern": r"/[0-9]+-",
        "selectors": {
            "listing_container": ".bien",
            "link": "a",
            "price": ".prix",
            "image": "img"
        }
    },
    {
        "id": "factor",
        "url": "https://www.factorimmo.com/nouveautes/1",
        "base": "https://www.factorimmo.com",
        "pattern": r"/[0-9]+-",
        "selectors": {
            "listing_container": ".bien",
            "link": "a",
            "price": ".prix",
            "image": "img"
        }
    },
    {
        "id": "pousset",
        "url": "https://www.immobilier-pousset.fr/vente/1",
        "base": "https://www.immobilier-pousset.fr",
        "pattern": r"/vente/",
        "selectors": {
            "listing_container": ".bien",
            "link": "a",
            "price": ".prix",
            "image": "img"
        }
    },
    {
        "id": "arobase",
        "url": "https://www.arobaseimmobilier.fr/vente/1",
        "base": "https://www.arobaseimmobilier.fr",
        "pattern": r"/[0-9]+-",
        "selectors": {
            "listing_container": ".bien",
            "link": "a",
            "price": ".prix",
            "image": "img"
        }
    },
    {
        "id": "immo46",
        "url": "https://www.immo46.com/fr/a-vendre",
        "base": "https://www.immo46.com",
        "pattern": r",P[0-9]",
        "selectors": {
            "listing_container": ".property",
            "link": "a",
            "price": ".price",
            "image": "img"
        }
    },
    {
        "id": "pleinsud",
        "url": "https://www.pleinsudimmo.fr/nos-biens-immobiliers",
        "base": "https://www.pleinsudimmo.fr",
        "pattern": r"\.html$",
        "selectors": {
            "listing_container": ".bien",
            "link": "a",
            "price": ".prix",
            "image": "img"
        }
    },
    {
        "id": "signature_agenaise",
        "url": "https://www.la-signature-agenaise.fr/fr/vente?orderBy=2",
        "base": "https://www.la-signature-agenaise.fr",
        "pattern": r"/fr/vente/",
        "selectors": {
            "listing_container": ".property",
            "link": "a",
            "price": ".price",
            "image": "img"
        }
    },
    {
        "id": "maisondelimmobilier",
        "url": "https://www.maisondelimmobilier.com/catalog/advanced_search_result.php?C_28=Vente",
        "base": "https://www.maisondelimmobilier.com",
        "pattern": r"fiches",
        "selectors": {
            "listing_container": ".product",
            "link": "a",
            "price": ".price",
            "image": "img"
        }
    },
    {
        "id": "guy_hoquet",
        "url": "https://www.guy-hoquet.com/biens/result#1&f20=46_c2,47_c2,82_c2",
        "base": "https://www.guy-hoquet.com",
        "pattern": r"/bien/",
        "selectors": {
            "listing_container": ".annonce",
            "link": "a",
            "price": ".price",
            "image": "img"
        }
    },
    {
        "id": "eleonor",
        "url": "https://www.agence-eleonor.fr/fr/vente",
        "base": "https://www.agence-eleonor.fr",
        "pattern": r"/fr/vente/",
        "selectors": {
            "listing_container": ".property",
            "link": "a",
            "price": ".price",
            "image": "img"
        }
    },
    {
        "id": "ledil",
        "url": "https://ledil.immo/recherche/tous-types/47+46+82?",
        "base": "https://ledil.immo",
        "pattern": r"/bien/",
        "selectors": {
            "listing_container": ".property",
            "link": "a",
            "price": ".price",
            "image": "img"
        }
    },
    {
        "id": "charles_loftie",
        "url": "https://charles-loftie-immo.com/fr/recherche",
        "base": "https://charles-loftie-immo.com",
        "pattern": r"/fr/selection",
        "selectors": {
            "listing_container": ".property",
            "link": "a",
            "price": ".price",
            "image": "img"
        }
    },
    {
        "id": "prada_prestige",
        "url": "https://prada-prestige-immo.fr/notre-selection",
        "base": "https://prada-prestige-immo.fr",
        "pattern": r"detail",
        "selectors": {
            "listing_container": ".property",
            "link": "a",
            "price": ".price",
            "image": "img"
        }
    },
    {
        "id": "orpi",
        "url": "https://www.orpi.com/recherche/buy?sort=date-down",
        "base": "https://www.orpi.com",
        "pattern": r"annonce-vente",
        "selectors": {
            "listing_container": ".search-card",
            "link": "a",
            "price": ".price",
            "image": "img"
        }
    }
]
# ─────────────────────────────────────────
# URL FIX
# ─────────────────────────────────────────
def fix_url(url, base_url):
    if not url:
        return None
    if not url.startswith("http"):
        url = base_url.rstrip("/") + "/" + url.lstrip("/")
    parsed = urlparse(url)
    if not parsed.path.endswith("/") and not re.search(r"\.[a-z0-9]{2,4}$", parsed.path, re.I):
        url += "/"
    return url

# ─────────────────────────────────────────
# HOOFDPAGINA RETRY
# ─────────────────────────────────────────
async def goto_with_retry(page, url):
    for attempt in range(1, 4):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=25000)
            await asyncio.sleep(2.5)  # wacht op JS-rendering
            return
        except Exception as e:
            print(f"  ! Retry {attempt}/3 voor hoofdpagina: {url}")
            await asyncio.sleep(1.5 * attempt)
    raise Exception(f"Hoofdpagina definitief mislukt: {url}")

# ─────────────────────────────────────────
# EXTRACT 8 NIEUWSTE LINKS (DOM-volgorde)
# ─────────────────────────────────────────
async def extract_listing_links(page, config):
    sel = config.get("selectors", {})

    # 1. Wacht tot body bestaat
    try:
        await page.wait_for_selector("body", timeout=8000)
    except:
        pass

    # 2. Lazy-load scroll in 4 stappen
    for _ in range(4):
        await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        await asyncio.sleep(1.2)

    # 3. Custom listing containers
    if "listing_container" in sel:
        containers = await page.query_selector_all(sel["listing_container"])
        if len(containers) > 0:
            links = []
            for c in containers:
                try:
                    a = await c.query_selector(sel.get("link", "a"))
                    if a:
                        href = await a.get_attribute("href")
                        if href:
                            links.append(fix_url(href, config["base"]))
                except:
                    continue

            if len(links) > 0:
                return links[:MAX_LISTINGS_PER_SITE]

    # 4. Fallback: regex op alle <a> tags
    raw_links = await page.evaluate("""
        () => Array.from(document.querySelectorAll('a')).map(a => a.href)
    """)

    pattern = re.compile(config["pattern"], re.I)
    filtered = [l for l in raw_links if pattern.search(l)]

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for l in filtered:
        if l not in seen:
            seen.add(l)
            unique.append(l)

    fixed = [fix_url(l, config["base"]) for l in unique]

    return fixed[:MAX_LISTINGS_PER_SITE]

# ─────────────────────────────────────────
# DETAIL SCRAPER (kern)
# ─────────────────────────────────────────
async def scrape_details(browser, url, config):
    sel = config.get("selectors", {})

    context = await browser.new_context(
        viewport={"width": 1280, "height": 800},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    page = await context.new_page()

    await stealth_async(page)

    page.set_default_timeout(20000)
    page.set_default_navigation_timeout(25000)

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=25000)
        await page.evaluate("window.scrollTo(0, 400)")
        await asyncio.sleep(0.5)

        content = await page.content()
        soup = BeautifulSoup(content, "html.parser")

        # PRIJS
        prijs = "N/A"
        if "price" in sel:
            el = await page.query_selector(sel["price"])
            if el:
                prijs = (await el.inner_text()).strip()
        else:
            visible_text = await page.evaluate("() => document.body.innerText")
            price_matches = re.findall(r"(\d[\d\s.]*)\s?€|€\s?(\d[\d\s.]*)", visible_text)
            if price_matches:
                for m in price_matches:
                    found = m[0] or m[1]
                    clean = re.sub(r"[^\d]", "", found)
                    if clean and int(clean) > 1000:
                        prijs = f"{found.strip()} €"
                        break

        # FOTO
        foto = "N/A"
        if "image" in sel:
            img = await page.query_selector(sel["image"])
            if img:
                src = await img.get_attribute("src")
                if src:
                    foto = fix_url(src, config["base"])
        else:
            og = soup.find("meta", property="og:image")
            if og and og.get("content"):
                foto = og["content"]

        # PLAATS
        h1 = soup.find("h1")
        plaats = h1.get_text(strip=True)[:100] if h1 else "Onbekend"

        # M2
        visible_text = await page.evaluate("() => document.body.innerText")
        m2_match = re.search(r"(\d+)\s?m²", visible_text)
        m2 = m2_match.group(1) if m2_match else "N/A"

        await context.close()
        return {
            "Bron": config["id"],
            "Plaats": plaats,
            "Prijs": prijs,
            "m2 Binnen": m2,
            "Foto": foto,
            "URL": url
        }

    except Exception as e:
        await context.close()
        raise e

# ─────────────────────────────────────────
# DETAIL SCRAPER MET RETRY
# ─────────────────────────────────────────
async def scrape_detail_with_retry(browser, url, config):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return await asyncio.wait_for(
                scrape_details(browser, url, config),
                timeout=35
            )
        except Exception as e:
            print(f"    ! Retry {attempt}/{MAX_RETRIES} voor {url}: {e}")
            await asyncio.sleep(1.5 * attempt)

    print(f"    ✖ Definitief mislukt: {url}")
    return None

# ─────────────────────────────────────────
# SITE SCRAPER
# ─────────────────────────────────────────
async def scrape_site(browser, config):
    print(f"--- Start: {config['id']} ---")

    context = await browser.new_context()
    page = await context.new_page()

    page.set_default_timeout(20000)
    page.set_default_navigation_timeout(25000)

    results = []

    try:
        await goto_with_retry(page, config["url"])

        links = await extract_listing_links(page, config)
        print(f"  {config['id']}: {len(links)} nieuwste links gevonden")

        tasks = [
            scrape_detail_with_retry(browser, link, config)
            for link in links
        ]

        detail_results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in detail_results:
            if isinstance(r, Exception):
                print("    ! Detail fout:", r)
            elif r:
                results.append(r)

    except Exception as e:
        print(f"  ! Fout bij {config['id']}: {e}")

    await context.close()
    return results

# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_SITES)

        async def sem_task(config):
            async with semaphore:
                return await scrape_site(browser, config)

        all_results = await asyncio.gather(*[sem_task(c) for c in SCRAPER_CONFIG])

        flat = [item for sub in all_results for item in sub]

        with open("data.json", "w", encoding="utf-8") as f:
            json.dump(flat, f, ensure_ascii=False, indent=2)

        print(f"\n✅ Klaar! Totaal {len(flat)} huizen gescraped.")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
