import asyncio
import json
import re
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

MAX_LISTINGS_PER_SITE = 6
MAX_CONCURRENT_SITES = 3

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
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
        "card_selector": ".jet-listing-grid__item",
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
    "url": "...",
    "base": "...",
    "pattern": r"/vente/",
    "card_selector": ".property, .item, article, .annonce",
},

    {
        "id": "human",
        "url": "https://www.human-immobilier.fr/achat-maison-tarn-et-garonne?og=0&sort=date-desc",
        "base": "https://www.human-immobilier.fr",
        "pattern": r"/annonce-",
    },
 {
    "id": "letuc",
    "url": "https://www.letuc.com/vente",
    "base": "https://www.letuc.com",
    "pattern": r"/vente/",
    "card_selector": ".property, .item, article, .annonce",
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
        "pattern": r"/fr/biens/",
        "card_selector": "article.property",
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
        "card_selector": ".jet-listing-grid__item, .jet-listing-dynamic-link__link, article",

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
    "card_selector": "article.property, .property-item",
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
    "card_selector": ".property-item, .estate-item, article.property",
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
    "pattern": r"/vente/",
    "card_selector": "article.property, .property-item",
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
    "card_selector": ".gh-search-result-card",
    "requires_js": True,
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
    "pattern": r"/fr/biens/",
    "card_selector": ".w-dyn-item, .listing-card, .property-card",
},

{
    "id": "prada_prestige",
    "url": "https://prada-prestige-immo.fr/notre-selection",
    "base": "https://prada-prestige-immo.fr",
    "pattern": r"/fr/vente/",
    "card_selector": "article.property, .property-item",
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
    # data-src
    img = card.find("img", attrs={"data-src": True})
    if img and not is_bad_image(img["data-src"]):
        return fix_url(img["data-src"], base)

    # src
    img = card.find("img", src=True)
    if img and not is_bad_image(img["src"]):
        return fix_url(img["src"], base)

    # srcset
    img = card.find("img", srcset=True)
    if img:
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
# GENERIC TITLE (alleen voor plaats-detectie)
# ---------------------------------------------------------
def extract_title(card, a):
    for tag in ["h1", "h2", "h3"]:
        h = card.find(tag)
        if h:
            t = h.get_text(strip=True)
            if t:
                return t

    for tag in ["strong", "b"]:
        h = card.find(tag)
        if h:
            t = h.get_text(strip=True)
            if t:
                return t

    t = a.get_text(" ", strip=True)
    if len(t.split()) > 2:
        return t

    txt = card.get_text(" ", strip=True)
    if len(txt.split()) > 3:
        return txt[:200]

    return ""

# ---------------------------------------------------------
# LISTING VALIDATION
# ---------------------------------------------------------
def looks_like_listing(text):
    return (
        "€" in text
        or "chambre" in text.lower()
        or "pièce" in text.lower()
        or "bed" in text.lower()
    )

# ---------------------------------------------------------
# SITE-SPECIFIEKE PARSERS
# ---------------------------------------------------------
def parse_lot_immoco(card, base):
    a = card.select_one("h1 a")
    url = fix_url(a["href"], base) if a else None
    titel = a.get_text(strip=True) if a else ""
    price_el = card.select_one("span[itemprop='price']")
    prijs = price_el.get("content") + " €" if price_el else card.get_text(" ", strip=True)
    img = card.select_one("div.panel-heading img")
    foto = fix_url(img["src"], base) if img else extract_image(card, base)
    return {"Titel": titel, "PrijsRaw": prijs, "Foto": foto, "URL": url}

def parse_quercygascogne(card, base):
    a = card.select_one("a.property__link")
    url = fix_url(a["href"], base) if a else None
    title_el = card.select_one(".property__title h2 span")
    titel = title_el.get_text(strip=True) if title_el else ""
    price_el = card.select_one(".property__price span")
    prijs = price_el.get_text(strip=True) if price_el else card.get_text(" ", strip=True)
    img = card.select_one(".property__visual img")
    foto = fix_url(img.get("data-src") or img.get("src"), base) if img else extract_image(card, base)
    return {"Titel": titel, "PrijsRaw": prijs, "Foto": foto, "URL": url}

def parse_mouly(card, base):
    a = card.select_one("a.item__title")
    url = fix_url(a["href"], base) if a else None
    title_el = card.select_one(".title__content-2")
    titel = title_el.get_text(strip=True) if title_el else ""
    price_el = card.select_one(".item__price .__price-value")
    prijs = price_el.get_text(strip=True) if price_el else card.get_text(" ", strip=True)
    img = card.select_one(".decorate__img")
    foto = fix_url(img["src"], base) if img else extract_image(card, base)
    return {"Titel": titel, "PrijsRaw": prijs, "Foto": foto, "URL": url}

def parse_wheeler(card, base):
    # URL
    a = card.select_one("a.jet-listing-dynamic-link__link")
    url = fix_url(a["href"], base) if a else None

    # Titel
    title_el = card.select_one(".jet-listing-dynamic-link__label, h2, .title")
    titel = title_el.get_text(strip=True) if title_el else ""

    # Prijs
    price_el = card.select_one(".jet-listing-dynamic-field__content")
    prijs = price_el.get_text(" ", strip=True) if price_el else ""

    # Foto
    img = card.select_one("img")
    foto = None
    if img:
        foto = fix_url(
            img.get("data-src")
            or img.get("data-lazy-src")
            or img.get("src")
            or img.get("srcset", "").split(" ")[0],
            base
        )

    return {
        "Titel": titel,
        "PrijsRaw": prijs,
        "Foto": foto,
        "URL": url,
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
        titel += " – " + subtitle_el.get_text(" ", strip=True)
    price_el = card.select_one(".price")
    prijs = price_el.get_text(" ", strip=True) if price_el else card.get_text(" ", strip=True)
    return {"Titel": titel, "PrijsRaw": prijs, "Foto": foto, "URL": url}

def parse_beauxvillages(card, base):
    a = card.select_one("a.jet-listing-dynamic-link__link")
    url = fix_url(a["href"], base) if a else None

    title_el = card.select_one(".jet-listing-dynamic-link__label")
    titel = title_el.get_text(strip=True) if title_el else ""

    price_el = card.select_one(".jet-listing-dynamic-field__content")
    prijs = price_el.get_text(" ", strip=True) if price_el else ""

    img = card.select_one("img")
    foto = fix_url(img.get("src"), base) if img else None

    return {
        "Titel": titel,
        "PrijsRaw": prijs,
        "Foto": foto,
        "URL": url,
    }

def parse_villereal(card, base):
    a = card.select_one("a.property__link")
    url = fix_url(a["href"], base) if a else None

    title_el = card.select_one(".property__title h2 span")
    titel = title_el.get_text(strip=True) if title_el else ""

    price_el = card.select_one(".property__price span")
    prijs = price_el.get_text(strip=True) if price_el else ""

    img = card.select_one(".property__visual img")
    foto = None
    if img:
        foto = fix_url(img.get("data-src") or img.get("src"), base)

    return {
        "Titel": titel,
        "PrijsRaw": prijs,
        "Foto": foto,
        "URL": url,
    }

def parse_soleil47(card, base):
    # URL
    a = card.find("a", href=True)
    url = fix_url(a["href"], base) if a else None

    # Titel
    title_el = card.select_one(".property__title h2, .property-title, h2")
    titel = title_el.get_text(strip=True) if title_el else ""

    # Prijs
    price_el = card.select_one(".property__price span, .price, .property-price")
    prijs = price_el.get_text(" ", strip=True) if price_el else ""

    # Foto
    img = card.select_one("img")
    foto = None
    if img:
        foto = fix_url(
            img.get("data-src") or img.get("src") or img.get("data-lazy-src"),
            base
        )

    return {
        "Titel": titel,
        "PrijsRaw": prijs,
        "Foto": foto,
        "URL": url,
    }

def parse_factor(card, base):
    # URL
    a = card.find("a", href=True)
    url = fix_url(a["href"], base) if a else None

    # Titel
    title_el = card.select_one("h2.title, .property-title, h2")
    titel = title_el.get_text(strip=True) if title_el else ""

    # Prijs
    price_el = card.select_one(".price, .property-price, .estate-price")
    prijs = price_el.get_text(" ", strip=True) if price_el else ""

    # Foto
    img = card.select_one("img")
    foto = None
    if img:
        foto = fix_url(
            img.get("data-src") or img.get("src") or img.get("data-lazy-src"),
            base
        )

    return {
        "Titel": titel,
        "PrijsRaw": prijs,
        "Foto": foto,
        "URL": url,
    }

def parse_arobase(card, base):
    # URL
    a = card.find("a", href=True)
    url = fix_url(a["href"], base) if a else None

    # Titel
    title_el = card.select_one("h2.title, .property-title, h2")
    titel = title_el.get_text(strip=True) if title_el else ""

    # Prijs
    price_el = card.select_one(".price, .property-price")
    prijs = price_el.get_text(" ", strip=True) if price_el else ""

    # Foto
    img = card.select_one("img")
    foto = None
    if img:
        foto = fix_url(
            img.get("data-src") or img.get("src") or img.get("data-lazy-src"),
            base
        )

    return {
        "Titel": titel,
        "PrijsRaw": prijs,
        "Foto": foto,
        "URL": url,
    }
def parse_guy_hoquet(card, base):
    # URL
    a = card.select_one("a[href]")
    url = fix_url(a["href"], base) if a else None

    # Titel
    title_el = card.select_one(".gh-title, h2, .title")
    titel = title_el.get_text(strip=True) if title_el else ""

    # Prijs
    price_el = card.select_one(".gh-price, .price")
    prijs = price_el.get_text(" ", strip=True) if price_el else ""

    # Foto
    img = card.select_one("img")
    foto = None
    if img:
        foto = fix_url(
            img.get("data-src") or img.get("src") or img.get("data-lazy-src"),
            base
        )

    return {
        "Titel": titel,
        "PrijsRaw": prijs,
        "Foto": foto,
        "URL": url,
    }
def parse_charles_loftie(card, base):
    # URL
    a = card.find("a", href=True)
    url = fix_url(a["href"], base) if a else None

    # Titel
    title_el = card.select_one(".listing-title, h3, .title")
    titel = title_el.get_text(strip=True) if title_el else ""

    # Prijs
    price_el = card.select_one(".price, .listing-price")
    prijs = price_el.get_text(" ", strip=True) if price_el else ""

    # Foto
    img = card.select_one("img")
    foto = None
    if img:
        foto = fix_url(
            img.get("src") or img.get("data-src") or img.get("srcset", "").split(" ")[0],
            base
        )

    return {
        "Titel": titel,
        "PrijsRaw": prijs,
        "Foto": foto,
        "URL": url,
    }
def parse_prada_prestige(card, base):
    # URL
    a = card.find("a", href=True)
    url = fix_url(a["href"], base) if a else None

    # Titel
    title_el = card.select_one(".property__title, h2, .title")
    titel = title_el.get_text(strip=True) if title_el else ""

    # Prijs
    price_el = card.select_one(".property__price span, .price")
    prijs = price_el.get_text(" ", strip=True) if price_el else ""

    # Foto
    img = card.select_one("img")
    foto = None
    if img:
        foto = fix_url(
            img.get("data-src") or img.get("src") or img.get("data-lazy-src"),
            base
        )

    return {
        "Titel": titel,
        "PrijsRaw": prijs,
        "Foto": foto,
        "URL": url,
    }
def parse_letuc(card, base):
    # URL
    a = card.find("a", href=True)
    url = fix_url(a["href"], base) if a else None

    # Titel
    title_el = card.select_one("h2, .property-title, .title")
    titel = title_el.get_text(strip=True) if title_el else ""

    # PRIJS — Letuc heeft 3 varianten:
    # 1) Koop: <span class="__price-value"> 249 000 € </span>
    # 2) Huur: <span class="__price-value"> 650 € CC </span>
    # 3) Referentie-ID: 115 553 335 (moet genegeerd worden)

    prijs = ""

    # 1) Koopprijs
    price_el = card.select_one(".__price-value, .price-value")
    if price_el:
        raw = price_el.get_text(" ", strip=True)

        # referentie-ID’s eruit filteren (te lang)
        if len(raw.replace(" ", "")) > 9:
            raw = ""

        # huur uitsluiten (optioneel)
        if any(x in raw.lower() for x in ["cc", "hc", "/mois", "mois"]):
            raw = ""

        prijs = raw

    # Foto
    img = card.select_one("img")
    foto = None
    if img:
        foto = fix_url(
            img.get("data-src")
            or img.get("data-lazy-src")
            or img.get("src")
            or img.get("srcset", "").split(" ")[0],
            base
        )

    return {
        "Titel": titel,
        "PrijsRaw": prijs,
        "Foto": foto,
        "URL": url,
    }

def parse_pouget(card, base):
    # URL
    a = card.find("a", href=True)
    url = fix_url(a["href"], base) if a else None

    # Titel
    title_el = card.select_one("h2, .title, .property-title")
    titel = title_el.get_text(strip=True) if title_el else ""

    # Prijs
    price_el = card.select_one(".price, .property-price, .item-price")
    prijs = price_el.get_text(" ", strip=True) if price_el else ""

    # Foto
    img = card.select_one("img")
    foto = None
    if img:
        foto = fix_url(
            img.get("data-src")
            or img.get("data-lazy-src")
            or img.get("src")
            or img.get("srcset", "").split(" ")[0],
            base
        )

    return {
        "Titel": titel,
        "PrijsRaw": prijs,
        "Foto": foto,
        "URL": url,
    }


SITE_PARSERS = {
    "lot_immoco": parse_lot_immoco,
    "quercygascogne": parse_quercygascogne,
    "mouly": parse_mouly,
    "wheeler": parse_wheeler,
    "eleonor": parse_eleonor,
    "beauxvillages": parse_beauxvillages,
    "villereal":  parse_villereal,
    "soleil47": parse_soleil47,
    "factor": parse_factor,
    "arobase": parse_arobase,
    "guy_hoquet": parse_guy_hoquet,
    "charles_loftie": parse_charles_loftie,
    "prada_prestige": parse_prada_prestige,
    "letuc": parse_letuc,
    "pouget": parse_pouget,

}

# ---------------------------------------------------------
# PLAATS-DETECTIE
# ---------------------------------------------------------
FORBIDDEN_PLACE_WORDS = {
    "maison","appartement","immeuble","terrain","propriete","propriété",
    "villa","ensemble","magnifique","charmante","charmant","belle","grand",
    "estimation","country","chateau","château","exclusif","exclusivité",
    "garage","jardin","piscine","dependances","dépendances","vue","boisé",
    "boise","studio","local","commerce","commercial"
}

def normalize_word(w):
    w = w.strip(" ,.;:!?\t\n").lower()
    w = w.replace("’","'")
    w = w.replace("é","e").replace("è","e").replace("ê","e")
    w = w.replace("à","a").replace("â","a")
    w = w.replace("ô","o").replace("ù","u").replace("û","u")
    return w

def is_valid_place_word(w):
    if not w:
        return False
    w_norm = normalize_word(w)
    if w_norm in FORBIDDEN_PLACE_WORDS:
        return False
    if any(c.isdigit() for c in w_norm):
        return False
    if len(w_norm) < 3:
        return False
    return True

def format_place(words):
    return " ".join(w.capitalize() for w in words)

def detect_place_from_url(url):
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

def detect_place_from_title(title):
    if not title:
        return None
    parts = re.split(r"[,\-–|/]", title)
    for p in parts:
        words = [w for w in p.split() if is_valid_place_word(w)]
        if words:
            return format_place(words)
    return None

def detect_place_from_text(text):
    if not text:
        return None
    words = [w for w in text.split() if is_valid_place_word(w)]
    if words:
        return words[0].capitalize()
    return None

def detect_place(url, title, text):
    # -----------------------------------------
    # SITE-SPECIFIEKE OVERRIDES
    # -----------------------------------------
    if url and "lot-immoco.net" in url:
        return "Lot"

    # -----------------------------------------
    # GENERIEKE DETECTIE
    # -----------------------------------------
    return (
        detect_place_from_url(url)
        or detect_place_from_title(title)
        or detect_place_from_text(text)
        or "Onbekend"
    )


# ---------------------------------------------------------
# PRIJS-DETECTIE
# ---------------------------------------------------------
def extract_price(text):
    if not text:
        return "N/A"
    matches = re.findall(r"(\d[\d\s \.]*\d)\s*€", text)
    if not matches:
        return "N/A"
    price = matches[0]
    price = price.replace(" "," ").replace(" ","").replace(".","")
    try:
        n = int(price)
        return f"{n:,}".replace(",", " ") + " €"
    except:
        return matches[0].strip() + " €"

# ---------------------------------------------------------
# m2 BINNEN / BUITEN
# ---------------------------------------------------------
OUTSIDE_HINTS = ["terrain","jardin","parcelle","land","plot","boise","boisé","hectares","ha"]

def extract_m2(text):
    if not text:
        return ("N/A","N/A")
    tokens = text.split()
    inside = None
    outside = None
    for i,tok in enumerate(tokens):
        m = re.match(r"(\d+)\s*(m²|m2)", tok.replace("mÂ²","m²"))
        if not m:
            if i+1 < len(tokens) and re.match(r"m²|m2", tokens[i+1]):
                m = re.match(r"(\d+)", tok)
            else:
                continue
        if not m:
            continue
        val = m.group(1)
        context = " ".join(tokens[max(0,i-5):i]).lower()
        if any(h in context for h in OUTSIDE_HINTS):
            if outside is None:
                outside = val
        else:
            if inside is None:
                inside = val
    return (inside or "N/A", outside or "N/A")

# ---------------------------------------------------------
# SLAAPKAMERS
# ---------------------------------------------------------
def extract_bedrooms(text):
    if not text:
        return "N/A"
    m = re.search(r"(\d+)\s*(chambres?|ch\.|bedrooms?|bed)", text, re.I)
    if m:
        return m.group(1)
    return "N/A"

# ---------------------------------------------------------
# NIEUW?
# ---------------------------------------------------------
def detect_new(text):
    if not text:
        return "Nee"
    t = text.lower()
    if any(w in t for w in ["nouveaute","nouveauté","new","recent","récent"]):
        return "Ja"
    if "exclusiv" in t:
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
        if node.name in ("article","li","section","div"):
            return node
    return a.parent

# ---------------------------------------------------------
# SCRAPE LIST PAGE
# ---------------------------------------------------------
async def scrape_list_page(browser, config):
    site_id = config["id"]
    print(f"--- Start: {site_id} ---")

    context = await browser.new_context(
        viewport={"width":1400,"height":900},
        user_agent="Mozilla/5.0"
    )
    page = await context.new_page()

    # SAFE GOTO
    try:
        await page.goto(config["url"], wait_until="networkidle", timeout=60000)
    except:
        print(f"{site_id}: timeout, skipping")
        await context.close()
        return []

   # SCROLL STRATEGY
    if site_id == "beauxvillages":
        # infinite scroll until no new items
        last_height = 0
        for _ in range(12):
            try:
                await page.mouse.wheel(0, 3000)
                await asyncio.sleep(1.5)
                new_height = await page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            except:
                break

    elif config.get("requires_js"):
        # wait for JS-rendered cards
        try:
            await page.wait_for_selector(config["card_selector"], timeout=20000)
        except:
            print(f"{site_id}: no cards rendered")
            await context.close()
            return []

        # infinite scroll until no new cards appear
        last_count = 0
        for _ in range(12):
            cards = await page.query_selector_all(config["card_selector"])
            if len(cards) == last_count:
                break
            last_count = len(cards)
            await page.mouse.wheel(0, 3000)
            await asyncio.sleep(1.2)

    else:
        # normal scroll for static sites
        for _ in range(4):
            try:
                await page.mouse.wheel(0, 2000)
                await asyncio.sleep(1)
            except:
                break



    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")

    pattern = re.compile(config["pattern"], re.I)

    anchors = []
    for a in soup.find_all("a", href=True):
        if pattern.search(a["href"]):
            anchors.append((a, fix_url(a["href"], config["base"])))

 # SPECIAL FIX: Wheeler duplicates cards in DOM
if site_id == "wheeler":
    seen = set()
    unique = []
    for a, href in anchors:
        if href not in seen:
            seen.add(href)
            unique.append((a, href))
    # limit to real cards (JetEngine repeats 6x)
    unique = unique[:6]
else:
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
        parsed = parser(card, config["base"])
        titel = parsed.get("Titel", "")
        prijs_raw = parsed.get("PrijsRaw", text)
        foto = parsed.get("Foto") or extract_image(card, config["base"]) or "N/A"
        url = parsed.get("URL") or href
    else:
        titel = extract_title(card, a)
        prijs_raw = text
        foto = extract_image(card, config["base"]) or "N/A"
        url = href

    # FILTER: huur uitsluiten
    if any(x in prijs_raw.lower() for x in ["cc", "hc", "/mois", "mois", "€/mois"]):
        continue

    prijs = extract_price(prijs_raw)
    m2_binnen, m2_buiten = extract_m2(text)
    slaapkamers = extract_bedrooms(text)
    nieuw = detect_new(text + " " + titel)
    plaats = detect_place(url, titel, text)

    listings.append({
        "Bron": site_id,
        "Plaats": plaats,
        "Nieuw?": nieuw,
        "Prijs": prijs,
        "m2 Binnen": m2_binnen,
        "m2 Buiten": m2_buiten,
        "Slaapkamers": slaapkamers,
        "URL": url,
        "Foto": foto,
    })

    if len(listings) >= MAX_LISTINGS_PER_SITE:
        break


    print(f"{site_id}: {len(listings)} listings")
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

        with open("data.json","w",encoding="utf-8") as f:
            json.dump(flat,f,ensure_ascii=False,indent=2)

        print(f"\n✅ Klaar! {len(flat)} panden gescraped.")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
