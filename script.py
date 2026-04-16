import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time

def extract_features(text):
    data = {
        'Plaats': 'Onbekend',
        'Prijs': 'N/A',
        'm2_binnen': 'N/A',
        'm2_buiten': 'N/A',
        'Slaapkamers': 'N/A',
        'Badkamers': 'N/A',
        'Is_Nieuw': False
    }

    city_match = re.search(r'(?:(?<=\s)|(?<=^))([A-Z\s\-\']{3,})(?=\s\d{5}|\s\(|\s$|\s-)', text)
    zip_match = re.search(r'\b(\d{5})\b', text)

    if city_match:
        data['Plaats'] = city_match.group(1).strip()
    elif zip_match:
        data['Plaats'] = f"Regio {zip_match.group(1)}"

    if re.search(r'(nouveau|nouveauté|new|récent|exclusivité)', text, re.I):
        data['Is_Nieuw'] = True

    price = re.search(r'(\d[\d\s.,]*€)', text)
    if price:
        data['Prijs'] = price.group(1).replace('\xa0', ' ').strip()

    m2_matches = re.findall(r'(\d+[.,]?\d*)\s*m[²2]', text)
    if m2_matches:
        clean_nums = []
        for n in m2_matches:
            try:
                num = int(float(n.replace(',', '.').replace(' ', '')))
                clean_nums.append(num)
            except:
                continue

        if clean_nums:
            clean_nums.sort()
            data['m2_binnen'] = str(clean_nums[0])
            if len(clean_nums) > 1:
                data['m2_buiten'] = str(clean_nums[-1])

    rooms = re.search(r'(\d+)\s*(chambre|chb|ch|bed)', text, re.I)
    if rooms:
        data['Slaapkamers'] = rooms.group(1)

    return data


def run_real_estate_scraper():
    configs = [
       {"id": "ladresse_tournon", "url": "https://www.ladresse.com/agence/l-adresse-tournon-d-agenais/266/acheter?sort=date-desc", "base": "https://www.ladresse.com", "pattern": "/achat/"},
        {"id": "beauxvillages", "url": "https://beauxvillages.com/fr/nouveau-sur-le-march%C3%A9?hotsheet=1", "base": "https://beauxvillages.com", "pattern": "/property/"},
        {"id": "lot_immoco", "url": "https://www.lot-immoco.net/a-vendre/1", "base": "https://www.lot-immoco.net", "pattern": "\\.html$"},
        {"id": "pouget", "url": "https://www.agencespouget.com/nos-biens", "base": "https://www.agencespouget.com", "pattern": "/vente/"},
        {"id": "human", "url": "https://www.human-immobilier.fr/achat-maison-tarn-et-garonne?og=0&sort=date-desc", "base": "https://www.human-immobilier.fr", "pattern": "/annonce-"},
        {"id": "letuc", "url": "https://www.letuc.com/recherche/", "base": "https://www.letuc.com", "pattern": "/t[0-9]+/"},
        {"id": "nestenn", "url": "https://immobilier-villeneuve-sur-lot.nestenn.com/achat-immobilier", "base": "https://immobilier-villeneuve-sur-lot.nestenn.com", "pattern": "ref-"},
        {"id": "century21", "url": "https://www.century21-bg-villeneuve.com/annonces/achat/", "base": "https://www.century21-bg-villeneuve.com", "pattern": "/detail/"},
        {"id": "lot_et_garonne", "url": "https://www.lot-et-garonne-immobilier.com/bien-a-acheter.html", "base": "https://www.lot-et-garonne-immobilier.com", "pattern": "\\.html$"},
        {"id": "valadie", "url": "https://valadie-immobilier.com/fr/biens/a_vendre/1", "base": "https://valadie-immobilier.com", "pattern": "/fiche/"},
        {"id": "villereal", "url": "https://www.immobilier-villereal.com/fr/nos-biens-tous", "base": "https://www.immobilier-villereal.com", "pattern": "/property/"},
        {"id": "quercygascogne", "url": "https://www.quercygascogne.fr/ventes/1", "base": "https://www.quercygascogne.fr", "pattern": "/[0-9]+-"},
        {"id": "xavier", "url": "https://xavierimmobilier.fr/gb/15-properties-up-to-350000", "base": "https://xavierimmobilier.fr", "pattern": "/[0-9]+-"},
        {"id": "wheeler", "url": "https://wheeler-property.com/for-sale/", "base": "https://wheeler-property.com", "pattern": "/properties/"},
        {"id": "mouly", "url": "https://www.mouly-immobilier.com/recherche/", "base": "https://www.mouly-immobilier.com", "pattern": "/[0-9]+-"},
        {"id": "soleil47", "url": "https://www.soleil-immobilier-47.com/vente/1", "base": "https://www.soleil-immobilier-47.com", "pattern": "/[0-9]+-"},
        {"id": "marin", "url": "https://www.immobilier-marin.com/vente/1", "base": "https://www.immobilier-marin.com", "pattern": "/[0-9]+-"},
        {"id": "factor", "url": "https://www.factorimmo.com/nos-biens/", "base": "https://www.factorimmo.com", "pattern": "/[0-9]+-"},
        {"id": "pousset", "url": "https://www.immobilier-pousset.fr/vente/1", "base": "https://www.immobilier-pousset.fr", "pattern": "/[0-9]+-/"},
        {"id": "arobase", "url": "https://www.arobaseimmobilier.fr/vente/1", "base": "https://www.arobaseimmobilier.fr", "pattern": "/[0-9]+-"},
        {"id": "immo46", "url": "https://www.immo46.com/fr/a-vendre", "base": "https://www.immo46.com", "pattern": ",P[0-9]"},
        {"id": "pleinsud", "url": "https://www.pleinsudimmo.fr/nos-biens-immobiliers", "base": "https://www.pleinsudimmo.fr", "pattern": "\\.html$"},
        {"id": "ledil", "url": "https://ledil.immo/recherche/tous-types/46+47+82?", "base": "https://ledil.immo", "pattern": "/bien/"}
    ]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    all_results = []
    seen_urls = set()

    for conf in configs:
        try:
            print(f"Scraping: {conf['id']}")

            res = requests.get(conf['url'], headers=headers, timeout=15)
            res.raise_for_status()  # 🔥 belangrijke toevoeging

            soup = BeautifulSoup(res.text, 'html.parser')
            links = soup.find_all('a', href=True)

            count = 0

            for a in links:
                href = a['href']

                if re.search(conf['pattern'], href):
                    full_url = href if href.startswith('http') else conf['base'] + href

                    if full_url in seen_urls:
                        continue

                    seen_urls.add(full_url)

                    container = a.find_parent(['div', 'article', 'li', 'section', 'td'])
                    text = container.get_text(separator=' ', strip=True) if container else ""

                    if len(text) < 40:
                        continue

                    features = extract_features(text)

                    all_results.append({
                        'Bron': conf['id'],
                        'Plaats': features['Plaats'],
                        'Nieuw?': 'JA' if features['Is_Nieuw'] else 'Nee',
                        'Prijs': features['Prijs'],
                        'm2 Binnen': features['m2_binnen'],
                        'm2 Buiten': features['m2_buiten'],
                        'Slaapkamers': features['Slaapkamers'],
                        'URL': full_url
                    })

                    count += 1

                if count >= 6:
                    break

            time.sleep(2)  # iets rustiger → minder kans op blokkade

        except Exception as e:
            print(f"Fout bij {conf['id']}: {e}")

    return pd.DataFrame(all_results)


# RUN
df_final = run_real_estate_scraper()

print(f"Totaal gevonden: {len(df_final)}")
print(df_final.head())

# Opslaan
df_final.to_csv('huizen_met_plaats.csv', index=False)
df_final.to_json("data.json", orient="records", indent=2)

print("✅ Data opgeslagen!")
