import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import logging
from urllib.parse import urljoin
from typing import List, Dict, Optional, Tuple
import re

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://en.wikipedia.org"
INDEX_URL = f"{BASE_URL}/wiki/Lists_of_Olympic_medalists"
OUTPUT_DIR = "scraper/output"

os.makedirs(OUTPUT_DIR, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
})

SPORT_KEYWORDS = [
    'Athletics', 'Swimming', 'Gymnastics', 'Volleyball', 'Basketball',
    'Football', 'Tennis', 'Badminton', 'Boxing', 'Weightlifting',
    'Judo', 'Wrestling', 'Fencing', 'Archery', 'Shooting'
]


def fetch_page(url: str) -> Optional[BeautifulSoup]:
    """Fetch a page and return BeautifulSoup object, or None on error."""
    try:
        response = SESSION.get(url, timeout=15)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    except requests.RequestException as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return None


def get_sport_links() -> List[Tuple[str, str]]:
    """Extract sport-specific links from the main index page."""
    logger.info("Fetching index page to find sport links...")
    soup = fetch_page(INDEX_URL)
    if not soup:
        logger.error("Could not fetch index page")
        return []

    sports = []
    content = soup.find('div', {'id': 'mw-content-text'})
    if not content:
        logger.warning("Could not find main content div")
        return []

    for link in content.find_all('a', href=True):
        href = link.get('href', '')
        text = link.get_text(strip=True)

        if href.startswith('/wiki/') and 'Olympic_medalists' in href:
            if href == '/wiki/Lists_of_Olympic_medalists':
                continue

            sport_match = None
            for keyword in SPORT_KEYWORDS:
                if keyword.lower() in text.lower():
                    sport_match = keyword
                    break

            if sport_match:
                full_url = urljoin(BASE_URL, href)
                sports.append((sport_match, full_url))

    seen = set()
    unique_sports = []
    for sport, url in sports:
        if url not in seen:
            seen.add(url)
            unique_sports.append((sport, url))

    return unique_sports


def detect_columns(headers: List[str]) -> Dict[str, Optional[int]]:
    """Detect medal table column indices from header row."""
    headers_lower = [h.lower().strip() for h in headers]

    col_indices = {
        'athlete': None,
        'country': None,
        'year': None,
        'event': None,
        'medal': None
    }

    athlete_patterns = ['athlete', 'name', 'competitor', 'sportsperson']
    country_patterns = ['country', 'noc', 'nation', 'team']
    year_patterns = ['year', 'games', 'olympic games']
    event_patterns = ['event', 'sport', 'discipline']
    medal_patterns = ['medal', 'type']

    for i, h in enumerate(headers_lower):
        if col_indices['athlete'] is None and any(p in h for p in athlete_patterns):
            col_indices['athlete'] = i

        if col_indices['country'] is None and any(p in h for p in country_patterns):
            col_indices['country'] = i

        if col_indices['year'] is None and any(p in h for p in year_patterns):
            col_indices['year'] = i

        if col_indices['event'] is None and any(p in h for p in event_patterns):
            col_indices['event'] = i

        if col_indices['medal'] is None and any(p in h for p in medal_patterns):
            col_indices['medal'] = i

    return col_indices


def detect_podium_columns(headers: List[str]) -> Tuple[Optional[int], Dict[str, int]]:
    """Detect wide-format medal table columns: Games | Gold | Silver | Bronze."""
    headers_lower = [h.lower().strip() for h in headers]
    games_idx = None
    medal_cols: Dict[str, int] = {}

    for i, h in enumerate(headers_lower):
        if games_idx is None and ("games" in h or "year" in h):
            games_idx = i
        if "gold" in h:
            medal_cols["Gold"] = i
        elif "silver" in h:
            medal_cols["Silver"] = i
        elif "bronze" in h:
            medal_cols["Bronze"] = i

    return games_idx, medal_cols


def parse_medal_cell(cell) -> Tuple[str, str]:
    """Parse athlete/team and country from a podium cell."""
    links = [a.get_text(strip=True) for a in cell.find_all("a")]
    links = [l for l in links if l and l.lower() != "details"]

    if len(links) >= 2:
        country = links[-1]
        athlete = ", ".join(links[:-1]).strip()
        return athlete, country

    text = cell.get_text(" ", strip=True)
    if not text:
        return "", ""
    return text, ""


def extract_year_and_season(year_text: str) -> Tuple[Optional[int], Optional[str]]:
    """Extract Olympic year and season (Summer/Winter) from text."""
    if not year_text:
        return None, None

    year = None
    season = None

    year_match = re.search(r'\b(19\d{2}|20\d{2})\b', year_text)
    if year_match:
        year = int(year_match.group(1))

    if 'summer' in year_text.lower():
        season = 'Summer'
    elif 'winter' in year_text.lower():
        season = 'Winter'

    return year, season


def detect_gender(event_text: str) -> str:
    """Detect gender category from event name."""
    if not event_text:
        return 'Mixed'

    event_lower = event_text.lower()

    if "men's" in event_lower or " men " in event_lower or event_lower.startswith("men"):
        return 'Men'
    if "women's" in event_lower or " women " in event_lower or event_lower.startswith("women"):
        return 'Women'
    if 'mixed' in event_lower:
        return 'Mixed'

    return 'Mixed'


def scrape_sport(sport_name: str, url: str) -> List[Dict]:
    """Scrape medal data from a sport-specific Wikipedia page."""
    logger.info(f"Scraping {sport_name}...")
    soup = fetch_page(url)
    if not soup:
        return []

    medals = []
    tables = soup.find_all('table', {'class': 'wikitable'})

    if not tables:
        logger.warning(f"No wikitables found on {sport_name} page")
        return []

    logger.info(f"Found {len(tables)} tables on {sport_name} page")

    for table_idx, table in enumerate(tables):
        try:
            rows = table.find_all('tr')
            if len(rows) < 2:
                continue

            headers = [cell.get_text(strip=True) for cell in rows[0].find_all(['th', 'td'])]
            if len(headers) < 2:
                continue

            table_caption = table.find("caption")
            event_from_table = table_caption.get_text(" ", strip=True) if table_caption else "Unknown"

            # Handle wide podium tables (Games | Gold | Silver | Bronze)
            games_idx, podium_cols = detect_podium_columns(headers)
            if games_idx is not None and podium_cols:
                max_idx = max([games_idx] + list(podium_cols.values()))
                for row in rows[1:]:
                    cell_elements = row.find_all(['td', 'th'])
                    if len(cell_elements) <= max_idx:
                        continue

                    games_text = cell_elements[games_idx].get_text(" ", strip=True)
                    year, season = extract_year_and_season(games_text)

                    for medal_type, medal_idx in podium_cols.items():
                        athlete, country = parse_medal_cell(cell_elements[medal_idx])
                        if not athlete or not country:
                            continue

                        medals.append({
                            'athlete_name': athlete,
                            'country_code': country,
                            'year': year,
                            'season': season or 'Unknown',
                            'sport': sport_name,
                            'event_name': event_from_table,
                            'gender': detect_gender(event_from_table),
                            'medal_type': medal_type
                        })
                continue

            col_map = detect_columns(headers)
            if col_map['athlete'] is None or col_map['country'] is None or col_map['medal'] is None:
                continue

            max_idx = max(i for i in col_map.values() if i is not None)

            for row_idx, row in enumerate(rows[1:], start=1):
                cells = [cell.get_text(strip=True) for cell in row.find_all(['td', 'th'])]
                if len(cells) < max_idx + 1:
                    continue

                try:
                    athlete = cells[col_map['athlete']].strip()
                    country = cells[col_map['country']].strip()
                    medal = cells[col_map['medal']].strip()

                    year = None
                    season = None
                    if col_map['year'] is not None:
                        year_text = cells[col_map['year']].strip()
                        year, season = extract_year_and_season(year_text)

                    event = cells[col_map['event']].strip() if col_map['event'] is not None else None

                    if not athlete or not country or not medal:
                        continue

                    if medal.lower() not in ['gold', 'silver', 'bronze']:
                        continue

                    gender = detect_gender(event) if event else 'Mixed'

                    medals.append({
                        'athlete_name': athlete,
                        'country_code': country,
                        'year': year,
                        'season': season or 'Unknown',
                        'sport': sport_name,
                        'event_name': event or 'Unknown',
                        'gender': gender,
                        'medal_type': medal.capitalize()
                    })

                except (IndexError, ValueError) as e:
                    logger.debug(f"Error in {sport_name} table {table_idx} row {row_idx}: {e}")
                    continue

        except Exception as e:
            logger.warning(f"Error processing table {table_idx} in {sport_name}: {e}")
            continue

    logger.info(f"Extracted {len(medals)} medals from {sport_name}")
    return medals


def save_csv(data: List[Dict], filename: str) -> None:
    """Save data to CSV file."""
    if not data:
        logger.warning(f"Skipping {filename} - no data")
        return

    filepath = os.path.join(OUTPUT_DIR, filename)
    pd.DataFrame(data).to_csv(filepath, index=False)
    logger.info(f"Saved {len(data)} rows to {filepath}")


def main() -> None:
    """Main scraping orchestration."""
    logger.info("=" * 60)
    logger.info("OLYMPIC MEDALS SCRAPER STARTED")
    logger.info("=" * 60)

    sports = get_sport_links()
    if not sports:
        logger.error("No sports found")
        return

    logger.info(f"Found {len(sports)} sports to scrape")

    all_medals = []
    sport_counts = {}

    for idx, (sport_name, url) in enumerate(sports, start=1):
        logger.info(f"[{idx}/{len(sports)}] Processing {sport_name}...")
        medals = scrape_sport(sport_name, url)

        if medals:
            all_medals.extend(medals)
            sport_counts[sport_name] = len(medals)
            save_csv(medals, f"{sport_name.replace(' ', '_')}.csv")
        else:
            sport_counts[sport_name] = 0

        time.sleep(1)

    logger.info("=" * 60)
    if all_medals:
        save_csv(all_medals, "all_medals.csv")
        logger.info(f"TOTAL MEDALS SCRAPED: {len(all_medals)}")
        logger.info("Breakdown by sport:")
        for sport, count in sorted(sport_counts.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {sport}: {count}")
    else:
        logger.error("No medals scraped")

    logger.info("SCRAPER COMPLETED")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
