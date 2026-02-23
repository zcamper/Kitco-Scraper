import asyncio
import re
from datetime import datetime, timezone
from urllib.parse import quote_plus, urljoin, urlparse

from apify import Actor
from bs4 import BeautifulSoup
from curl_cffi.requests import Session

SITE_HOST = 'online.kitco.com'
SITE_HOSTS = {'online.kitco.com', 'kitco.com', 'www.kitco.com'}
BASE_URL = 'https://online.kitco.com'
# Kitco uses AJAX endpoints for category listings
CATEGORY_AJAX_MAP = {
    'silver': f'{BASE_URL}/silver-ajax',
    'gold': f'{BASE_URL}/gold-ajax',
    'platinum': f'{BASE_URL}/platinum-ajax',
    'palladium': f'{BASE_URL}/palladium-ajax',
}
AVAILABILITY_STATES = ['In Stock', 'Out of Stock', 'Pre-Order', 'Sold Out', 'Coming Soon', 'Discontinued']
MAX_DESCRIPTION_LENGTH = 2000
SKIP_PATH_SEGMENTS = ['/about', '/contact', '/faq', '/help', '/blog', '/account', '/cart', '/checkout', '/shipping', '/privacy', '/terms']

products_scraped = 0
scraped_urls: set[str] = set()


def parse_price(price_str: str) -> float | None:
    if not price_str:
        return None
    match = re.search(r'\$?([\d,]+\.?\d*)', price_str)
    if match:
        try:
            return float(match.group(1).replace(',', ''))
        except ValueError:
            return None
    return None


def validate_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        host = parsed.hostname or ''
        return parsed.scheme in ('http', 'https') and host in SITE_HOSTS
    except Exception:
        return False


def is_search_url(url: str) -> bool:
    return '/search' in url or '?q=' in url


def is_product_url(url: str) -> bool:
    if not validate_url(url):
        return False
    parsed = urlparse(url)
    path = parsed.path.strip('/')
    if not path:
        return False
    # Kitco products: /buy/{id}/{slug}
    if path.startswith('buy/'):
        return True
    return False


def is_category_url(url: str) -> bool:
    if is_search_url(url):
        return False
    if is_product_url(url):
        return False
    parsed = urlparse(url)
    path = parsed.path.lower().strip('/')
    if not path:
        return True
    categories = ('silver', 'gold', 'platinum', 'palladium', 'refinery')
    for cat in categories:
        if path == cat or path.startswith(f'{cat}/'):
            return True
    return False


def extract_ajax_products(html: str) -> list[dict]:
    """Extract products from a Kitco AJAX category response."""
    soup = BeautifulSoup(html, 'html.parser')
    products = []
    seen = set()

    for card in soup.select('div.product-card, div[itemscope][itemtype*="Product"]'):
        # URL
        link_el = card.select_one('a[href*="/buy/"], a.product-title-link')
        if not link_el:
            continue
        url = link_el.get('href', '')
        if url and not url.startswith('http'):
            url = f"{BASE_URL}{url}"
        if url in seen:
            continue
        seen.add(url)

        # Name
        name_el = card.select_one('[itemprop="name"], .headline_product, .product-title-link')
        name = name_el.get_text(strip=True) if name_el else link_el.get_text(strip=True)

        # Price — from data attribute or itemprop
        price_text = None
        price_numeric = None
        price_attr = card.get('data-price', '')
        if price_attr:
            try:
                price_numeric = float(price_attr)
                price_text = f"${price_numeric:,.2f}"
            except (ValueError, TypeError):
                pass
        if not price_text:
            price_el = card.select_one('[itemprop="price"], .item_unit_price, .product-price')
            if price_el:
                content = price_el.get('content', '')
                if content:
                    try:
                        price_numeric = float(content)
                        price_text = f"${price_numeric:,.2f}"
                    except ValueError:
                        pass
                if not price_text:
                    price_text = price_el.get_text(strip=True)
                    price_numeric = parse_price(price_text)

        # Image
        img_el = card.select_one('[itemprop="image"], .product-img img, img')
        image = None
        if img_el:
            image = img_el.get('src') or img_el.get('data-src') or img_el.get('content')
            if image and not image.startswith('http'):
                image = f"{BASE_URL}{image}"

        # SKU / product ID
        pid = card.get('data-pid', '')

        # Description
        desc_el = card.select_one('.product-description')
        description = desc_el.get_text(strip=True)[:MAX_DESCRIPTION_LENGTH] if desc_el else None

        if name:
            products.append({
                'url': url,
                'name': name,
                'price': price_text,
                'priceNumeric': price_numeric,
                'image': image,
                'sku': str(pid) if pid else None,
                'description': description,
                'availability': 'In Stock',
            })

    return products


def extract_product_details(html: str) -> dict:
    soup = BeautifulSoup(html, 'html.parser')

    h1 = soup.select_one('h1')
    name = h1.get_text(strip=True) if h1 else None

    price_text = None
    price_numeric = None

    price_el = soup.select_one('[itemprop="price"], .item_unit_price, .product-price, #product-price')
    if price_el:
        content = price_el.get('content', '')
        if content:
            try:
                price_numeric = float(content)
                price_text = f"${price_numeric:,.2f}"
            except ValueError:
                pass
        if not price_text:
            price_text = price_el.get_text(strip=True)
            price_numeric = parse_price(price_text)

    og_image = soup.select_one('meta[property="og:image"]')
    image_url = og_image.get('content') if og_image else None
    if not image_url:
        img_el = soup.select_one('.product-image img, .gallery img, [itemprop="image"]')
        if img_el:
            image_url = img_el.get('src') or img_el.get('content')
            if image_url and not image_url.startswith('http'):
                image_url = f"{BASE_URL}{image_url}"

    sku = None
    sku_el = soup.select_one('[itemprop="sku"], .product-sku')
    if sku_el:
        sku = sku_el.get('content') or sku_el.get_text(strip=True)

    availability = "In Stock"
    avail_el = soup.select_one('[itemprop="availability"]')
    if avail_el:
        avail_text = avail_el.get('content', '') or avail_el.get('href', '') or avail_el.get_text()
        if 'OutOfStock' in avail_text:
            availability = "Out of Stock"
        elif 'PreOrder' in avail_text:
            availability = "Pre-Order"

    if availability == "In Stock":
        page_text = soup.get_text()
        if 'Out of Stock' in page_text or 'Sold Out' in page_text:
            availability = 'Out of Stock'

    desc_el = soup.select_one('.product-description, [itemprop="description"], .product-details')
    description = desc_el.get_text(strip=True)[:MAX_DESCRIPTION_LENGTH] if desc_el else None

    return {
        'name': name,
        'price': price_text if price_text and '$' in str(price_text) else None,
        'priceNumeric': price_numeric if price_numeric else parse_price(price_text) if price_text else None,
        'imageUrl': image_url,
        'sku': sku,
        'availability': availability,
        'description': description,
    }


def init_session(proxies: dict) -> Session:
    http = Session(impersonate="chrome110")
    home_resp = http.get(f"{BASE_URL}/", proxies=proxies, timeout=30)
    Actor.log.info(f"Homepage warm-up: status={home_resp.status_code}, cookies={len(http.cookies)}")
    if home_resp.status_code != 200:
        Actor.log.warning(f"Homepage returned {home_resp.status_code}, scraping may fail")
    http.headers.update({'Referer': f'{BASE_URL}/'})
    return http


async def scrape_category_ajax(http: Session, metal: str, proxies: dict, max_items: int) -> None:
    """Scrape products from Kitco's category AJAX endpoints."""
    global products_scraped

    ajax_url = CATEGORY_AJAX_MAP.get(metal)
    if not ajax_url:
        Actor.log.warning(f"No AJAX endpoint for metal: {metal}")
        return

    Actor.log.info(f"Fetching category via AJAX: {ajax_url}")
    try:
        resp = http.post(ajax_url, proxies=proxies, timeout=30,
                        headers={'X-Requested-With': 'XMLHttpRequest'})
    except Exception as e:
        Actor.log.error(f"Failed to fetch category AJAX {ajax_url}: {e}")
        return

    if resp.status_code != 200:
        Actor.log.warning(f"Category AJAX returned {resp.status_code}")
        return

    products = extract_ajax_products(resp.text)
    Actor.log.info(f"Found {len(products)} products in {metal} category")

    for product in products:
        if products_scraped >= max_items:
            break

        prod_url = product['url'].rstrip('/')
        if prod_url in scraped_urls:
            continue
        scraped_urls.add(prod_url)

        # Fetch full product page for details
        try:
            prod_resp = http.get(prod_url, proxies=proxies, timeout=30)
            if prod_resp.status_code == 200:
                details = extract_product_details(prod_resp.text)
                await Actor.push_data({
                    'url': prod_url,
                    'name': details['name'] or product.get('name', ''),
                    'price': details['price'] or product.get('price'),
                    'priceNumeric': details['priceNumeric'] or product.get('priceNumeric'),
                    'imageUrl': details['imageUrl'] or product.get('image'),
                    'sku': details['sku'] or product.get('sku'),
                    'availability': details['availability'],
                    'description': details['description'] or product.get('description'),
                    'scrapedAt': datetime.now(timezone.utc).isoformat(),
                })
            else:
                Actor.log.warning(f"Product page returned {prod_resp.status_code}, using listing data")
                await Actor.push_data({
                    'url': prod_url,
                    'name': product.get('name', ''),
                    'price': product.get('price'),
                    'priceNumeric': product.get('priceNumeric'),
                    'imageUrl': product.get('image'),
                    'sku': product.get('sku'),
                    'availability': product.get('availability', 'In Stock'),
                    'description': product.get('description'),
                    'scrapedAt': datetime.now(timezone.utc).isoformat(),
                })
        except Exception as e:
            Actor.log.warning(f"Failed to fetch product {prod_url}: {e}")
            await Actor.push_data({
                'url': prod_url,
                'name': product.get('name', ''),
                'price': product.get('price'),
                'priceNumeric': product.get('priceNumeric'),
                'imageUrl': product.get('image'),
                'sku': product.get('sku'),
                'availability': product.get('availability', 'In Stock'),
                'description': product.get('description'),
                'scrapedAt': datetime.now(timezone.utc).isoformat(),
            })

        products_scraped += 1
        Actor.log.info(f"Scraped {products_scraped}/{max_items} products")


async def scrape_search(http: Session, query: str, proxies: dict, max_items: int) -> None:
    """Search by finding matching products from the silver/gold AJAX categories."""
    global products_scraped
    query_lower = query.lower()

    # Determine which categories to search based on query
    metals_to_search = []
    if any(w in query_lower for w in ('silver', 'coin', 'eagle', 'maple', 'round', 'bar')):
        metals_to_search.append('silver')
    if any(w in query_lower for w in ('gold',)):
        metals_to_search.append('gold')
    if any(w in query_lower for w in ('platinum',)):
        metals_to_search.append('platinum')
    if any(w in query_lower for w in ('palladium',)):
        metals_to_search.append('palladium')
    if not metals_to_search:
        metals_to_search = ['silver']

    for metal in metals_to_search:
        if products_scraped >= max_items:
            break

        ajax_url = CATEGORY_AJAX_MAP.get(metal)
        if not ajax_url:
            continue

        Actor.log.info(f"Searching '{query}' in {metal} category")
        try:
            resp = http.post(ajax_url, proxies=proxies, timeout=30,
                            headers={'X-Requested-With': 'XMLHttpRequest'})
        except Exception as e:
            Actor.log.error(f"Failed to fetch category AJAX {ajax_url}: {e}")
            continue

        if resp.status_code != 200:
            continue

        all_products = extract_ajax_products(resp.text)

        # Filter products by search query keywords
        keywords = [w for w in query_lower.split() if len(w) > 2]
        matched = []
        for p in all_products:
            name_lower = p['name'].lower()
            if any(kw in name_lower for kw in keywords):
                matched.append(p)

        # If no keyword match, use all products from the category
        if not matched:
            matched = all_products

        Actor.log.info(f"Found {len(matched)} products matching '{query}' in {metal}")

        for product in matched:
            if products_scraped >= max_items:
                break

            prod_url = product['url'].rstrip('/')
            if prod_url in scraped_urls:
                continue
            scraped_urls.add(prod_url)

            try:
                prod_resp = http.get(prod_url, proxies=proxies, timeout=30)
                if prod_resp.status_code == 200:
                    details = extract_product_details(prod_resp.text)
                    await Actor.push_data({
                        'url': prod_url,
                        'name': details['name'] or product.get('name', ''),
                        'price': details['price'] or product.get('price'),
                        'priceNumeric': details['priceNumeric'] or product.get('priceNumeric'),
                        'imageUrl': details['imageUrl'] or product.get('image'),
                        'sku': details['sku'] or product.get('sku'),
                        'availability': details['availability'],
                        'description': details['description'] or product.get('description'),
                        'scrapedAt': datetime.now(timezone.utc).isoformat(),
                    })
                else:
                    await Actor.push_data({
                        'url': prod_url,
                        'name': product.get('name', ''),
                        'price': product.get('price'),
                        'priceNumeric': product.get('priceNumeric'),
                        'imageUrl': product.get('image'),
                        'sku': product.get('sku'),
                        'availability': 'In Stock',
                        'description': product.get('description'),
                        'scrapedAt': datetime.now(timezone.utc).isoformat(),
                    })
            except Exception as e:
                Actor.log.warning(f"Failed to fetch product {prod_url}: {e}")

            products_scraped += 1
            Actor.log.info(f"Scraped {products_scraped}/{max_items} products")


async def scrape_product(http: Session, url: str, proxies: dict, max_items: int) -> None:
    global products_scraped
    if products_scraped >= max_items:
        return

    url = url.rstrip('/')
    if url in scraped_urls:
        return
    scraped_urls.add(url)

    Actor.log.info(f"Fetching product ({products_scraped + 1}/{max_items}): {url}")
    try:
        response = http.get(url, proxies=proxies, timeout=30)
    except Exception as e:
        Actor.log.error(f"Failed to fetch product {url}: {e}")
        return

    if response.status_code != 200:
        Actor.log.warning(f"Non-200 status ({response.status_code}) for product {url}")
        return

    details = extract_product_details(response.text)
    await Actor.push_data({
        'url': url,
        'name': details['name'],
        'price': details['price'],
        'priceNumeric': details['priceNumeric'],
        'imageUrl': details['imageUrl'],
        'sku': details['sku'],
        'availability': details['availability'],
        'description': details['description'],
        'scrapedAt': datetime.now(timezone.utc).isoformat(),
    })

    products_scraped += 1
    Actor.log.info(f"Scraped {products_scraped}/{max_items} products")


async def main():
    global products_scraped

    async with Actor:
        actor_input = await Actor.get_input() or {}
        start_urls_input = actor_input.get("start_urls", [])
        search_terms = actor_input.get("search_terms", [])
        max_items = actor_input.get("max_items", 10)

        search_queries = []
        start_urls = []
        for term in search_terms:
            term = term.strip()
            if term:
                search_queries.append(term)
                Actor.log.info(f"Added search term: '{term}'")

        for item in start_urls_input:
            if isinstance(item, dict) and "url" in item:
                url = item["url"]
            elif isinstance(item, str):
                url = item
            else:
                continue
            if validate_url(url):
                start_urls.append(url)
            else:
                Actor.log.warning(f"Skipping non-Kitco URL: {url}")

        if not search_queries and not start_urls:
            default_term = "Silver coin"
            search_queries = [default_term]
            Actor.log.info(f"No input provided, defaulting to search: '{default_term}'")

        Actor.log.info(f"Starting Kitco Scraper with {len(search_queries)} search queries, {len(start_urls)} start URLs, max_items={max_items}")

        Actor.log.info("Configuring RESIDENTIAL proxy with US country")
        proxy_configuration = await Actor.create_proxy_configuration(
            actor_proxy_input={
                'useApifyProxy': True,
                'apifyProxyGroups': ['RESIDENTIAL'],
                'apifyProxyCountry': 'US',
            },
        )

        proxy_url = await proxy_configuration.new_url()
        proxies = {"http": proxy_url, "https": proxy_url}

        http = init_session(proxies)

        for query in search_queries:
            if products_scraped >= max_items:
                break
            await scrape_search(http, query, proxies, max_items)

        for url in start_urls:
            if products_scraped >= max_items:
                break
            if is_product_url(url):
                await scrape_product(http, url, proxies, max_items)
            elif is_category_url(url):
                path = urlparse(url).path.strip('/').split('/')[0].lower()
                if path in CATEGORY_AJAX_MAP:
                    await scrape_category_ajax(http, path, proxies, max_items)
            else:
                Actor.log.warning(f"Could not classify URL: {url}")

        Actor.log.info(f'Scraping completed. Total products scraped: {products_scraped}')


if __name__ == "__main__":
    asyncio.run(main())
