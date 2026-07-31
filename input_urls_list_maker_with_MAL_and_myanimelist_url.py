import requests
import re

URL = "https://raw.githubusercontent.com/srtfile/hianime_mal_id_verify_1-main/refs/heads/main/data/anime_data_part3.json"
OUTPUT_FILE = "input_urls_list_maker_with_MAL_and_myanimelist_url.txt"
ERROR_FILE = "input_urls_list_maker_with_MAL_and_myanimelist_url_skipped_items.txt"

def fetch_data(url):
    res = requests.get(url)
    res.raise_for_status()
    return res.json()

def process(data):
    serial = 1
    skipped = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out, \
         open(ERROR_FILE, "w", encoding="utf-8") as err:

        for item in data:
            anime_url = item.get("HiAnime_URL", "")
            total_eps = item.get("Total Episodes", "")
            mal_url = item.get("MAL_URL", "")
            anime_type = item.get("Type", "")

            reason = None

            # MAL_ID
            match_id = re.search(r"/anime/(\d+)", mal_url or "")
            if not match_id:
                reason = "Invalid MAL_URL"

            # slug
            match_slug = re.search(r"/anime/([^/?]+)", anime_url or "")
            if not match_slug and not reason:
                reason = "Invalid HiAnime_URL"

            # episodes
            try:
                total_eps_int = int(total_eps)
                if total_eps_int <= 0:
                    raise ValueError
            except:
                if anime_type.lower() == "movie":
                    total_eps_int = 1
                else:
                    reason = "Invalid episodes"

            if reason:
                skipped += 1
                err.write(f"ID {item.get('ID')} | {reason} | {anime_url} | {mal_url} | {total_eps} | {anime_type}\n")
                continue

            mal_id = match_id.group(1)
            slug = match_slug.group(1)

            line = f"{serial}. https://hianime.ad/watch/{slug}/ep-1 to {total_eps_int} | MAL_ID: {mal_id} | {mal_url}\n"
            out.write(line)

            serial += 1

    print(f"✅ Done: {serial-1} valid, {skipped} skipped")

def main():
    data = fetch_data(URL)
    process(data)

if __name__ == "__main__":
    main()
