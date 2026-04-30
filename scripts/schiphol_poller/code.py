"""Quick smoke test for Schiphol fetching/filtering/normalization."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from poller import fetch_window, is_codeshare, is_filtered_service, normalize

count_total = 0
count_kept = 0
shown = 0

for raw in fetch_window():
    count_total += 1
    if is_codeshare(raw) or is_filtered_service(raw):
        continue
    event = normalize(raw)
    if event is None:
        continue
    count_kept += 1
    if shown < 3:
        print(f"--- Sample {shown + 1} ---")
        print(event.to_dict())
        print()
        shown += 1
    if count_total > 60:  # cap per test rapido
        break

print(f"Total seen: {count_total}, kept: {count_kept}")