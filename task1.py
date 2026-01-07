# threading_search.py
# keyword search in txt files using threading

import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List


def get_files(path: str) -> List[str]:
    path = os.path.abspath(path)
    if os.path.isfile(path):
        return [path]
    files = []
    for root, _, names in os.walk(path):
        for n in names:
            if n.lower().endswith(".txt"):
                files.append(os.path.join(root, n))
    return files


def scan_file(file_path: str, keywords_lower: List[str]) -> List[str]:
    found = set()
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                s = line.lower()
                for kw in keywords_lower:
                    if kw in s:
                        found.add(kw)
                if len(found) == len(keywords_lower):
                    break
    except OSError:
        return []
    return list(found)


def search_threading(path: str, keywords: List[str], workers: int = 8) -> Dict[str, List[str]]:
    files = get_files(path)
    k_low = [k.lower() for k in keywords]
    back = {k.lower(): k for k in keywords}
    result: Dict[str, List[str]] = {k: [] for k in keywords}

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for file_path, matched in zip(files, ex.map(lambda p: scan_file(p, k_low), files)):
            for kw in matched:
                result[back[kw]].append(file_path)

    for k in result:
        result[k].sort()

    return result


if __name__ == "__main__":
    path = input("path to folder/file: ").strip()
    keywords = [x.strip() for x in input("keywords (comma): ").split(",") if x.strip()]
    workers = int(input("threads: ").strip() or "8")

    t0 = time.perf_counter()
    res = search_threading(path, keywords, workers)
    dt = time.perf_counter() - t0

    print(f"time: {dt:.6f}s")
    print(res)
