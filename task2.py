# multiprocessing_search.py
# keyword search in txt files using multiprocessing and Queue

import os
import time
from multiprocessing import Process, Queue, cpu_count
from typing import Dict, List, Tuple


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


def split_chunks(items: List[str], n: int) -> List[List[str]]:
    n = max(1, n)
    return [items[i::n] for i in range(n)]


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


def worker(files: List[str], keywords_lower: List[str], q: Queue) -> None:
    for fp in files:
        for kw in scan_file(fp, keywords_lower):
            q.put((kw, fp))
    q.put(None)


def search_multiprocessing(path: str, keywords: List[str], workers: int) -> Dict[str, List[str]]:
    files = get_files(path)
    k_low = [k.lower() for k in keywords]
    back = {k.lower(): k for k in keywords}
    result: Dict[str, List[str]] = {k: [] for k in keywords}

    q: Queue = Queue()
    chunks = split_chunks(files, workers)
    procs = [Process(target=worker, args=(chunk, k_low, q)) for chunk in chunks]

    for p in procs:
        p.start()

    done = 0
    while done < len(procs):
        item = q.get()
        if item is None:
            done += 1
        else:
            kw, fp = item  # type: ignore[misc]
            result[back[kw]].append(fp)

    for p in procs:
        p.join()

    for k in result:
        result[k] = sorted(set(result[k]))

    return result


if __name__ == "__main__":
    path = input("path to folder/file: ").strip()
    keywords = [x.strip() for x in input("keywords (comma): ").split(",") if x.strip()]
    workers_raw = input("processes (empty = cpu-1): ").strip()
    workers = int(workers_raw) if workers_raw else max(1, cpu_count() - 1)

    t0 = time.perf_counter()
    res = search_multiprocessing(path, keywords, workers)
    dt = time.perf_counter() - t0

    print(f"time: {dt:.6f}s")
    print(res)
