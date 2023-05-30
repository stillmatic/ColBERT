from quart import Quart, request
from functools import lru_cache
import heapq
import math
import os
from dotenv import load_dotenv
from dataclasses import dataclass
from datetime import datetime

from quart_schema import QuartSchema, validate_request, validate_response
from colbert.infra import Run, RunConfig, ColBERTConfig
from colbert import Searcher

load_dotenv()

INDEX_NAME = os.getenv("INDEX_NAME")
INDEX_ROOT = os.getenv("INDEX_ROOT")
app = Quart(__name__)

searcher = Searcher(index=f"{INDEX_ROOT}/{INDEX_NAME}")
counter = {"api" : 0}


@lru_cache(maxsize=1000000)
async def _api_search_query(query, k):
    print(f"Query={query}")
    if k is None: k = 10
    # do not allow more than 100 results
    k = min(int(k), 100)
    pids, ranks, scores = searcher.search(query, k=k)
    
    # pretty sure this is not used, unless it is to warm the cache
    # passages = [searcher.collection[pid] for pid in pids]
    probs = [math.exp(score) for score in scores]
    probs = [prob / sum(probs) for prob in probs]

    topk = []
    for pid, rank, score, prob in zip(pids, ranks, scores, probs):
        text = searcher.collection[pid]
        # d = {'text': text, 'pid': pid, 'rank': rank, 'score': score, 'prob': prob}

        # If we have not yet found k elements, or this element is greater than the smallest element on the heap,
        if len(topk) < k or topk[0]['score'] < score:
            # If heap is full, remove the smallest element
            if len(topk) == k:
                heapq.heappop(topk)
            # Insert the new element
            heapq.heappush(topk, {'score': -score, 'pid': pid, 'text': text, 'rank': rank, 'prob': prob})

    topk = [{'text':i['text'], 'pid':i['pid'], 'rank':i['rank'], 'score': -i['score'], 'prob':i['prob']} for i in heapq.nsmallest(k, topk, key=lambda p: p['score'])]

    return {"query" : query, "topk": topk}

@dataclass
class SearchRequest:
    query: str
    k: int

@dataclass
class SearchResponse:
    query: str
    topk: list

@app.route("/api/search", methods=["GET"])
@validate_request(SearchRequest)
@validate_response(SearchResponse)
async def api_search():
    if request.method == "GET":
        counter["api"] += 1
        print("API request count:", counter["api"])
        return _api_search_query(request.args.get("query"), request.args.get("k"))
    else:
        return ('', 405)

if __name__ == "__main__":
    port = int(os.getenv("PORT") or "8000")
    app.run("0.0.0.0", port=port)
