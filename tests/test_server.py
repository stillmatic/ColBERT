from ..server import app, SearchRequest, SearchResponse

async def test_search_request() -> None:
    test_client = app.test_client()
    response = await test_client.post("/todos/", json=SearchRequest(query="Abc", k=10))
    data = await response.get_json()
    assert data == {"id": 1, "task": "Abc", "due": None}