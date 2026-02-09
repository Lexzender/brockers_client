import json
from urllib.parse import urlparse

import httpx


class MailApi:

    def __init__(self, base_url: str = "http://185.185.143.231:8085") -> None:
        self._base_url = base_url
        self._client = httpx.Client(base_url=self._base_url)

    def find_message(self, query: str) -> httpx.Response:
        params = {"query": query,
                  "limit": 1,
                  "kind": "containing",
                  "start": 0}
        response = self._client.get("/mail/mail/search", params=params)
        print(response.content)
        return response


    def extract_confirmation_id(self, query: str) -> str:
        response = self.find_message(query=query)
        data = response.json()
        items = data.get("items", [])
        body = items[0]["Content"]["Body"]
        body_data = json.loads(body)
        confirmation_url = body_data["ConfirmationLinkUrl"]
        path = urlparse(confirmation_url).path.rstrip("/")
        confirmation_id = path.split("/")[-1]
        if not confirmation_id:
            raise ValueError("Confirmation id not found")
        return confirmation_id
