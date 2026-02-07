import httpx


class AccountApi:

    def __init__(self, base_url: str = "http://185.185.143.231:8085") -> None:
        self._base_url = base_url
        self._client = httpx.Client(base_url=self._base_url)

    def register_user(self, login: str, email: str, password: str) -> httpx.Response:
        data = {"login": login, "email": email, "password": password}
        response =  self._client.post("/register/user/async-register", json=data)
        print(response.content)
        return response

    def activate_user(self, token: str, login: str) -> httpx.Response:
        params = {"token": token}
        response = self._client.put("/register/user/activate", params=params)
        if response.status_code != 200:
            raise AssertionError(f"Expected 200, got {response.status_code}")
        response_data = response.json()
        response_login = response_data.get("resource", {}).get("login")
        if response_login != login:
            raise AssertionError(f"Expected login {login}, got {response_login}")
        print(response.content)
        return response

