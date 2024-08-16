# --*-- coding: utf-8 --*--
class DingoResponse():
    def __init__(self, status_code: int = 200, data="", text="", message="ok"):
        self.status_code = status_code
        self.data = data
        self.text = text
        self.message = message