import typing


class InvalidApiKeysError(ValueError):
    def __init__(self, account_id: str):
        super(InvalidApiKeysError, self).__init__()
        self.account_id: str = account_id


class WrongAccountError(ValueError):
    def __init__(self, account_id: str):
        super(WrongAccountError, self).__init__()
        self.account_id: str = account_id


class WrongOrderError(ValueError):
    def __init__(self, order_id: typing.Optional[str]):
        super(WrongOrderError, self).__init__()
        self.order_id: typing.Optional[str] = order_id
