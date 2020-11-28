class BitmexBaseModel:
    def to_json(self) -> str:
        raise NotImplementedError()

    def update(self, update: 'BitmexBaseModel') -> 'BitmexBaseModel':
        for attr, val in update.__dict__.items():
            self.__dict__[attr] = val or self.__dict__[attr]
        return self
