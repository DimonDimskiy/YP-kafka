import faust


class Message(faust.Record):
    sender: str
    recipient: str
    value: str

class BanUser(faust.Record):
    initiator: str
    target: str
    unblock: bool

class ForbiddenWord(faust.Record):
    value: str
    unblock: bool