import logging

from websockets import server as sws

from .conn import AbstractHBIC

logger = logging.getLogger(__name__)


class SWSHBIC(AbstractHBIC, sws.WebSocketServerProtocol):
    """
    Server WebSocket support
    """

    @classmethod
    def create_server(cls):
        pass

    def __init__(self, ctx, **kwargs):
        super().__init__(ctx=ctx, **kwargs)
