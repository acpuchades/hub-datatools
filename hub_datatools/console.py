import colorama
import logging
from colorama import Fore, Style
from logging import Formatter, StreamHandler


class CustomFormatter(Formatter):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def format(self, record):
        match record.levelno:

            case logging.INFO:
                record.levelname = Fore.GREEN + record.levelname + Style.RESET_ALL

            case logging.WARNING:
                record.levelname = Fore.YELLOW + record.levelname + Style.RESET_ALL

            case logging.ERROR | logging.CRITICAL:
                record.levelname = Fore.RED + record.levelname + Style.RESET_ALL

        return super().format(record)


def initialize() -> None:
    colorama.init()

    fmt = '[%(levelname)s] %(message)s'
    stdout_handler = StreamHandler()
    stdout_handler.setFormatter(CustomFormatter(fmt))

    logger = logging.getLogger()
    logger.addHandler(stdout_handler)
