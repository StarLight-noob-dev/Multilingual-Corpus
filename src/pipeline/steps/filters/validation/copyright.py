from typing import override

from src.models.record import EditionRecord
from src.pipeline.steps import BaseFilter


class EditionAnyCopyrightFilter(BaseFilter):
    """
    Filter to include records that are either copyrighted or not copyrighted.

    This filter includes all records regardless of their copyright status.
    """

    def __init__(self, skip_warning: bool = False):

        from time import sleep
        from colorama import Fore, Style, init
        init(autoreset=True)
        print(
            f"""
        {Fore.YELLOW}[WARNING]{Style.RESET_ALL}: {Fore.RED}EditionAnyCopyrightFilter includes all records regardless of copyright status.{Style.RESET_ALL}
        \t{Fore.YELLOW}This may lead to legal issues{Style.RESET_ALL} if copyrighted works are processed without proper authorization.
        \tEnsure that you have the {Fore.GREEN}necessary rights{Style.RESET_ALL} to process and distribute copyrighted materials.

        \t{Fore.CYAN}This filter is intended for testing or special use cases only.{Style.RESET_ALL}
        """
        )
        if not skip_warning:
            sleep(3)

    @override
    def filter(self, data: EditionRecord) -> bool:
        """
        Include all records regardless of copyright status.

        Args:
            data (EditionRecord): The edition record to be evaluated.

        Returns:
            bool: Always returns True to include all records.
        """
        return True