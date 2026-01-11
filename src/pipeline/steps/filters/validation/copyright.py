from datetime import date
from typing import Callable, override

from src.models.record import EditionRecord
from src.pipeline.steps import BaseFilter
from src.repositories import AuthorRepository


class EditionCopyrightFilter(BaseFilter):
    """
    Filter to include or exclude data based on copyright status.

    By default, it includes only records that are not copyrighted.
    It can also be configured to exclude non-copyrighted records.
    """

    def __init__(self,
                 copyright_function: Callable[[EditionRecord, AuthorRepository], bool] = None,
                 include_copyrighted: bool = False,
                 years_post_mortem_to_copyright: int = 70, # § 64 UrhG
                 repository: AuthorRepository = None):
        """
        Initialize the CopyrightFilter.

        If no copyright_function is provided, the default logic based on German law (§ 64 UrhG) is used.

        Args:
            copyright_function (Callable[[EditionRecord, AuthorRepository], bool): A function that determines the
                copyright status of a record, returning True if the record has copyright and False otherwise.
            include_copyrighted (bool): If True, include only copyrighted records; if False, exclude them.
            years_post_mortem_to_copyright (int): Number of years after author's death until copyright expires.
            repository (AuthorRepository): Repository to fetch author data for copyright determination.
        """
        self.copyright_function = copyright_function or self._is_copyrighted
        self.include_copyrighted = include_copyrighted
        self.copyright_window_years = years_post_mortem_to_copyright
        self.repository = repository
        self.current_year = date.today().year

    @override
    def filter(self, data: EditionRecord) -> bool:
        """
        Filter the data based on its copyright status.
        This method uses the provided copyright function to determine if the record is copyrighted or not.

        Args:
            data (EditionRecord): The edition record to be evaluated.

        Returns:
            bool: True if the record should be included based on the filter criteria, False otherwise. The inclusion
                    depends on whether we are including or excluding copyrighted records.
        """
        if data is None:
            return False
        return self.copyright_function(data, self.repository) == self.include_copyrighted

    def _is_copyrighted(self, data: EditionRecord, repo: AuthorRepository) -> bool:
        """Default copyright determination logic based on German law (§ 64 UrhG)."""
        if not repo:
            return True  # Assume copyrighted if no repository is provided

        authors = repo.get_many_by_ids(data.authors)
        last_author_dead_year = max(
            (a.death_date for a in authors if a and a.death_date),
            default=None
        )

        # Author is known — apply copyright window after death (§ 64 UrhG)
        if last_author_dead_year and last_author_dead_year > 0:
            expiry_year = last_author_dead_year + self.copyright_window_years
            return self.current_year <= expiry_year

        '''
        TODO: It's possible to have an author but not their actual death year because it couldn't be determined.
        In such cases, we might want to implement additional logic, such as checking for their birth year or other
        relevant data to make a more informed decision about the copyright status.
        
        For now, we will skip this and default to the anonymous work rule below.
        '''

        # Anonymous/pseudonymous works — apply copyright window after publication (§ 66 UrhG)
        publication_year = data.publishing_date or None
        if publication_year and publication_year > 0:
            expiry_year = publication_year + self.copyright_window_years
            return self.current_year <= expiry_year

        # Default to copyrighted if no conditions met
        return True


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