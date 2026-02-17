from datetime import date
from typing import Callable, override, Any

from src.models.record import EditionRecord, CopyrightInfo, CopyrightStatus
from src.pipeline.steps import BaseTransformer
from src.repositories import AuthorRepository


class EditionCopyrightCalculation(BaseTransformer):
    """
    Calculates and annotates the copyright status of Edition records based on author data and publication date.

    This transformer uses either a provided copyright function or a default logic based on German law (§ 64 UrhG)
    to determine whether an edition record is copyrighted. It updates the `has_copyright` and
    `copyright_reason` fields of the EditionRecord accordingly.
    """

    def __init__(self,
                 copyright_function: Callable[[EditionRecord, AuthorRepository], EditionRecord] = None,
                 years_post_mortem_to_copyright: int = 70,  # § 64 UrhG
                 repository: AuthorRepository = None):
        """
        Initialize the Copyright Calculation Transformer.

        If no copyright_function is provided, the default logic based on German law (§ 64 UrhG) is used.

        Args:
            copyright_function (Callable[[EditionRecord, AuthorRepository], bool): A function that determines the
                copyright status of a record, returning True if the record has copyright and False otherwise.
            years_post_mortem_to_copyright (int): Number of years after author's death until copyright expires.
            repository (AuthorRepository): Repository to fetch author data for copyright determination.
        """
        self.copyright_function = copyright_function or self._is_copyrighted
        self.copyright_window_years = years_post_mortem_to_copyright
        self.repository = repository
        self.current_year = date.today().year

    @override
    def transform(self, data: EditionRecord) -> Any:
        """
        Transform the EditionRecord by determining its copyright status.

        Args:
            data (EditionRecord): The edition record to be evaluated.

        Returns:
            EditionRecord: The updated edition record with copyright status and reason.
        """
        if data is None:
            return data
        return self.copyright_function(data, self.repository)

    def _is_copyrighted(self, data: EditionRecord, repo: AuthorRepository) -> EditionRecord:
        """Default copyright determination logic based on German law (§ 64 UrhG)."""
        if repo:
            authors = repo.get_many_by_ids(data.authors)
            last_author_dead_year = max(
                (a.death_date.parsed_val for a in authors if a and a.death_date),
                default=None
            )

            # Author is known — apply copyright window after death (§ 64 UrhG)
            if last_author_dead_year and last_author_dead_year > 0:
                expiry_year = last_author_dead_year + self.copyright_window_years
                enough_time_passed = self.current_year > expiry_year
                info = CopyrightInfo(
                    status=CopyrightStatus.PUBLIC_DOMAIN if enough_time_passed else CopyrightStatus.IN_COPYRIGHT,
                    reason=f"Author died in {last_author_dead_year}, copyright expires in {expiry_year}." if not enough_time_passed else None
                )
                data.copyright = info
                return data

        '''
        TODO: It's possible to have an author but not their actual death year because it couldn't be determined.
        In such cases, we might want to implement additional logic, such as checking for their birth year or other
        relevant data to make a more informed decision about the copyright status.

        For now, we will skip this and default to the anonymous work rule below.
        '''

        # Anonymous/pseudonymous works — apply copyright window after publication (§ 66 UrhG)
        publication_year = data.publishing_date.parsed_val or None
        if publication_year and publication_year > 0:
            expiry_year = publication_year + self.copyright_window_years
            enough_time_passed = self.current_year > expiry_year
            info = CopyrightInfo(
                status=CopyrightStatus.PUBLIC_DOMAIN if enough_time_passed else CopyrightStatus.IN_COPYRIGHT,
                reason=f"Published in {publication_year}, copyright expires in {expiry_year}." if not enough_time_passed else None
            )
            data.copyright = info
            return data

        # Default to copyrighted if no conditions met
        default = CopyrightInfo(
            status=CopyrightStatus.IN_COPYRIGHT,
            reason="No sufficient information to determine copyright status. Defaulting to copyrighted."
        )
        data.copyright = default
        return data