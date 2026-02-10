from dataclasses import asdict

from src.mappers import BaseMapper, AuthorMapper
from src.models.record import ParsedDate


def assert_dataclass_equal(a, b):
    assert asdict(a) == asdict(b)


def test_mapper_roundtrip(mapper_cls: BaseMapper,
                          domain_instance,
                          orm_instance,
                          domain_eq=assert_dataclass_equal):
    """Test that mapping from domain to ORM and back yields the original domain instance."""
    orm_mapped = mapper_cls.to_orm(domain_instance)
    assert isinstance(orm_mapped, mapper_cls.ORM_CLASS)

    domain_mapped = mapper_cls.to_domain(orm_instance)
    assert isinstance(domain_mapped, mapper_cls.DOMAIN_CLASS)

    round_trip = mapper_cls.to_domain(
        mapper_cls.to_orm(domain_instance)
    )

    domain_eq(domain_instance, round_trip)


class TestMapper:

    def test_mapper_contract_author(self):
        from src.models.record import AuthorRecord
        from src.models.orm import AuthorORM
        author_model = AuthorRecord(
            ol_id="OL123A",
            name="Jane Doe",
            birth_date=ParsedDate("19_!", -1, False, "Fail to Parse"),
            death_date=ParsedDate("1950", 1950, True)
        )
        author_orm = AuthorORM(
            ol_id="OL123A",
            name="Jane Doe",
            birth_date={
                "raw": "19_!",
                "parsed_val": -1,
                "is_exact": False,
                "reason": "Fail to Parse"
            },
            death_date={
                "raw": "1950",
                "parsed_val": 1950,
                "is_exact": True,
                "reason": None
            }
        )
        test_mapper_roundtrip(AuthorMapper(), author_model, author_orm)

    def test_mapper_contract_edition(self):
        from src.models.record import EditionRecord
        from src.models.orm import EditionORM
        edition_model = EditionRecord(
            ol_id="OL123M",
            title="Example Book",
            author_ids=["OL123A", "OL456A"],
            publish_year=2020
        )
        edition_orm = EditionORM(
            ol_id="OL123M",
            title="Example Book",
            author_ids=["OL123A", "OL456A"],
            publish_year=2020
        )
        from src.mappers import EditionMapper
        test_mapper_roundtrip(EditionMapper, edition_model, edition_orm)