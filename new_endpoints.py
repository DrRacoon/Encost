#!/bin/python3.12
from itertools import groupby
from typing import Final, LiteralString, Optional

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship


engine = sa.create_engine(
    'sqlite:///C:/Users/Ринкман/Downloads/client.sqlite', echo=True
)


class Base(DeclarativeBase):
    type_annotation_map = {str: sa.VARCHAR}


class Endpoint(Base):
    __tablename__ = 'endpoints'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[Optional[str]]
    active: Mapped[Optional[str]]

    reasons: Mapped[list["EndpointReason"]] = relationship(back_populates="endpoint")


class EndpointReason(Base):
    __tablename__ = 'endpoint_reasons'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    endpoint_id: Mapped[Optional[int]] = mapped_column(sa.ForeignKey("endpoints.id"))
    reason_name: Mapped[Optional[str]]
    reason_hierarchy: Mapped[Optional[str]]

    endpoint: Mapped[Endpoint] = relationship(back_populates="reasons")


# Участки - самая непонятная из таблиц, так как автоинкрементный индекс, получается,
# не соотносится с участком 1:1.
class EndpointGroup(Base):
    __tablename__ = 'endpoint_groups'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    endpoint_id: Mapped[Optional[int]] = mapped_column(sa.ForeignKey("endpoints.id"))
    name: Mapped[Optional[str]]

    endpoint: Mapped[Endpoint] = relationship(uselist=False)


# Данные
reason_map: Final[dict[LiteralString, LiteralString]] = {
    "Сварочный аппарат №1": "Сварка",
    "Пильный аппарат №2": "Старый ЧПУ",
    "Фрезер №3": "Фрезерный станок",
}
new_group_name: Final[LiteralString] = "Цех №2"
existing_endpoint_names_to_update: Final[set[LiteralString]] = {
    "Пильный станок", "Старый ЧПУ"
}

new_endpoints: Final[tuple[Endpoint, ...]] = tuple(
    Endpoint(name=name, active="true")
    for name in ("Сварочный аппарат №1", "Пильный аппарат №2", "Фрезер №3")
)


def try_transaction(session: Session) -> None:
    session.add_all(new_endpoints)
    session.flush()

    name_to_reasons = {
        name: tuple(row[1] for row in group)
        for name, group in groupby(
            session.execute(
                sa.select(Endpoint.name, EndpointReason)
                .join(Endpoint)
                .where(Endpoint.name.in_(reason_map.values()))
                .order_by(Endpoint.name)
            ),
            lambda row: row[0],
        )
    }

    for new_endpoint in new_endpoints:
        new_endpoint.reasons.extend(
            EndpointReason(
                endpoint_id=new_endpoint.id,
                reason_name=reason.reason_name,
                reason_hierarchy=reason.reason_hierarchy,
            )
            for reason in name_to_reasons[reason_map[new_endpoint.name]]
        )

    session.add_all(
        EndpointGroup(endpoint_id=endpoint.id, name=new_group_name)
        for endpoint in new_endpoints
    )

    endpoint_ids_to_update = tuple(
        session.execute(
            sa.select(Endpoint.id)
            .where(Endpoint.name.in_(existing_endpoint_names_to_update))
        ).scalars()
    )
    session.execute(
        sa.delete(EndpointGroup)
        .where(
            EndpointGroup.endpoint_id.in_(
                endpoint_id for endpoint_id in endpoint_ids_to_update
            )
        )
    )
    session.add_all(
        EndpointGroup(endpoint_id=endpoint_id, name=new_group_name)
        for endpoint_id in endpoint_ids_to_update
    )

    session.commit()


def main() -> None:
    with Session(engine) as session:
        try:
            try_transaction(session)
        except Exception:
            session.rollback()
            raise


if __name__ == "__main__":
    main()
