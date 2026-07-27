from typing import TYPE_CHECKING, List, Optional

from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from app.models.sub_task import SubTask


class SubTaskType(SQLModel, table=True):
    __tablename__ = "sub_task_types"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(unique=True, index=True, nullable=False)
    display_name: str = Field(nullable=False)
    description: str = Field(default="")
    is_enabled: bool = Field(default=True)

    sub_tasks: List["SubTask"] = Relationship(back_populates="sub_task_type")
