import uuid
from datetime import datetime
from typing import Optional
from sqlalchemy import DateTime, Boolean, text, String, ForeignKey, Text, CheckConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, relationship
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class MixinCore:
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        index=True,
        unique=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, onupdate=func.now())
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("false"))


class Document(Base, MixinCore):
    __tablename__ = "documents"

    filename: Mapped[str] = mapped_column(String(255), index=True, nullable=False, unique=True)
    status: Mapped[str] = mapped_column(String(50), default="processing")

    # --- Стадии пайплайна (Mapped автоматически подхватит тип bool) ---
    term_search_done: Mapped[bool] = mapped_column(default=False)
    abbr_search_done: Mapped[bool] = mapped_column(default=False)
    term_defs_done: Mapped[bool] = mapped_column(default=False)
    abbr_defs_done: Mapped[bool] = mapped_column(default=False)
    term_conflicts_done: Mapped[bool] = mapped_column(default=False)
    abbr_conflicts_done: Mapped[bool] = mapped_column(default=False)

    # --- Прогресс поиска ---
    finding_abbr_chunks: Mapped[int] = mapped_column(server_default="0")
    finding_term_chunks: Mapped[int] = mapped_column(server_default="0")

    # --- Прогресс определений ---
    defining_abbrs: Mapped[int] = mapped_column(server_default="0")
    defining_terms: Mapped[int] = mapped_column(server_default="0")

    # --- Конфликты ---
    total_abbr_conflicts: Mapped[int] = mapped_column(server_default="0")
    total_term_conflicts: Mapped[int] = mapped_column(server_default="0")

    # --- Батчи ---
    term_batches_total: Mapped[int] = mapped_column(server_default="0")
    term_batches_done: Mapped[int] = mapped_column(server_default="0")
    abbr_batches_total: Mapped[int] = mapped_column(server_default="0")
    abbr_batches_done: Mapped[int] = mapped_column(server_default="0")

    chunks: Mapped[list["Chunk"]] = relationship(back_populates="document", cascade="all, delete-orphan")

    __table_args__ = (
        CheckConstraint(
            "status IN ('processing', 'completed_term', 'completed_abbr', 'completed', 'error')",
            name="ck_document_status",
        ),
    )


class Chunk(Base, MixinCore):
    __tablename__ = "chunks"

    doc_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("documents.id", ondelete="CASCADE"), index=True)
    text: Mapped[str] = mapped_column(Text)
    order: Mapped[int] = mapped_column()

    document: Mapped["Document"] = relationship(back_populates="chunks")
    items: Mapped[list["ExtractedItem"]] = relationship(back_populates="chunk", cascade="all, delete-orphan")


class ExtractedItem(Base, MixinCore):
    __tablename__ = "extracted_items"

    chunk_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("chunks.id", ondelete="CASCADE"), index=True)
    item_type: Mapped[str] = mapped_column(String(20))
    word: Mapped[str] = mapped_column(String(255))
    definition: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_final: Mapped[bool] = mapped_column(Boolean, default=False)

    chunk: Mapped["Chunk"] = relationship(back_populates="items")


class GlobalDictionary(Base):
    __tablename__ = "global_dictionary"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    word: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    item_type: Mapped[str] = mapped_column(String(4))
    definition: Mapped[str] = mapped_column(Text)


class TransliterationDictionary(Base, MixinCore):
    __tablename__ = "transliteration_entries"

    abbr: Mapped[str] = mapped_column(String(20))
    ru_variant: Mapped[str] = mapped_column(String(60))


class SystemState(Base):
    __tablename__ = "system_state"

    key: Mapped[str] = mapped_column(String(50), primary_key=True) # "dict_build_status"
    value: Mapped[str] = mapped_column(String(50)) # "processing", "ready", "error"
    updated_at: Mapped[datetime] = mapped_column(default=func.now(), onupdate=func.now())
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)