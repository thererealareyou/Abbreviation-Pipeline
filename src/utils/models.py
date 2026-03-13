from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Text, JSON, DateTime, CheckConstraint, Index
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

Base = declarative_base()


class Document(Base):
    __tablename__ = "documents"

    id = Column(Integer, primary_key=True)
    filename = Column(String, unique=True, index=True, nullable=False)
    status = Column(String, default="processing", nullable=False)

    # Стадии пайплайна
    term_search_done = Column(Boolean, default=False, nullable=False)
    abbr_search_done = Column(Boolean, default=False, nullable=False)
    term_defs_done = Column(Boolean, default=False, nullable=False)
    abbr_defs_done = Column(Boolean, default=False, nullable=False)
    term_conflicts_done = Column(Boolean, default=False, nullable=False)
    abbr_conflicts_done = Column(Boolean, default=False, nullable=False)

    final_dictionary = Column(JSON, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    chunks = relationship("Chunk", back_populates="document", cascade="all, delete-orphan")

    __table_args__ = (
        CheckConstraint(
            "status IN ('processing', 'resolving_conflicts', 'completed', 'error')",
            name="ck_document_status",
        ),
    )


class Chunk(Base):
    __tablename__ = "chunks"

    id = Column(Integer, primary_key=True)
    doc_id = Column(Integer, ForeignKey("documents.id", ondelete="CASCADE"), nullable=False)
    text = Column(Text, nullable=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    document = relationship("Document", back_populates="chunks")
    extracted_items = relationship("ExtractedItem", back_populates="chunk", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_chunk_doc_id", "doc_id"),
    )


class ExtractedItem(Base):
    __tablename__ = "extracted_items"

    id = Column(Integer, primary_key=True)
    chunk_id = Column(Integer, ForeignKey("chunks.id", ondelete="CASCADE"), nullable=False)
    item_type = Column(String, nullable=False)
    word = Column(String, index=True, nullable=False)
    definition = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    chunk = relationship("Chunk", back_populates="extracted_items")

    __table_args__ = (
        CheckConstraint(
            "item_type IN ('term', 'abbr')",
            name="ck_extracted_item_type",
        ),
        Index("ix_extracted_item_chunk_type", "chunk_id", "item_type"),
    )
