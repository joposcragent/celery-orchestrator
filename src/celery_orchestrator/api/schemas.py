from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class CollectionBatchEvent(BaseModel):
    triggered_at: str | None = Field(None, alias="triggeredAt")
    correlation_id: UUID | None = Field(None, alias="correlationId")

    model_config = ConfigDict(populate_by_name=True, extra="allow")


class CollectionQueryEvent(BaseModel):
    search_query: str = Field(alias="searchQuery")
    parent_batch_task_uuid: UUID = Field(alias="parentBatchTaskUuid")

    model_config = {"populate_by_name": True}


class CollectionQueryProgressEvent(BaseModel):
    collection_query_task_uuid: UUID = Field(alias="collectionQueryTaskUuid")
    current_page: int | None = Field(None, alias="currentPage", ge=1)
    total_pages: int | None = Field(None, alias="totalPages", ge=1)
    execution_log_fragment: str | None = Field(None, alias="executionLogFragment")
    detail: dict[str, Any] | None = None

    model_config = {"populate_by_name": True}


class CollectionQueryCompleteEvent(BaseModel):
    collection_query_task_uuid: UUID = Field(alias="collectionQueryTaskUuid")
    execution_log: Any = Field(alias="executionLog")
    result: dict[str, Any]

    model_config = {"populate_by_name": True}


class EvaluationEnqueueEvent(BaseModel):
    job_posting_uuid: UUID = Field(alias="jobPostingUuid")
    evaluation_task_uuid: UUID | None = Field(None, alias="evaluationTaskUuid")
    parent_task_uuid: UUID | None = Field(None, alias="parentTaskUuid")

    model_config = {"populate_by_name": True}


class EvaluationCompleteEvent(BaseModel):
    evaluation_task_uuid: UUID = Field(alias="evaluationTaskUuid")
    execution_log: Any = Field(alias="executionLog")
    result: dict[str, Any]

    model_config = {"populate_by_name": True}


class NotificationEvent(BaseModel):
    job_posting_uuid: UUID = Field(alias="jobPostingUuid")
    relevance_score: float | None = Field(None, alias="relevanceScore")
    notification_threshold: float | None = Field(None, alias="notificationThreshold")
    message: str | None = None
    evaluation_task_uuid: UUID | None = Field(None, alias="evaluationTaskUuid")

    model_config = {"populate_by_name": True}


class OrchestrationTaskCreate(BaseModel):
    task_uuid: UUID | None = Field(None, alias="taskUuid")
    task_type: str = Field(alias="type")
    parent_task_uuid: UUID | None = Field(None, alias="parentTaskUuid")
    initial_context: dict[str, Any] | None = Field(None, alias="initialContext")

    model_config = {"populate_by_name": True, "protected_namespaces": ()}


class OrchestrationTaskProgress(BaseModel):
    recorded_at: str | None = Field(None, alias="recordedAt")
    execution_log_fragment: str | None = Field(None, alias="executionLogFragment")
    detail: dict[str, Any] | None = None

    model_config = {"populate_by_name": True}


class OrchestrationTaskCompletion(BaseModel):
    terminal_status: str = Field(alias="terminalStatus")
    execution_log: Any | None = Field(None, alias="executionLog")
    result: dict[str, Any] | None = None
    error_message: str | None = Field(None, alias="errorMessage")

    model_config = {"populate_by_name": True}
