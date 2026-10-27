"""Strict v1 administrator metadata review routes. Workers are never started here."""
from typing import Annotated, Literal

from fastapi import Request
from pydantic import BaseModel, ConfigDict, Field

from .library_api import InputError, _params, _number
from .metadata_service import MetadataError


class _Body(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)


Identifier = Annotated[str, Field(min_length=1, max_length=200, pattern=r'^[A-Za-z0-9_.:-]+$')]


class RetentionPolicy(_Body):
    backup_allowed: bool
    all_copies_deletion_required: bool
    policy_id: str | None = Field(default=None, max_length=200)


class RightsDecision(_Body):
    state: Literal['allowed', 'review', 'blocked']
    officiality: Literal['verified', 'unknown', 'not_applicable']
    match_state: Literal['exact', 'review', 'not_applicable']
    display_allowed: bool
    cache_allowed: bool
    derivative_allowed: bool
    cache_until_ms: int | None = Field(ge=0, le=253402300799999)
    retention_policy: RetentionPolicy
    evidence: dict


class CandidateReview(_Body):
    expected_control_version: int = Field(ge=1, le=9223372036854775806)
    expected_head_version: int = Field(ge=0, le=9223372036854775806)
    request_id: Identifier
    review_status: Literal['pending', 'approved', 'rejected']
    select: bool


class RightsReview(_Body):
    work_id: Identifier
    kind: Literal['description', 'cover']
    expected_version: int = Field(ge=0, le=9223372036854775806)
    request_id: Identifier
    decision: RightsDecision


class EditDescription(_Body):
    text: str = Field(min_length=1, max_length=16384)
    rights_id: Identifier
    expected_rights_version: int = Field(ge=1, le=9223372036854775806)
    expected_head_version: int = Field(ge=0, le=9223372036854775806)
    request_id: Identifier


class GeneratedRange(_Body):
    start: int = Field(ge=0, le=16_000_000)
    end: int = Field(ge=1, le=16_000_000)


class GeneratedEvidenceRequest(_Body):
    ranges: list[GeneratedRange] = Field(min_length=1, max_length=16)


class GeneratedUnitEvidence(_Body):
    unit_id: str = Field(min_length=75, max_length=75, pattern=r'^summary-v1:[0-9a-f]{64}$')
    range_indexes: list[int] = Field(min_length=1, max_length=16)


class GeneratedFlagResolution(_Body):
    code: str = Field(min_length=1, max_length=256)
    resolution: Literal['accept_with_evidence', 'reject', 'fix_required']
    range_indexes: list[int] = Field(max_length=16)


class GeneratedReview(_Body):
    expected_control_version: int = Field(ge=1, le=9223372036854775806)
    expected_head_version: int = Field(ge=0, le=9223372036854775806)
    expected_rights_version: int = Field(ge=1, le=9223372036854775806)
    request_id: Identifier
    review_context_digest: str = Field(pattern=r'^[0-9a-f]{64}$')
    ranges: list[GeneratedRange] = Field(min_length=1, max_length=16)
    review_units: list[GeneratedUnitEvidence] = Field(max_length=1)
    grounding_disposition: Literal['grounded', 'insufficient']
    spoiler_disposition: Literal['acceptable', 'reject', 'not_applicable']
    flag_resolutions: list[GeneratedFlagResolution] = Field(max_length=64)
    decision: Literal['approved', 'rejected']
    select: bool


def install_metadata_admin_routes(app, metadata, credentials):
    def creds(request, *, scalar=()):
        _params(request, scalar=scalar)
        app.state.library.supervisor.admit()
        return credentials(request)

    @app.get('/api/v1/admin/metadata/review-queue')
    def review_queue(request: Request):
        token = creds(request, scalar=('limit',))[0]
        limit = _number(request.query_params.get('limit', '50'), low=1, high=100)
        try:
            return metadata.review_queue(token, limit=limit)
        except MetadataError:
            raise InputError() from None

    @app.get('/api/v1/admin/metadata/candidates/{candidate_id}/generated-review')
    def generated_review_detail(candidate_id: Identifier, request: Request):
        token = creds(request)[0]
        try:
            return metadata.generated_review_detail(token, candidate_id=candidate_id)
        except MetadataError:
            raise InputError() from None

    @app.post('/api/v1/admin/metadata/candidates/{candidate_id}/generated-evidence')
    def generated_evidence(candidate_id: Identifier, body: GeneratedEvidenceRequest,
                           request: Request):
        token = creds(request)[0]
        try:
            return metadata.read_generated_evidence(token, candidate_id=candidate_id,
                ranges=[item.model_dump() for item in body.ranges])
        except MetadataError:
            raise InputError() from None

    @app.post('/api/v1/admin/metadata/candidates/{candidate_id}/generated-review')
    def review_generated_candidate(candidate_id: Identifier, body: GeneratedReview,
                                   request: Request):
        token, csrf = creds(request)
        try:
            return metadata.review_generated_candidate(token, csrf,
                candidate_id=candidate_id,
                expected_control_version=body.expected_control_version,
                expected_head_version=body.expected_head_version,
                expected_rights_version=body.expected_rights_version,
                request_id=body.request_id,
                review_context_digest=body.review_context_digest,
                ranges=[item.model_dump() for item in body.ranges],
                review_units=[item.model_dump() for item in body.review_units],
                grounding_disposition=body.grounding_disposition,
                spoiler_disposition=body.spoiler_disposition,
                flag_resolutions=[item.model_dump() for item in body.flag_resolutions],
                decision=body.decision, select=body.select)
        except MetadataError:
            raise InputError() from None

    @app.post('/api/v1/admin/metadata/candidates/{candidate_id}/review')
    def review_candidate(candidate_id: Identifier, body: CandidateReview, request: Request):
        token, csrf = creds(request)
        return metadata.review_candidate(token, csrf, candidate_id=candidate_id,
            expected_control_version=body.expected_control_version,
            expected_head_version=body.expected_head_version,
            request_id=body.request_id, review_status=body.review_status, select=body.select)

    @app.post('/api/v1/admin/metadata/rights/{rights_id}/review')
    def review_rights(rights_id: Identifier, body: RightsReview, request: Request):
        token, csrf = creds(request)
        return metadata.review_rights(token, csrf, rights_id=rights_id, work_id=body.work_id,
            kind=body.kind, expected_version=body.expected_version,
            request_id=body.request_id, decision=body.decision.model_dump(exclude_unset=True))

    @app.post('/api/v1/admin/metadata/works/{work_id}/description')
    def edit_description(work_id: Identifier, body: EditDescription, request: Request):
        token, csrf = creds(request)
        return metadata.edit_description(token, csrf, work_id=work_id, text=body.text,
            rights_id=body.rights_id, expected_rights_version=body.expected_rights_version,
            expected_head_version=body.expected_head_version, request_id=body.request_id)

    @app.get('/api/v1/admin/metadata/works/{work_id}')
    def work_metadata(work_id: Identifier, request: Request):
        token = creds(request)[0]
        return metadata.work_metadata_state(token, work_id=work_id)
