"""MergeWhisper Strict Data Contracts & Schema Definitions (Pydantic v2).

Defines production-grade schemas for the 5-stage release pipeline:
- Stage 01: Ingest (IngestData)
- Stage 02: Audit (AuditData)
- Stage 03: Verify (VerifyData)
- Stage 04: Changelog (ChangelogData)
- Stage 05: Ship (ShipData)

Alongside the structured output manifest (PipelineStagesManifest) and complete
telemetry manifest (SynthesisResponse).
"""

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class BaseSchema(BaseModel):
    """Base model applying strict Pydantic v2 configuration across all contracts."""

    model_config = ConfigDict(
        populate_by_name=True,
        validate_assignment=True,
        extra="ignore",
    )


# ==============================================================================
# Stage 01: Ingest Data Contract
# ==============================================================================

class IngestData(BaseSchema):
    """Stage 01: Ingestion model validating spoken intent and git diff context."""

    intent: str = Field(
        ...,
        description="Developer's architectural directive or spoken intent.",
    )
    diff: Optional[str] = Field(
        default=None,
        description="Optional raw git diff, staged changes, or patch context.",
    )
    target_repo: str = Field(
        default="lokebuilds/MergeWhisper",
        description="Target GitHub/GitLab repository identifier in owner/repo format.",
    )
    base_branch: str = Field(
        default="main",
        description="Target upstream branch to merge changes into.",
    )
    head_branch: str = Field(
        default="feat/live-demo",
        description="Working feature or bugfix branch containing changes.",
    )
    tech_stack: str = Field(
        default="Python FastAPI",
        description="Identified or declared technology stack of the target service.",
    )
    word_count: int = Field(
        default=0,
        ge=0,
        description="Total word count of developer voice intent and dictation directives.",
    )
    payload: Optional[str] = Field(
        default=None,
        description="Optional raw text payload alias for intent.",
    )

    @model_validator(mode="before")
    @classmethod
    def populate_intent_from_payload(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "textPayload" in data and not data.get("intent"):
                data["intent"] = str(data["textPayload"])
            if "payload" in data and not data.get("intent"):
                data["intent"] = str(data["payload"])
        return data

    @field_validator("intent")
    @classmethod
    def validate_non_empty_intent(cls, value: str) -> str:
        """Ensure intent contains actionable textual directives."""
        if not isinstance(value, str):
            raise ValueError("Intent must be a string.")
        stripped = value.strip()
        if not stripped:
            raise ValueError("Intent cannot be empty or contain solely whitespace characters.")
        return stripped


# Backward compatibility alias
IngestionPayload = IngestData


# ==============================================================================
# Stage 02: Audit Data Contract
# ==============================================================================

class AuditData(BaseSchema):
    """Stage 02: DevOps risk assessment evaluating blast radius, safety tier, and rollback readiness."""

    risk_level: Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"] = Field(
        ...,
        description="Assessed risk classification tier based on change surface and architectural impact.",
    )
    blast_radius: str = Field(
        ...,
        description="Detailed description of affected subsystem boundaries and potential failure domains.",
    )
    is_breaking_change: bool = Field(
        ...,
        description="Indicates whether this change breaks external API contracts, database schemas, or client behavior.",
    )
    rollback_plan: str = Field(
        ...,
        min_length=1,
        description="Exact automated rollback or git revert command for rapid incident mitigation.",
    )
    intercepted_threats: List[str] = Field(
        default_factory=list,
        description="List of detected secret patterns, unredacted tokens, or destructive SQL DDL.",
    )

    @field_validator("rollback_plan")
    @classmethod
    def validate_non_empty_rollback_plan(cls, value: str) -> str:
        """Ensure rollback plan is explicitly documented with actionable remediation commands."""
        if not isinstance(value, str):
            raise ValueError("Rollback plan must be a string.")
        stripped = value.strip()
        if not stripped:
            raise ValueError("Rollback plan cannot be empty or contain solely whitespace.")
        return stripped


# Backward compatibility alias
DevOpsAudit = AuditData


# ==============================================================================
# Stage 03: Verify Data Contract (VerificationMatrix & EdgeCase)
# ==============================================================================

class EdgeCase(BaseModel):
    """Critical boundary regression or edge case condition with severity."""

    severity: str = Field(
        ...,
        description="Risk or severity classification ('red' or 'orange').",
    )
    description: str = Field(
        ...,
        description="Detailed failure condition or edge case scenario.",
    )

    @model_validator(mode="before")
    @classmethod
    def parse_raw_edge_case(cls, data: Any) -> Any:
        if isinstance(data, str):
            lower = data.lower()
            sev = "red" if any(k in lower for k in ["critical", "catastrophic", "breach", "fail", "drop", "deadlock", "loss", "timeout", "lock"]) else "orange"
            return {"severity": sev, "description": data}
        return data


class VerificationMatrix(BaseModel):
    """Stage 03: Targeted QA verification matrix and test execution strategy."""

    primary_objective: str = Field(
        ...,
        description="Primary verification objective describing what must be proven safe prior to merge.",
    )
    ordered_steps: List[str] = Field(
        default_factory=list,
        description="Ordered, reproducible test steps or automated execution commands (e.g. pytest, psql, curl).",
    )
    edge_cases: List[EdgeCase] = Field(
        default_factory=list,
        description="Critical boundary conditions, race hazards, and regression vectors with severity ('red' or 'orange').",
    )

    @model_validator(mode="before")
    @classmethod
    def normalize_verification_matrix(cls, data: Any) -> Any:
        if isinstance(data, dict):
            # Support objective <-> primary_objective
            if "objective" in data and not data.get("primary_objective"):
                data["primary_objective"] = data["objective"]
            elif "primary_objective" in data and not data.get("objective"):
                data["objective"] = data["primary_objective"]

            # Support verification_steps <-> ordered_steps
            if "verification_steps" in data and not data.get("ordered_steps"):
                data["ordered_steps"] = data["verification_steps"]
            elif "ordered_steps" in data and not data.get("verification_steps"):
                data["verification_steps"] = data["ordered_steps"]

            # Support edge_cases where items might be strings or EdgeCase dicts
            if "edge_cases" in data and isinstance(data["edge_cases"], list):
                parsed = []
                for ec in data["edge_cases"]:
                    if isinstance(ec, str):
                        lower = ec.lower()
                        sev = "red" if any(k in lower for k in ["critical", "catastrophic", "breach", "fail", "drop", "deadlock", "loss", "timeout", "lock"]) else "orange"
                        parsed.append({"severity": sev, "description": ec})
                    else:
                        parsed.append(ec)
                data["edge_cases"] = parsed
        return data

    @property
    def objective(self) -> str:
        return self.primary_objective

    @objective.setter
    def objective(self, val: str) -> None:
        self.primary_objective = val

    @property
    def verification_steps(self) -> List[str]:
        return self.ordered_steps

    @verification_steps.setter
    def verification_steps(self, val: List[str]) -> None:
        self.ordered_steps = val


# Backward compatibility aliases
VerifyData = VerificationMatrix
QACard = VerificationMatrix


# ==============================================================================
# Stage 04: Changelog Data Contract
# ==============================================================================

class ChangelogData(BaseSchema):
    """Stage 04: SemVer evaluation and Keep-a-Changelog compliance entry."""

    semver_bump: Literal["PATCH", "MINOR", "MAJOR"] = Field(
        ...,
        description="Calculated semantic version increment tier conforming to SemVer 2.0.0.",
    )
    current_version: str = Field(
        default="v18.2.0",
        description="Current active release version.",
    )
    next_version: str = Field(
        ...,
        description="Projected next release version reflecting the assessed bump tier.",
    )
    keep_a_changelog_markdown: str = Field(
        ...,
        description="Changelog markdown entry formatted per the 'Keep a Changelog' standard.",
    )


# Backward compatibility alias
ChangelogCard = ChangelogData


# ==============================================================================
# Stage 05: Ship Data Contract & Deliverables
# ==============================================================================

class GeminiShipData(BaseSchema):
    """Stage 05 structured output schema for Gemini generation."""

    title: str = Field(
        ...,
        description="Conventional commit pull request title synthesized from intent and diff.",
    )
    summary: str = Field(
        ...,
        description="High-level architectural summary of the change for review leads.",
    )
    markdown_body: str = Field(
        ...,
        description="Complete GitHub/GitLab-compliant Markdown description including test plan and checklists.",
    )
    branch_name: Optional[str] = Field(
        default=None,
        description="Dynamic contextual git branch name synthesized from intent (e.g. fix/session-cookie-recursion).",
    )
    channel_name: str = Field(
        default="eng-releases",
        description="Designated Slack communication channel for release broadcasts.",
    )
    broadcast_summary: str = Field(
        ...,
        description="Concise, plain-text summary suitable for mobile notifications and executive previews.",
    )
    issue_tracker_curl: str = Field(
        default="",
        description="A valid cURL command to POST a status update to a Linear or Jira API, including the PR link and marking the ticket as 'Done'.",
    )
    github_actions_ci_yaml: str = Field(
        default="",
        description="Executable GitHub Actions YAML workflow script (.github/workflows/verify-pr.yml) configured to execute the Stage 03 verification steps.",
    )


class ShipData(GeminiShipData):
    """Stage 05: Complete Pull Request dispatch and stakeholder release broadcast."""

    issue_tracker_curl: str = Field(
        default="",
        description="A valid cURL command to POST a status update to a Linear or Jira API, including the PR link and marking the ticket as 'Done'.",
    )
    github_dispatch_url: Optional[str] = Field(
        default=None,
        description="Repository dispatch webhook or compare URL.",
    )
    formatted_slack_payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Pre-formatted Slack Block Kit JSON data structure for rich visual messaging.",
    )


class PRCard(BaseSchema):
    """Deliverable projection: Pull Request card ready for repository dispatch."""

    title: str = Field(..., description="Conventional commit pull request title.")
    summary: str = Field(..., description="High-level architectural summary.")
    markdown_body: str = Field(..., description="Complete Markdown description.")
    github_dispatch_url: Optional[str] = Field(default=None, description="GitHub compare/dispatch URL.")
    issue_tracker_curl: str = Field(
        default="",
        description="A valid cURL command to POST a status update to a Linear or Jira API, including the PR link and marking the ticket as 'Done'.",
    )
    github_actions_ci_yaml: str = Field(
        default="",
        description="Executable GitHub Actions YAML workflow script (.github/workflows/verify-pr.yml).",
    )


class SlackCard(BaseSchema):
    """Deliverable projection: Stakeholder broadcast card formatted for engineering communication."""

    channel_name: str = Field(default="eng-releases", description="Designated Slack communication channel.")
    broadcast_summary: str = Field(..., description="Concise plain-text broadcast announcement.")
    formatted_slack_payload: Dict[str, Any] = Field(default_factory=dict, description="Slack Block Kit JSON.")


# ==============================================================================
# Structured Output Schema for Gemini Generation
# ==============================================================================

class PipelineStagesManifest(BaseSchema):
    """Enforced structured output schema for stages 02 through 05 via Gemini 3.8 Flash."""

    audit: AuditData = Field(..., description="Stage 02: DevOps risk assessment and rollback runbook.")
    verify: VerifyData = Field(..., description="Stage 03: QA verification matrix and test steps.")
    changelog: ChangelogData = Field(..., description="Stage 04: SemVer evaluation and Keep-a-Changelog entry.")
    ship: GeminiShipData = Field(..., description="Stage 05: Pull request dispatch and Slack release broadcast.")


# ==============================================================================
# Complete Synthesized Release Manifest
# ==============================================================================

class SynthesisResponse(BaseSchema):
    """Encapsulates the complete synthesized release manifest across all 5 stages with telemetry."""

    session_id: str = Field(
        ...,
        description="Unique session identifier tracking this orchestration transaction.",
    )
    latency_ms: float = Field(
        ...,
        ge=0.0,
        description="End-to-end orchestration pipeline latency in milliseconds.",
    )
    dev_time_saved_minutes: float = Field(
        ...,
        ge=0.0,
        description="Estimated engineering time saved through autonomous release deliverable synthesis.",
    )
    spoken_word_count: int = Field(
        ...,
        ge=0,
        description="Total word count of developer voice intent and dictation directives.",
    )

    # 5 Strict Pipeline Stages
    ingest: Optional[IngestData] = Field(default=None, description="Stage 01: Spoken intent & git diff context.")
    audit: AuditData = Field(..., description="Stage 02: SRE risk assessment & rollback runbook.")
    verify: VerifyData = Field(..., description="Stage 03: QA verification matrix & test steps.")
    changelog: ChangelogData = Field(..., description="Stage 04: SemVer evaluation & Keep-a-Changelog.")
    ship: ShipData = Field(..., description="Stage 05: GitHub PR & Slack release broadcast.")

    # UI backward-compatibility projection fields
    devops_audit: AuditData = Field(..., description="Alias projection for Stage 02 UI components.")
    qa_card: VerifyData = Field(..., description="Alias projection for Stage 03 UI components.")
    changelog_card: ChangelogData = Field(..., description="Alias projection for Stage 04 UI components.")
    pr_card: PRCard = Field(..., description="Alias projection for Stage 05 PR card.")
    slack_card: SlackCard = Field(..., description="Alias projection for Stage 05 Slack card.")

    @model_validator(mode="before")
    @classmethod
    def populate_cross_compat(cls, data: Any) -> Any:
        """Guarantee automatic cross-population between 5 stage fields and UI alias fields."""
        if isinstance(data, dict):
            # Populate audit <-> devops_audit
            if "devops_audit" in data and "audit" not in data:
                data["audit"] = data["devops_audit"]
            elif "audit" in data and "devops_audit" not in data:
                data["devops_audit"] = data["audit"]

            # Populate verify <-> qa_card
            if "qa_card" in data and "verify" not in data:
                data["verify"] = data["qa_card"]
            elif "verify" in data and "qa_card" not in data:
                data["qa_card"] = data["verify"]

            # Populate changelog <-> changelog_card
            if "changelog_card" in data and "changelog" not in data:
                data["changelog"] = data["changelog_card"]
            elif "changelog" in data and "changelog_card" not in data:
                data["changelog_card"] = data["changelog"]

            # Populate ship <-> pr_card / slack_card
            if "ship" in data:
                ship_val = data["ship"]
                ship_dict = ship_val if isinstance(ship_val, dict) else (ship_val.model_dump() if hasattr(ship_val, "model_dump") else ship_val.__dict__)
                if "pr_card" not in data:
                    data["pr_card"] = {
                        "title": ship_dict.get("title", ""),
                        "summary": ship_dict.get("summary", ""),
                        "markdown_body": ship_dict.get("markdown_body", ""),
                        "github_dispatch_url": ship_dict.get("github_dispatch_url"),
                        "issue_tracker_curl": ship_dict.get("issue_tracker_curl", ""),
                        "github_actions_ci_yaml": ship_dict.get("github_actions_ci_yaml", ""),
                    }
                if "slack_card" not in data:
                    data["slack_card"] = {
                        "channel_name": ship_dict.get("channel_name", "eng-releases"),
                        "broadcast_summary": ship_dict.get("broadcast_summary", ""),
                        "formatted_slack_payload": ship_dict.get("formatted_slack_payload", {}),
                    }
            elif "pr_card" in data and "slack_card" in data:
                pr = data["pr_card"]
                slack = data["slack_card"]
                pr_dict = pr if isinstance(pr, dict) else (pr.model_dump() if hasattr(pr, "model_dump") else pr.__dict__)
                slack_dict = slack if isinstance(slack, dict) else (slack.model_dump() if hasattr(slack, "model_dump") else slack.__dict__)
                if "ship" not in data:
                    data["ship"] = {
                        "title": pr_dict.get("title", ""),
                        "summary": pr_dict.get("summary", ""),
                        "markdown_body": pr_dict.get("markdown_body", ""),
                        "github_dispatch_url": pr_dict.get("github_dispatch_url"),
                        "channel_name": slack_dict.get("channel_name", "eng-releases"),
                        "broadcast_summary": slack_dict.get("broadcast_summary", ""),
                        "formatted_slack_payload": slack_dict.get("formatted_slack_payload", {}),
                        "issue_tracker_curl": pr_dict.get("issue_tracker_curl", ""),
                        "github_actions_ci_yaml": pr_dict.get("github_actions_ci_yaml", ""),
                    }
        return data


# ==============================================================================
# Iterative Refinement Request Contract
# ==============================================================================

class VoicePatchRequest(BaseSchema):
    """Enables real-time iterative prompt refinement by mutating existing release manifests."""

    session_id: str = Field(
        ...,
        description="Session identifier of the existing synthesis manifest to be selectively mutated.",
    )
    current_response: SynthesisResponse = Field(
        ...,
        description="Current state of the synthesis manifest prior to selective mutation.",
    )
    patch_instruction: str = Field(
        ...,
        description="Spoken follow-up voice prompt dictating selective adjustments to the release deliverables.",
    )

    @field_validator("patch_instruction")
    @classmethod
    def validate_non_empty_instruction(cls, value: str) -> str:
        """Ensure patch instruction contains actionable refinement directives."""
        if not isinstance(value, str):
            raise ValueError("Patch instruction must be a string.")
        stripped = value.strip()
        if not stripped:
            raise ValueError("Patch instruction cannot be empty or contain solely whitespace.")
        return stripped


# ==============================================================================
# System Health Telemetry
# ==============================================================================

class SystemHealthResponse(BaseSchema):
    """Diagnostic telemetry indicating service health and operational posture."""

    status: str = Field(default="healthy", description="Operational status.")
    app_name: str = Field(..., description="Registered application name.")
    active_environment: str = Field(..., description="Current running environment.")
    host: str = Field(..., description="Server binding host/hash interface.")
    port: int = Field(..., description="Server binding port.")
    gemini_configured: bool = Field(..., description="Indicates if Gemini credentials are provisioned.")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Server response timestamp.")


# ==============================================================================
# Real Git Execution Request & Response Contracts (Human-in-the-Loop)
# ==============================================================================

class PublishPRRequest(BaseSchema):
    """Payload for human-validated real Git commit & push execution."""

    target_repo: Optional[str] = Field(
        default=None,
        description="Target GitHub repository in owner/repo format.",
    )
    target_repository: Optional[str] = Field(
        default=None,
        description="Target GitHub repository in owner/repo format sent from frontend.",
    )
    is_preset: Optional[bool] = Field(
        default=False,
        description="Whether the request was triggered via a demo preset.",
    )
    preset_id: Optional[str] = Field(
        default=None,
        description="Identifier of the demo preset if triggered via preset.",
    )
    base_branch: str = Field(
        default="main",
        description="Target base branch to merge into.",
    )
    head_branch: str = Field(
        ...,
        description="Human-approved dynamic head branch to create, commit, and push.",
    )
    title: str = Field(
        ...,
        description="Human-reviewed and approved conventional commit PR title.",
    )
    markdown_body: str = Field(
        ...,
        description="Human-reviewed and approved PR markdown description.",
    )
    author_name: Optional[str] = Field(
        default=None,
        description="Optional author name for the Git commit.",
    )
    author_email: Optional[str] = Field(
        default=None,
        description="Optional author email for the Git commit.",
    )
    github_token: Optional[str] = Field(
        default=None,
        description="Optional user-provided GitHub Personal Access Token for remote repository branch creation.",
    )

    @field_validator("head_branch")
    @classmethod
    def validate_head_branch(cls, v: str) -> str:
        cleaned = v.strip()
        if not cleaned:
            raise ValueError("Head branch cannot be empty.")
        return cleaned


class PublishPRResponse(BaseSchema):
    """Execution telemetry and status from real Git subprocess execution."""

    success: bool = Field(
        ...,
        description="Whether the git operations completed successfully.",
    )
    head_branch: str = Field(
        ...,
        description="The branch that was checked out and pushed.",
    )
    commit_sha: Optional[str] = Field(
        default=None,
        description="SHA of the newly created commit.",
    )
    pr_url: Optional[str] = Field(
        default=None,
        description="Dynamic GitHub PR compare/create URL or live PR URL.",
    )
    execution_logs: List[str] = Field(
        default_factory=list,
        description="Ordered real git terminal execution logs.",
    )
    error: Optional[str] = Field(
        default=None,
        description="Error message if any git step failed.",
    )


# Legacy compatibility aliases
VoiceCommandInput = IngestData
PullRequestOrchestrationResult = SynthesisResponse

