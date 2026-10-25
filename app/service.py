"""Asynchronous Orchestration Service for MergeWhisper.

Coordinates live Gemini 3.8 Flash AI orchestration using the official google-genai SDK,
enforcing strict Pydantic v2 structured output schemas across all 5 stages of the release pipeline.
"""

import asyncio
import json
import logging
import os
import re
import time
import urllib.parse
import uuid
from typing import Any, AsyncGenerator, Dict, Optional

from google import genai
from google.genai import types

from app.config import Settings, get_settings
from app.schemas import (
    AuditData,
    ChangelogData,
    IngestData,
    PipelineStagesManifest,
    ShipData,
    SynthesisResponse,
    SystemHealthResponse,
    VerifyData,
    VoicePatchRequest,
)

logger = logging.getLogger("mergewhisper.orchestrator")

GEMINI_MODEL = "gemini-3.8-flash"


def generate_github_url(
    target_repo: str,
    base_branch: str,
    head_branch: str,
    title: str,
    markdown_body: str,
) -> str:
    """Generate a valid GitHub compare/pull-request URL with query parameters."""
    encoded_title = urllib.parse.quote(title)
    # Truncate markdown body to prevent HTTP 414 URI Too Long errors in browser URL bars
    body_snippet = markdown_body[:1200]
    encoded_body = urllib.parse.quote(body_snippet)
    return (
        f"https://github.com/{target_repo}/compare/{base_branch}...{head_branch}"
        f"?quick_pull=1&title={encoded_title}&body={encoded_body}"
    )


class MergeWhisperOrchestrator:
    """Live AI Orchestrator executing strictly through Google Gemini 3.8 Flash."""

    def __init__(self, settings: Optional[Settings] = None) -> None:
        self.settings: Settings = settings or get_settings()

    def get_api_key(self) -> str:
        """Resolve active Gemini API key from settings or environment."""
        key = (self.settings.gemini_api_key or os.getenv("GEMINI_API_KEY", "")).strip()
        if not key or key == "your_api_key_here":
            raise ValueError(
                "GEMINI_API_KEY is not configured. Please supply a valid Google Gemini API key in your .env file."
            )
        return key

    @property
    def is_gemini_available(self) -> bool:
        """Check if a valid Gemini API key is configured."""
        try:
            self.get_api_key()
            return True
        except ValueError:
            return False

    def get_client(self) -> genai.Client:
        """Instantiate official Google GenAI client."""
        return genai.Client(api_key=self.get_api_key())

    def build_system_prompt(self) -> str:
        """Construct strict system instructions for Google Gemini 3.8 Flash."""
        return (
            "You are an Elite Principal Staff DevOps Engineer, Release Architect, and QA Automation Lead at MergeWhisper.\n"
            "Your objective is to ingest developer spoken intent and raw git diff patches, analyze blast radius boundaries, "
            "and synthesize the 4 downstream release pipeline stages:\n"
            "- Stage 02: Audit (AuditData) - SRE risk assessment & rollback runbook\n"
            "- Stage 03: Verify (VerifyData) - QA verification matrix & test steps\n"
            "- Stage 04: Changelog (ChangelogData) - SemVer evaluation & Keep-a-Changelog\n"
            "- Stage 05: Ship (ShipData) - GitHub PR & Slack release broadcast\n\n"
            "SECURITY DIRECTIVE: If the provided git diff contains raw, unredacted credentials (such as AWS keys, API tokens, or passwords), you MUST set the Stage 02 Audit 'risk_level' exactly to \"CRITICAL\".\n\n"
            "STRICT CONSTRAINTS & FORMATTING RULES:\n"
            "1. Audit (Stage 02): Evaluate risk_level strictly as 'LOW', 'MEDIUM', 'HIGH', or 'CRITICAL'. "
            "If unredacted credentials or secrets exist in the git diff, risk_level MUST be set to 'CRITICAL'. "
            "Set is_breaking_change (boolean). Define blast_radius and an exact executable automated rollback_plan command.\n"
            "2. Verify (Stage 03): State the primary verification objective, ordered step-by-step verification_steps, and critical regression edge_cases.\n"
            "3. Changelog (Stage 04): Compute semver_bump ('PATCH', 'MINOR', or 'MAJOR'), current_version ('v18.2.0'), next_version, and keep_a_changelog_markdown.\n"
            "4. Ship (Stage 05): Provide conventional commit title, architectural summary, comprehensive markdown_body for the PR, channel_name ('eng-releases'), broadcast_summary, and formatted_slack_payload. For Stage 05, generate a realistic cURL command in the 'issue_tracker_curl' field that updates a Linear or Jira ticket with the new release details.\n"
            "5. You MUST return strictly valid JSON matching the PipelineStagesManifest schema."
        )

    async def synthesize_stream(self, payload: IngestData) -> AsyncGenerator[Dict[str, Any], None]:
        """Asynchronously stream developer intent synthesis yielding sequential status updates for steps 1-5."""
        start_time = time.perf_counter()
        client = self.get_client()

        logger.info(
            "Streaming Gemini 3.8 Flash synthesis for repo '%s' (intent: %s...)",
            payload.target_repo,
            payload.intent[:45],
        )

        user_content = (
            f"Developer Spoken Intent: {payload.intent}\n"
            f"Target Repository: {payload.target_repo}\n"
            f"Branch Topology: {payload.head_branch} -> {payload.base_branch}\n"
            f"Declared Tech Stack: {payload.tech_stack}\n"
            f"Raw Git Diff / Patch Logs:\n{payload.diff or 'None provided (voice-only directive)'}"
        )

        # Step 1: Ingesting developer intent & git context
        yield {"event": "step_complete", "step": 1}

        config = types.GenerateContentConfig(
            system_instruction=self.build_system_prompt(),
            response_mime_type="application/json",
            response_schema=PipelineStagesManifest,
            temperature=0.1,
            tools=[],
            automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
        )

        response = await client.aio.models.generate_content(
            model=GEMINI_MODEL,
            contents=user_content,
            config=config,
        )

        raw_text = response.text
        if not raw_text:
            raise ValueError(f"{GEMINI_MODEL} returned empty response.")

        cleaned_json = re.sub(r"^```(?:json)?\s*", "", raw_text.strip(), flags=re.MULTILINE)
        cleaned_json = re.sub(r"\s*```$", "", cleaned_json.strip(), flags=re.MULTILINE)

        parsed_data = json.loads(cleaned_json)
        manifest = PipelineStagesManifest.model_validate(parsed_data)

        # DevSecOps Security Directive Enforcement: If raw unredacted credentials exist in git diff, enforce CRITICAL
        if payload.diff and re.search(
            r"\b(AKIA|ABIA|ACCA|ASIA)[0-9A-Z]{16}\b|\b(sk_live_[a-zA-Z0-9]+|ghp_[a-zA-Z0-9]+)\b|(?:\bpassword\s*=\s*['\"][^'\"]+['\"])",
            payload.diff,
        ):
            manifest.audit.risk_level = "CRITICAL"

        # Step 2: Evaluating DevOps blast radius & risk
        yield {"event": "step_complete", "step": 2}

        # Construct complete Stage 05 ShipData deliverable
        dispatch_url = generate_github_url(
            target_repo=payload.target_repo,
            base_branch=payload.base_branch,
            head_branch=payload.head_branch,
            title=manifest.ship.title,
            markdown_body=manifest.ship.markdown_body,
        )

        # Step 3: Synthesizing conventional commit PR
        yield {"event": "step_complete", "step": 3}

        # Step 4: Computing SemVer bump & changelog
        yield {"event": "step_complete", "step": 4}

        slack_blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"Release: {manifest.ship.title}", "emoji": True},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Repository:*\n`{payload.target_repo}`"},
                    {"type": "mrkdwn", "text": f"*Branches:*\n`{payload.head_branch}` ➔ `{payload.base_branch}`"},
                    {"type": "mrkdwn", "text": f"*SemVer Tier:*\n`{manifest.changelog.semver_bump}`"},
                    {"type": "mrkdwn", "text": f"*Breaking Change:*\n{'⚠️ YES' if manifest.audit.is_breaking_change else '✅ NO'}"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Summary:* {manifest.ship.summary}\n*SemVer:* `{manifest.changelog.next_version}` ({manifest.changelog.semver_bump})",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Automated Rollback Command:*\n```{manifest.audit.rollback_plan}```",
                },
            },
        ]

        # Resolve issue tracker cURL webhook
        fallback_curl = (
            f"curl -X POST https://api.linear.app/graphql \\\n"
            f"  -H 'Content-Type: application/json' \\\n"
            f"  -H 'Authorization: Bearer $LINEAR_API_KEY' \\\n"
            f"  -d '{{\"query\":\"mutation {{ issueUpdate(id: \\\"ENG-402\\\", input: {{ stateId: \\\"completed\\\", description: \\\"Release PR: {dispatch_url} marked as Done\\\" }}) {{ success }} }}\"}}'"
        )
        issue_curl = (
            manifest.ship.issue_tracker_curl.strip()
            if getattr(manifest.ship, "issue_tracker_curl", None) and manifest.ship.issue_tracker_curl.strip()
            else fallback_curl
        )

        ship_stage = ShipData(
            title=manifest.ship.title,
            summary=manifest.ship.summary,
            markdown_body=manifest.ship.markdown_body,
            channel_name=manifest.ship.channel_name,
            broadcast_summary=manifest.ship.broadcast_summary,
            issue_tracker_curl=issue_curl,
            github_dispatch_url=dispatch_url,
            formatted_slack_payload={"channel": manifest.ship.channel_name, "blocks": slack_blocks},
        )

        # Step 5: Formatting Slack Block Kit payload
        yield {"event": "step_complete", "step": 5}

        # Compute precise live execution telemetry
        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
        spoken_words = len(payload.intent.split())
        time_saved = round(spoken_words * 0.25, 2)

        # Assemble Stage 01 IngestData
        ingest_data = IngestData(
            intent=payload.intent,
            diff=payload.diff,
            target_repo=payload.target_repo,
            base_branch=payload.base_branch,
            head_branch=payload.head_branch,
            tech_stack=payload.tech_stack,
            word_count=spoken_words,
        )

        synthesis_result = SynthesisResponse(
            session_id=f"sess_{uuid.uuid4()}",
            latency_ms=elapsed_ms,
            dev_time_saved_minutes=time_saved,
            spoken_word_count=spoken_words,
            ingest=ingest_data,
            audit=manifest.audit,
            verify=manifest.verify,
            changelog=manifest.changelog,
            ship=ship_stage,
        )

        yield {"event": "synthesis_complete", "data": synthesis_result.model_dump()}

    async def synthesize(self, payload: IngestData) -> SynthesisResponse:
        """Asynchronously process developer intent and git diff into the 5-stage release pipeline."""
        final_data = None
        async for msg in self.synthesize_stream(payload):
            if msg.get("event") == "synthesis_complete":
                final_data = msg.get("data")
        if not final_data:
            raise ValueError("Synthesis stream terminated prematurely.")
        return SynthesisResponse.model_validate(final_data)

    async def patch_manifest(self, patch_req: VoicePatchRequest) -> SynthesisResponse:
        """Apply iterative refinement mutation via live Gemini 3.8 Flash."""
        start_time = time.perf_counter()
        client = self.get_client()

        logger.info(
            "Dispatching Gemini 3.8 Flash refinement for session '%s' (instruction: %s)",
            patch_req.session_id,
            patch_req.patch_instruction[:50],
        )

        patch_prompt = (
            f"FOLLOW-UP REFINEMENT DIRECTIVE:\n"
            f"\"{patch_req.patch_instruction}\"\n\n"
            f"CURRENT RELEASE MANIFEST:\n"
            f"Audit: {patch_req.current_response.audit.model_dump_json()}\n"
            f"Verify: {patch_req.current_response.verify.model_dump_json()}\n"
            f"Changelog: {patch_req.current_response.changelog.model_dump_json()}\n"
            f"Ship: {patch_req.current_response.ship.model_dump_json()}\n\n"
            f"Selectively mutate the release deliverables according to the refinement directive above. "
            f"Preserve all other fields and return the complete updated PipelineStagesManifest JSON."
        )

        config = types.GenerateContentConfig(
            system_instruction=self.build_system_prompt(),
            response_mime_type="application/json",
            response_schema=PipelineStagesManifest,
            temperature=0.1,
            tools=[],
            automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
        )

        response = await client.aio.models.generate_content(
            model=GEMINI_MODEL,
            contents=patch_prompt,
            config=config,
        )

        raw_text = response.text
        if not raw_text:
            raise ValueError(f"{GEMINI_MODEL} returned empty response for refinement patch.")

        cleaned_json = re.sub(r"^```(?:json)?\s*", "", raw_text.strip(), flags=re.MULTILINE)
        cleaned_json = re.sub(r"\s*```$", "", cleaned_json.strip(), flags=re.MULTILINE)

        parsed_data = json.loads(cleaned_json)
        manifest = PipelineStagesManifest.model_validate(parsed_data)

        target_repo = "facebook/react"
        base_branch = "main"
        head_branch = "fix/auth-loop"
        if patch_req.current_response.ingest:
            target_repo = patch_req.current_response.ingest.target_repo
            base_branch = patch_req.current_response.ingest.base_branch
            head_branch = patch_req.current_response.ingest.head_branch

        dispatch_url = generate_github_url(
            target_repo=target_repo,
            base_branch=base_branch,
            head_branch=head_branch,
            title=manifest.ship.title,
            markdown_body=manifest.ship.markdown_body,
        )

        slack_blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"Release: {manifest.ship.title}", "emoji": True},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Summary:* {manifest.ship.summary}\n*SemVer:* `{manifest.changelog.next_version}` ({manifest.changelog.semver_bump})",
                },
            },
        ]

        fallback_curl = (
            f"curl -X POST https://api.linear.app/graphql \\\n"
            f"  -H 'Content-Type: application/json' \\\n"
            f"  -H 'Authorization: Bearer $LINEAR_API_KEY' \\\n"
            f"  -d '{{\"query\":\"mutation {{ issueUpdate(id: \\\"ENG-402\\\", input: {{ stateId: \\\"completed\\\", description: \\\"Refined PR: {dispatch_url} marked as Done\\\" }}) {{ success }} }}\"}}'"
        )
        issue_curl = (
            manifest.ship.issue_tracker_curl.strip()
            if getattr(manifest.ship, "issue_tracker_curl", None) and manifest.ship.issue_tracker_curl.strip()
            else fallback_curl
        )

        ship_stage = ShipData(
            title=manifest.ship.title,
            summary=manifest.ship.summary,
            markdown_body=manifest.ship.markdown_body,
            channel_name=manifest.ship.channel_name,
            broadcast_summary=manifest.ship.broadcast_summary,
            issue_tracker_curl=issue_curl,
            github_dispatch_url=dispatch_url,
            formatted_slack_payload={"channel": manifest.ship.channel_name, "blocks": slack_blocks},
        )

        elapsed_ms = round((time.perf_counter() - start_time) * 1000 + patch_req.current_response.latency_ms, 2)
        total_words = patch_req.current_response.spoken_word_count + len(patch_req.patch_instruction.split())
        time_saved = round(total_words * 0.25, 2)

        return SynthesisResponse(
            session_id=f"sess_{uuid.uuid4()}",
            latency_ms=elapsed_ms,
            dev_time_saved_minutes=time_saved,
            spoken_word_count=total_words,
            ingest=patch_req.current_response.ingest,
            audit=manifest.audit,
            verify=manifest.verify,
            changelog=manifest.changelog,
            ship=ship_stage,
        )


    def get_system_health(self) -> SystemHealthResponse:
        """Produce system diagnostic telemetry."""
        return SystemHealthResponse(
            status="operational",
            app_name=self.settings.app_name,
            active_environment=self.settings.active_environment,
            host=self.settings.host,
            port=self.settings.port,
            gemini_configured=self.is_gemini_available,
        )

    async def close(self) -> None:
        """Gracefully release any transport resources if active."""
        pass


# Global singleton instance provider
_orchestrator_service: Optional[MergeWhisperOrchestrator] = None


def get_orchestration_service() -> MergeWhisperOrchestrator:
    """Dependency provider for FastAPI route injection."""
    global _orchestrator_service
    if _orchestrator_service is None:
        _orchestrator_service = MergeWhisperOrchestrator()
    return _orchestrator_service
