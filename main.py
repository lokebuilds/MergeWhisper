"""Root Entry Point for MergeWhisper.

Production ASGI application entry point managing lifespan lifecycle events,
registering strict Pydantic v2 API routes, mounting static console assets,
and exposing health and preset telemetry.
"""

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
# Load environment variables from .env file at startup
load_dotenv()

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from app.config import Settings, get_settings
from app.mock_store import PRESETS_DATA
from app.schemas import (
    IngestData,
    PublishPRRequest,
    PublishPRResponse,
    SynthesisResponse,
    SystemHealthResponse,
    VoicePatchRequest,
)
from app.service import MergeWhisperOrchestrator

# Configure structured application logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("mergewhisper.main")

# Directory references
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

# Model definition
GEMINI_MODEL = "gemini-3.1-flash-lite"

# Centralized orchestrator instance
orchestrator = MergeWhisperOrchestrator()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan context manager coordinating startup and shutdown events."""
    settings = get_settings()
    logger.info(
        "Initializing MergeWhisper ASGI Application | App: %s v%s | Env: %s | Host: %s:%s",
        settings.app_name,
        settings.app_version,
        settings.active_environment,
        settings.host,
        settings.port,
    )
    logger.info(
        "External AI Model Status: Google Gemini (%s) is %s",
        GEMINI_MODEL,
        "CONFIGURED (Live Orchestration Active)"
        if orchestrator.is_gemini_available
        else "UNCONFIGURED (Awaiting valid GEMINI_API_KEY in .env)",
    )
    yield
    logger.info("Executing clean ASGI application shutdown and releasing transport resources.")
    await orchestrator.close()


settings = get_settings()

app = FastAPI(
    title=f"{settings.app_name} API",
    version=settings.app_version,
    description="Autonomous Voice-to-PR DevOps Risk Audit & QA Verification Orchestrator",
    lifespan=lifespan,
)

# Cross-Origin Resource Sharing (CORS) for seamless local developer console communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static asset directory
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")


@app.get(
    "/",
    response_class=FileResponse,
    status_code=status.HTTP_200_OK,
    tags=["Console"],
)
async def serve_developer_console() -> FileResponse:
    """Serve the zero-setup single-screen developer console interface."""
    index_file = STATIC_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file, status_code=status.HTTP_200_OK)
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Static developer console index not found.",
    )


@app.get(
    "/health",
    status_code=status.HTTP_200_OK,
    tags=["Diagnostics"],
)
async def warmup_probe() -> Dict[str, str]:
    """Cold-boot warmup probe keeping the serverless instance awake with zero initial latency."""
    return {"status": "hot", "system": "MergeWhisper Active"}


@app.get(
    "/api/health",
    response_model=SystemHealthResponse,
    status_code=status.HTTP_200_OK,
    tags=["Diagnostics"],
)
async def health_check() -> SystemHealthResponse:
    """Return platform operational status, environment configuration, and provider posture."""
    return orchestrator.get_system_health()


@app.get(
    "/api/fallback",
    status_code=status.HTTP_200_OK,
    tags=["Registry"],
)
@app.get(
    "/api/fallback-presets",
    status_code=status.HTTP_200_OK,
    tags=["Registry"],
)
async def list_fallback_presets() -> List[Dict[str, Any]]:
    """Return sample evaluation scenarios for quick form population in the UI."""
    return [dict(p) for p in PRESETS_DATA.values()]


@app.post(
    "/api/synthesize",
    response_model=SynthesisResponse,
    status_code=status.HTTP_200_OK,
    tags=["Orchestration"],
)
@app.post(
    "/api/insights",
    response_model=SynthesisResponse,
    status_code=status.HTTP_200_OK,
    tags=["Orchestration"],
    include_in_schema=False,
)
@app.post(
    "/api/orchestrate",
    response_model=SynthesisResponse,
    status_code=status.HTTP_200_OK,
    tags=["Orchestration"],
    include_in_schema=False,
)
async def synthesize_release_manifest(payload: IngestData) -> SynthesisResponse:
    """Synthesize complete release manifest with the four deliverables and DevOps risk audit."""
    try:
        return await orchestrator.synthesize(payload)
    except ValueError as exc:
        logger.warning("Configuration or validation error during synthesis: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        )
    except Exception as exc:
        logger.error("Synthesis execution failure: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Live Gemini generation failed: {str(exc)}",
        )


@app.websocket("/ws/synthesize")
async def websocket_synthesize(websocket: WebSocket) -> None:
    """Stream release pipeline synthesis status and manifest in real-time over WebSocket."""
    await websocket.accept()
    try:
        raw_payload = await websocket.receive_json()
        payload = IngestData.model_validate(raw_payload)
        async for status_update in orchestrator.synthesize_stream(payload):
            await websocket.send_json(status_update)
    except WebSocketDisconnect:
        logger.info("Client disconnected from /ws/synthesize WebSocket stream.")
    except Exception as exc:
        logger.error("Error in /ws/synthesize stream: %s", exc, exc_info=True)
        try:
            await websocket.send_json({"event": "error", "message": str(exc)})
        except Exception:
            pass
    finally:
        try:
            await websocket.close()
        except Exception:
            pass


@app.post(
    "/api/patch",
    response_model=SynthesisResponse,
    status_code=status.HTTP_200_OK,
    tags=["Orchestration"],
)
async def apply_voice_patch(patch_req: VoicePatchRequest) -> SynthesisResponse:
    """Iteratively mutate active release manifest deliverables via follow-up prompt."""
    try:
        return await orchestrator.patch_manifest(patch_req)
    except ValueError as exc:
        logger.warning("Configuration or validation error during voice patch: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        )
    except Exception as exc:
        logger.error("Voice patch execution failure: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Live Gemini refinement failed: {str(exc)}",
        )


@app.post(
    "/api/publish-pr",
    response_model=PublishPRResponse,
    status_code=status.HTTP_200_OK,
    tags=["DevSecOps"],
)
async def publish_pull_request(payload: PublishPRRequest) -> PublishPRResponse:
    """Execute real Git operations (checkout branch, commit, push) with human-validated PR text."""
    try:
        return await orchestrator.execute_git_publish(payload)
    except Exception as exc:
        logger.error("Git publication execution failure: %s", exc, exc_info=True)
        return PublishPRResponse(
            success=False,
            head_branch=payload.head_branch,
            execution_logs=[f"Execution failed: {str(exc)}"],
            error=str(exc),
        )


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.active_environment.lower() == "development",
    )
