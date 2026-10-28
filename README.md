# MergeWhisper

> **Turn spoken architectural decisions into executable pull requests with live blast-radius auditing.**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110%2B-009688.svg)](https://fastapi.tiangolo.com/)
[![Gemini 3.8 Flash](https://img.shields.io/badge/Google%20GenAI-Gemini%203.8%20Flash-4285F4.svg)](https://ai.google.dev/)
[![Wispr Flow](https://img.shields.io/badge/Voice%20Engine-Wispr%20Flow-6366F1.svg)](https://wisprflow.ai/)
[![Vercel Deployment](https://img.shields.io/badge/Deploy-Vercel%20Serverless-black.svg)](https://vercel.com/)

---

## Value Proposition

Engineers waste hours writing boilerplate PRs and chasing DevOps approvals. **MergeWhisper** turns spoken architectural decisions into executable pull requests with live blast-radius auditing.

By fusing real-time voice dictation (powered by **Wispr Flow**) with raw syntax changes (`git diff`), MergeWhisper autonomously synthesizes production-ready pull requests, audits system blast radius, formulates rollback runbooks, enforces SemVer bumps, and prepares immutable deployment artifacts in under three seconds.

---

## Key Enterprise Features

### 1. Voice-Driven Intent (Wispr Flow Native)
- Dictate complex code modifications, architectural decisions, and release notes naturally using Wispr Flow.
- Includes a dedicated **Universal Developer Profile** generator that primes the Wispr dictation vocabulary with exact programming acronyms (e.g., `camelCase`, `SemVer`, `Pydantic`, `Kubernetes`, `FastAPI`, `cURL`) for flawless transcription accuracy.

### 2. Real-Time WebSocket Orchestration & Serverless Fallback
- Dual-mode transport layer: connects via zero-latency **WebSocket streaming** (`/ws/synthesize`) for live, step-by-step pipeline execution feedback.
- Features dynamic client-side host resolution (`window.location.host`) with an automated **HTTP POST fallback** (`/api/synthesize`) designed specifically for serverless edge platforms like Vercel.

### 3. DevSecOps Fatal Halt (Automated Circuit Breaker)
- Automated AST and heuristic security inspection embedded in the synthesis pipeline.
- Automatically detects plaintext secrets, unredacted AWS/GCP access keys, and destructive database migrations (e.g., `DROP TABLE`, unindexed scans).
- Instantly halts downstream shipment, freezes the pull request generator, and alerts the engineering security team.

### 4. Human-in-the-Loop Review Gate & Direct Remote Git Publishing
- **Launch Control Review Gate**: Inspect and validate synthesized branch targets, conventional PR titles, and markdown descriptions in a 60/40 asymmetric dashboard.
- **Isolated Ephemeral Workspaces**: Directly executes headless Git subprocesses (`git clone`, `checkout -B`, `commit`, and `push`) inside isolated temporary environments with token-authenticated GitHub remote dispatch.

### 5. Immutable Release Artifacts
- **Conventional Pull Requests**: Generates structured titles, detailed context, and blast-radius summaries.
- **Exportable Markdown**: Single-click `.md` manifest download for compliance tracking and internal knowledge bases.
- **Slack Block Kit**: Ready-to-broadcast payloads formatted for immediate dispatch to engineering channels (`#eng-deployments`).
- **Issue Tracker Webhooks**: Generates validated cURL payloads for Linear GraphQL and Jira REST v3 endpoints.

---

## 5-Stage Release Pipeline Architecture

| Stage | Name | Core Responsibility | Output Artifact |
| :--- | :--- | :--- | :--- |
| **01** | **Ingest** | Ingests developer voice intent & git diff context | Validated Pydantic v2 Ingest Payload |
| **02** | **Audit** | SRE blast-radius assessment, rollback runbook & security scan | Risk Classification (`LOW` / `MED` / `CRITICAL`) |
| **03** | **Verify** | QA test matrix, edge cases, and deterministic verification steps | Automated QA Checklist |
| **04** | **Changelog** | SemVer evaluation (`MAJOR` / `MINOR` / `PATCH`) & Keep-a-Changelog Markdown | Release Notes Delta |
| **05** | **Ship** | Final PR synthesis, Human-in-the-Loop validation, and Git remote push | Live GitHub PR, Markdown Export, Slack Block Kit |

---

## Architecture & Tech Stack

```mermaid
flowchart LR
    A[Engineer Voice via Wispr Flow] --> B[Developer Console UI]
    C[Git Diff Context] --> B
    B -->|WebSocket / HTTP Fallback| D[FastAPI ASGI Gateway]
    D -->|Pydantic v2 Strict Schema| E[MergeWhisper Orchestrator]
    E -->|Structured Prompting| F[Google GenAI: Gemini 3.8 Flash]
    F -->|Validated SynthesisResponse| E
    E -->|JSON Stream / Payloads| B
    B --> G[GitHub PR Draft]
    B --> H[Slack Block Kit]
    B --> I[Downloadable .md]
```

### Frontend
- **Design Philosophy**: Strict 4px spatial physics, minimalist typography, and **Launch Control** dual-theme architecture (warm cream canvas `#f9faf9` in Light Mode, obsidian canvas `#0c0d12` in Dark Mode, Terminal Graphite Orange `#f58e0a` accents).
- **Stack**: HTML5, Vanilla JavaScript (ES2024), Tailwind CSS (JIT compiler via CDN).
- **Icons**: Lucide Icons.

### Backend
- **Engine**: Python 3.10+, FastAPI (Asynchronous ASGI).
- **Model SDK**: Google GenAI SDK (`google-genai`) running `gemini-3.8-flash`.
- **Validation**: Pydantic v2 strict typing and schema serialization.
- **Server**: Uvicorn with ASGI WebSocket protocols.

### Infrastructure & Deployment
- **Platform**: Vercel Serverless Functions (`@vercel/python`).
- **Configuration**: `vercel.json` routing all ASGI endpoints with a 60-second execution budget.
- **Transport**: Dynamic client resolution (`ws://` / `wss://` with automatic failover to HTTP `/api/synthesize`).

---

## DevSecOps & Zero-Trust Governance

MergeWhisper is engineered with a **Zero-Trust architecture** to ensure sensitive credentials and destructive migrations never reach production pull requests.

```mermaid
flowchart TD
    A[Raw Git Diff + Spoken Intent] --> B[Layer 1: Client-Side Regex Scrubber]
    B -->|Sanitized Payload| C[Layer 2: FastAPI Zero-Memory Gateway]
    C --> D[Layer 3: Gemini AST & Security Analyzer]
    D -->|Safe Diff| E[Greenlit: Synthesize PR & Release Artifacts]
    D -->|Plaintext Key / Destructive DDL| F[🛑 Fatal Halt Triggered]
    F --> G[Freeze PR Shipment]
    F --> H[Generate Emergency Rollback Command]
    F --> I[Dispatch Quarantined Alert to #eng-security]
```

### 1. Dual-Layer Security Firewall
- **Client-Side Pre-Scrubber**: Scans diffs in real time for common secret signatures (JWT tokens, private keys, database connection strings) before network dispatch.
- **Semantic AST Inspection**: Deeply analyzes syntax changes to detect unindexed database scans, unauthorized external network calls, and privileged escalation patterns.

### 2. Fatal Halt Circuit Breaker
When plaintext cloud credentials (e.g., AWS IAM access keys `AKIA...`, GCP service account keys, GitHub PATs) or destructive operations are detected:
- **Pipeline Lockdown**: The synthesis engine locks downstream shipment, preventing pull request creation and webhook execution.
- **Automated Rollback Runbook**: Instantly outputs the exact command line necessary to invalidate the credential (e.g., `aws iam delete-access-key --access-key-id AKIAIOSFODNN7EXAMPLE`).
- **Telemetry Quarantine**: Classifies the risk level as `CRITICAL` and drafts an emergency security incident payload.

### 3. Ephemeral Memory Policy (Zero Data Retention)
- Spoken audio transcripts and raw git diffs exist strictly in volatile RAM for `<3,000ms` during inference and are immediately released upon completion.
- **No Persistent Vector Databases**: Operates without vector storage or embedding caches, eliminating lingering codebase footprints.
- **Zero Training Ingestion**: Model inference calls are executed under enterprise zero-data-retention terms; developer diffs are never retained or used for model training.

---

## Local Setup & Installation

### Prerequisites
- Python 3.10 or higher
- Git
- Google Gemini API Key ([Google AI Studio](https://aistudio.google.com/))
- *(Optional)* GitHub Personal Access Token (`GITHUB_TOKEN`) with `repo` scope for automated PR publishing

### 1. Clone the Repository
```bash
git clone https://github.com/lokebuilds/MergeWhisper.git
cd MergeWhisper
```

### 2. Set Up Virtual Environment
```bash
# On Linux/macOS
python3 -m venv .venv
source .venv/bin/activate

# On Windows (PowerShell)
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 3. Install Production Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables
Create a `.env` file in the project root based on `.env.example`:
```bash
cp .env.example .env
```
Edit `.env` and add your API keys:
```ini
GEMINI_API_KEY=AIzaSyYourGeminiApiKeyHere
GITHUB_TOKEN=ghp_YourGitHubPersonalAccessTokenHere  # Optional: for direct git push
HOST=0.0.0.0
PORT=8000
ACTIVE_ENVIRONMENT=development
APP_NAME=MergeWhisper
```

### 5. Start the Development Server
```bash
uvicorn main:app --reload --port 8000
```
Open your browser at `http://localhost:8000` to launch the MergeWhisper Developer Console.

---

## License

This project is licensed under the [MIT License](LICENSE).
