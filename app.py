"""
================================================================================
  PyDeploy - Mini Python Deployment Platform
  Similar to Render/Heroku, specialized for Python apps.
  
  Single-file FastAPI backend with:
  - Supabase integration (auth, DB, storage)
  - ZIP upload & framework detection
  - Automated build & deployment engine
  - Process management & log capture
  - Web dashboard UI
================================================================================
"""

# ──────────────────────────────────────────────────────────────────────────────
# SECTION 1: IMPORTS
# ──────────────────────────────────────────────────────────────────────────────

import asyncio
import hashlib
import io
import json
import logging
import mimetypes
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import traceback
import uuid
import zipfile
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiofiles
import httpx
import psutil
from dotenv import load_dotenv
from fastapi import (
    BackgroundTasks,
    Cookie,
    Depends,
    FastAPI,
    File,
    Form,
    HTTPException,
    Request,
    Response,
    UploadFile,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from supabase import Client, create_client

# ──────────────────────────────────────────────────────────────────────────────
# SECTION 2: CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────

load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pydeploy")

# Environment variables
SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY: str = os.environ.get("SUPABASE_KEY", "")
SUPABASE_SERVICE_KEY: str = os.environ.get("SUPABASE_SERVICE_KEY", SUPABASE_KEY)
SECRET_KEY: str = os.environ.get("SECRET_KEY", "change-me-in-production-" + uuid.uuid4().hex)
PORT: int = int(os.environ.get("PORT", "8000"))
BASE_DEPLOY_DIR: str = os.environ.get("BASE_DEPLOY_DIR", "/tmp/pydeploy_apps")
MAX_ZIP_SIZE_MB: int = int(os.environ.get("MAX_ZIP_SIZE_MB", "100"))
MAX_UNZIPPED_SIZE_MB: int = int(os.environ.get("MAX_UNZIPPED_SIZE_MB", "500"))
BASE_APP_PORT: int = int(os.environ.get("BASE_APP_PORT", "9000"))
MAX_CONCURRENT_DEPLOYMENTS: int = int(os.environ.get("MAX_CONCURRENT_DEPLOYMENTS", "10"))
STORAGE_BUCKET: str = os.environ.get("STORAGE_BUCKET", "pydeploy-projects")
PLATFORM_TITLE: str = "PyDeploy"

# ── AI Agent config ────────────────────────────────────────────────────────
# Primary: Google Gemini API (gemini-3.1-pro-preview-customtools)
#   → purpose-built for agentic workflows with custom tools
# Fallback: OpenRouter free models
GEMINI_API_KEY: str = os.environ.get("GEMINI_API_KEY", "")
GEMINI_BASE: str = "https://generativelanguage.googleapis.com/v1beta/models"
GEMINI_PRIMARY: str = "gemini-3.1-pro-preview-customtools"  # best for custom tool agents
GEMINI_FALLBACK: str = "gemini-3.1-pro-preview"             # same model, general endpoint

# OpenRouter fallbacks (used if Gemini fails)
OPENROUTER_API_KEY: str = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_BASE: str = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODELS: List[str] = [
    "openrouter/auto",
    "meta-llama/llama-3.3-70b-instruct:free",
    "mistralai/devstral-2512:free",
    "google/gemini-2.0-flash-exp:free",
    "deepseek/deepseek-r1-0528:free",
]
AI_MAX_ROUNDS: int = 50          # generous — agent should never run out of rounds
AI_CMD_TIMEOUT: int = 180        # seconds per shell command
AI_API_RETRIES: int = 3          # retries per model on transient errors
AI_RETRY_DELAY: float = 2.0      # base seconds between retries (doubles each time)

# Deployment states
class DeployState:
    PENDING    = "pending"
    BUILDING   = "building"
    RUNNING    = "running"
    FAILED     = "failed"
    STOPPED    = "stopped"
    RESTARTING = "restarting"

# ──────────────────────────────────────────────────────────────────────────────
# SECTION 3: SUPABASE CLIENT
# ──────────────────────────────────────────────────────────────────────────────

_supabase_client: Optional[Client] = None
_supabase_service_client: Optional[Client] = None


def get_supabase() -> Client:
    """Return or create the shared Supabase client (anon key)."""
    global _supabase_client
    if _supabase_client is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise RuntimeError("SUPABASE_URL and SUPABASE_KEY env vars must be set.")
        _supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _supabase_client


def get_supabase_service() -> Client:
    """Return or create the Supabase service-role client (bypasses RLS)."""
    global _supabase_service_client
    if _supabase_service_client is None:
        if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
            raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_KEY env vars must be set.")
        _supabase_service_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    return _supabase_service_client


def _reset_supabase_clients():
    """Force-recreate Supabase clients (called after SSL/connection errors)."""
    global _supabase_client, _supabase_service_client
    _supabase_client = None
    _supabase_service_client = None


async def _sb_execute(fn, retries: int = 3, label: str = "db"):
    """
    Run a synchronous Supabase call in a thread, with retry + SSL error recovery.
    Wraps EVERY DB call so a transient SSL hiccup never crashes a route handler.
    fn: zero-arg callable that performs the Supabase .execute() and returns result
    """
    import httpx as _httpx
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            # Run the blocking Supabase call in a thread pool (non-blocking)
            result = await asyncio.get_event_loop().run_in_executor(None, fn)
            return result
        except (_httpx.ConnectError, _httpx.RemoteProtocolError,
                _httpx.ReadError, _httpx.WriteError, _httpx.TimeoutException,
                OSError) as exc:
            last_exc = exc
            # SSL EOF / connection error: reset client so next call re-connects
            _reset_supabase_clients()
            wait = 0.5 * (2 ** (attempt - 1))   # 0.5s, 1s, 2s
            logger.warning(f"[{label}] network error (attempt {attempt}/{retries}): {exc} — retry in {wait}s")
            await asyncio.sleep(wait)
        except Exception as exc:
            last_exc = exc
            logger.warning(f"[{label}] unexpected error (attempt {attempt}/{retries}): {exc}")
            await asyncio.sleep(0.5)
    # All retries exhausted — raise so callers can handle gracefully
    raise RuntimeError(f"[{label}] failed after {retries} retries: {last_exc}") from last_exc


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 4: DATABASE HELPERS
# ──────────────────────────────────────────────────────────────────────────────

async def db_init_tables():
    """
    Ensure all required tables exist in Supabase Postgres.
    Uses the service client to run raw SQL via the REST API.
    NOTE: In production, prefer running migrations via Supabase dashboard.
    """
    sb = get_supabase_service()

    # We use Supabase's rpc to run SQL.  Create a stored proc if needed,
    # or rely on the dashboard.  Here we attempt table creation gracefully.

    sql_statements = [
        """
        CREATE TABLE IF NOT EXISTS users (
            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            email       TEXT UNIQUE NOT NULL,
            password    TEXT NOT NULL,
            username    TEXT,
            created_at  TIMESTAMPTZ DEFAULT now(),
            updated_at  TIMESTAMPTZ DEFAULT now()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS projects (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id         UUID REFERENCES users(id) ON DELETE CASCADE,
            name            TEXT NOT NULL,
            description     TEXT,
            framework       TEXT,
            zip_path        TEXT,
            deploy_dir      TEXT,
            status          TEXT DEFAULT 'pending',
            startup_cmd     TEXT,
            env_vars        JSONB DEFAULT '{}',
            port            INTEGER,
            created_at      TIMESTAMPTZ DEFAULT now(),
            updated_at      TIMESTAMPTZ DEFAULT now()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS deployments (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            project_id      UUID REFERENCES projects(id) ON DELETE CASCADE,
            user_id         UUID REFERENCES users(id) ON DELETE CASCADE,
            status          TEXT DEFAULT 'pending',
            framework       TEXT,
            startup_cmd     TEXT,
            pid             INTEGER,
            port            INTEGER,
            error_msg       TEXT,
            build_output    TEXT,
            started_at      TIMESTAMPTZ,
            finished_at     TIMESTAMPTZ,
            created_at      TIMESTAMPTZ DEFAULT now()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS logs (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            deployment_id   UUID REFERENCES deployments(id) ON DELETE CASCADE,
            project_id      UUID REFERENCES projects(id) ON DELETE CASCADE,
            level           TEXT DEFAULT 'info',
            message         TEXT NOT NULL,
            source          TEXT DEFAULT 'system',
            created_at      TIMESTAMPTZ DEFAULT now()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS sessions (
            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id     UUID REFERENCES users(id) ON DELETE CASCADE,
            token       TEXT UNIQUE NOT NULL,
            expires_at  TIMESTAMPTZ NOT NULL,
            created_at  TIMESTAMPTZ DEFAULT now()
        );
        """,
    ]

    for stmt in sql_statements:
        try:
            sb.rpc("exec_sql", {"query": stmt}).execute()
        except Exception:
            # Tables might already exist or rpc not available — skip silently
            pass


async def db_create_user(email: str, password_hash: str, username: str) -> Dict:
    payload = {"email": email, "password": password_hash, "username": username}
    result = await _sb_execute(lambda: get_supabase_service().table("users").insert(payload).execute(), label="db_create_user")
    return result.data[0] if result.data else {}


async def db_get_user_by_email(email: str) -> Optional[Dict]:
    try:
        result = await _sb_execute(lambda: get_supabase_service().table("users").select("*").eq("email", email).limit(1).execute(), label="db_get_user_by_email")
        return result.data[0] if result.data else None
    except Exception as exc:
        logger.warning(f"db_get_user_by_email failed: {exc}"); return None


async def db_get_user_by_id(user_id: str) -> Optional[Dict]:
    try:
        result = await _sb_execute(lambda: get_supabase_service().table("users").select("*").eq("id", user_id).limit(1).execute(), label="db_get_user_by_id")
        return result.data[0] if result.data else None
    except Exception as exc:
        logger.warning(f"db_get_user_by_id failed: {exc}"); return None


async def db_create_session(user_id: str) -> str:
    token = uuid.uuid4().hex + uuid.uuid4().hex
    expires = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
    payload = {"user_id": user_id, "token": token, "expires_at": expires}
    await _sb_execute(lambda: get_supabase_service().table("sessions").insert(payload).execute(), label="db_create_session")
    return token


# ── In-memory session cache ────────────────────────────────────────────────
# Stores {token: (session_dict, cached_at_timestamp)}
# Entries are reused for SESSION_CACHE_TTL seconds before re-validating with
# Supabase. On logout the entry is evicted immediately.
_session_cache: Dict[str, tuple] = {}
SESSION_CACHE_TTL: int = int(os.environ.get("SESSION_CACHE_TTL", "60"))  # seconds


def _session_cache_evict_expired():
    """Remove stale entries (called lazily on each cache access)."""
    now = time.monotonic()
    stale = [t for t, (_, cached_at) in _session_cache.items()
             if now - cached_at > SESSION_CACHE_TTL]
    for t in stale:
        del _session_cache[t]


async def db_get_session(token: str) -> Optional[Dict]:
    # 1. Check in-memory cache first
    _session_cache_evict_expired()
    cached = _session_cache.get(token)
    if cached is not None:
        session_data, cached_at = cached
        if time.monotonic() - cached_at <= SESSION_CACHE_TTL:
            return session_data  # no Supabase call

    # 2. Cache miss — hit Supabase (resilient)
    expires = datetime.now(timezone.utc).isoformat()
    def _fetch():
        sb = get_supabase_service()
        return sb.table("sessions").select("*, users(*)").eq("token", token).gt("expires_at", expires).limit(1).execute()
    try:
        result = await _sb_execute(_fetch, label="db_get_session")
        session_data = result.data[0] if result.data else None
    except Exception as exc:
        logger.warning(f"db_get_session failed: {exc}")
        return None

    # 3. Populate cache (even None, so we dont hammer Supabase for invalid tokens)
    _session_cache[token] = (session_data, time.monotonic())
    return session_data


async def db_delete_session(token: str):
    _session_cache.pop(token, None)
    try:
        await _sb_execute(lambda: get_supabase_service().table("sessions").delete().eq("token", token).execute(), label="db_delete_session")
    except Exception: pass


async def db_create_project(user_id: str, name: str, description: str = "") -> Dict:
    payload = {"user_id": user_id, "name": name, "description": description, "status": DeployState.PENDING}
    result = await _sb_execute(lambda: get_supabase_service().table("projects").insert(payload).execute(), label="db_create_project")
    return result.data[0] if result.data else {}


async def db_update_project(project_id: str, updates: Dict) -> Dict:
    updates["updated_at"] = datetime.now(timezone.utc).isoformat()
    upd = dict(updates)
    try:
        result = await _sb_execute(lambda: get_supabase_service().table("projects").update(upd).eq("id", project_id).execute(), label="db_update_project")
        return result.data[0] if result.data else {}
    except Exception as exc:
        logger.warning(f"db_update_project failed: {exc}"); return {}


async def db_get_project(project_id: str) -> Optional[Dict]:
    try:
        result = await _sb_execute(lambda: get_supabase_service().table("projects").select("*").eq("id", project_id).limit(1).execute(), label="db_get_project")
        return result.data[0] if result.data else None
    except Exception as exc:
        logger.warning(f"db_get_project failed: {exc}"); return None


async def db_list_projects(user_id: str) -> List[Dict]:
    try:
        result = await _sb_execute(lambda: get_supabase_service().table("projects").select("*").eq("user_id", user_id).order("created_at", desc=True).execute(), label="db_list_projects")
        return result.data or []
    except Exception as exc:
        logger.warning(f"db_list_projects failed: {exc}"); return []


async def db_delete_project(project_id: str):
    try:
        await _sb_execute(lambda: get_supabase_service().table("projects").delete().eq("id", project_id).execute(), label="db_delete_project")
    except Exception: pass


async def db_create_deployment(project_id: str, user_id: str, framework: str, startup_cmd: str) -> Dict:
    payload = {"project_id": project_id, "user_id": user_id, "framework": framework,
               "startup_cmd": startup_cmd, "status": DeployState.PENDING,
               "started_at": datetime.now(timezone.utc).isoformat()}
    result = await _sb_execute(lambda: get_supabase_service().table("deployments").insert(payload).execute(), label="db_create_deployment")
    return result.data[0] if result.data else {}


async def db_update_deployment(deployment_id: str, updates: Dict) -> Dict:
    upd = dict(updates)
    try:
        result = await _sb_execute(lambda: get_supabase_service().table("deployments").update(upd).eq("id", deployment_id).execute(), label="db_update_deployment")
        return result.data[0] if result.data else {}
    except Exception as exc:
        logger.warning(f"db_update_deployment failed: {exc}"); return {}


async def db_get_deployment(deployment_id: str) -> Optional[Dict]:
    try:
        result = await _sb_execute(lambda: get_supabase_service().table("deployments").select("*").eq("id", deployment_id).limit(1).execute(), label="db_get_deployment")
        return result.data[0] if result.data else None
    except Exception as exc:
        logger.warning(f"db_get_deployment failed: {exc}"); return None


async def db_list_deployments(project_id: str) -> List[Dict]:
    try:
        result = await _sb_execute(lambda: get_supabase_service().table("deployments").select("*").eq("project_id", project_id).order("created_at", desc=True).limit(20).execute(), label="db_list_deployments")
        return result.data or []
    except Exception as exc:
        logger.warning(f"db_list_deployments failed: {exc}"); return []


# ── Log batching ────────────────────────────────────────────────────────────
# Buffer log lines and flush them in batches to avoid hammering Supabase
# with one HTTP POST per line (a 100-line pip install = 100 API calls).
_log_buffer: Dict[str, List[Dict]] = {}        # deployment_id → [rows]
_log_flush_tasks: Dict[str, asyncio.Task] = {} # deployment_id → pending flush task
LOG_BATCH_WINDOW: float = 0.4                  # seconds to accumulate before flushing
LOG_BATCH_MAX: int = 50                        # flush immediately at this many rows


async def _flush_log_buffer(deployment_id: str):
    """Write buffered log rows to Supabase in a single batch insert."""
    await asyncio.sleep(LOG_BATCH_WINDOW)
    rows = _log_buffer.pop(deployment_id, [])
    _log_flush_tasks.pop(deployment_id, None)
    if not rows:
        return
    try:
        rows_copy = list(rows)
        await _sb_execute(lambda: get_supabase_service().table("logs").insert(rows_copy).execute(), label="log_batch_flush")
    except Exception as e:
        logger.warning(f"Batch log flush failed ({len(rows)} rows): {e}")


async def db_add_log(deployment_id: str, project_id: str, message: str,
                     level: str = "info", source: str = "system"):
    """Buffer a log line; flush to Supabase in batches to minimise API calls."""
    row = {
        "deployment_id": deployment_id,
        "project_id": project_id,
        "level": level,
        "message": message[:4000],
        "source": source,
    }

    buf = _log_buffer.setdefault(deployment_id, [])
    buf.append(row)

    # Immediate flush when buffer is large
    if len(buf) >= LOG_BATCH_MAX:
        rows = _log_buffer.pop(deployment_id, [])
        old_task = _log_flush_tasks.pop(deployment_id, None)
        if old_task and not old_task.done():
            old_task.cancel()
        try:
            rows_copy = list(rows)
            await _sb_execute(lambda: get_supabase_service().table("logs").insert(rows_copy).execute(), label="log_immediate_flush")
        except Exception as e:
            logger.warning(f"Immediate log flush failed: {e}")
        return

    # Otherwise schedule a deferred flush (debounced)
    old_task = _log_flush_tasks.get(deployment_id)
    if old_task and not old_task.done():
        return  # already scheduled
    _log_flush_tasks[deployment_id] = asyncio.create_task(
        _flush_log_buffer(deployment_id)
    )


async def db_flush_logs(deployment_id: str):
    """Force-flush any remaining buffered logs for a deployment."""
    rows = _log_buffer.pop(deployment_id, [])
    old_task = _log_flush_tasks.pop(deployment_id, None)
    if old_task and not old_task.done():
        old_task.cancel()
    if not rows:
        return
    try:
        rows_copy = list(rows)
        await _sb_execute(lambda: get_supabase_service().table("logs").insert(rows_copy).execute(), label="log_final_flush")
    except Exception as e:
        logger.warning(f"Final log flush failed: {e}")


async def db_get_logs(deployment_id: str, limit: int = 200) -> List[Dict]:
    try:
        lim = limit
        result = await _sb_execute(lambda: get_supabase_service().table("logs").select("*").eq("deployment_id", deployment_id).order("created_at", desc=False).limit(lim).execute(), label="db_get_logs")
        return result.data or []
    except Exception as exc:
        logger.warning(f"db_get_logs failed: {exc}"); return []


async def db_get_project_logs(project_id: str, limit: int = 200) -> List[Dict]:
    try:
        lim = limit
        result = await _sb_execute(lambda: get_supabase_service().table("logs").select("*").eq("project_id", project_id).order("created_at", desc=False).limit(lim).execute(), label="db_get_project_logs")
        return result.data or []
    except Exception as exc:
        logger.warning(f"db_get_project_logs failed: {exc}"); return []


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 5: STORAGE HELPERS (Supabase Storage)
# ──────────────────────────────────────────────────────────────────────────────

async def storage_ensure_bucket():
    """Create the storage bucket if it doesn't exist."""
    try:
        sb = get_supabase_service()
        buckets = sb.storage.list_buckets()
        bucket_names = [b.name for b in buckets]
        if STORAGE_BUCKET not in bucket_names:
            sb.storage.create_bucket(STORAGE_BUCKET, options={"public": False})
            logger.info(f"Created storage bucket: {STORAGE_BUCKET}")
    except Exception as e:
        logger.warning(f"Could not ensure storage bucket: {e}")


async def storage_upload_zip(project_id: str, file_bytes: bytes, filename: str) -> str:
    """Upload ZIP file to Supabase Storage. Returns the storage path."""
    try:
        sb = get_supabase_service()
        path = f"{project_id}/{filename}"
        sb.storage.from_(STORAGE_BUCKET).upload(
            path,
            file_bytes,
            file_options={"content-type": "application/zip"},
        )
        return path
    except Exception as e:
        logger.error(f"Storage upload failed: {e}")
        return ""


async def storage_get_zip_url(storage_path: str) -> str:
    """Get a signed URL for downloading the ZIP."""
    try:
        sb = get_supabase_service()
        result = sb.storage.from_(STORAGE_BUCKET).create_signed_url(storage_path, 3600)
        return result.get("signedURL", "")
    except Exception as e:
        logger.error(f"Failed to get signed URL: {e}")
        return ""


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 6: SECURITY & AUTH HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    """Simple SHA-256 password hash (use bcrypt in production)."""
    salt = SECRET_KEY[:16]
    return hashlib.sha256(f"{salt}{password}".encode()).hexdigest()


def verify_password(password: str, hashed: str) -> bool:
    return hash_password(password) == hashed


async def get_current_user(request: Request) -> Optional[Dict]:
    """Extract user from session cookie."""
    token = request.cookies.get("session_token")
    if not token:
        return None
    session = await db_get_session(token)
    if not session:
        return None
    return session.get("users")


async def require_user(request: Request) -> Dict:
    """Dependency: require authenticated user or redirect to login."""
    user = await get_current_user(request)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            headers={"Location": "/login"},
        )
    return user


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 7: FRAMEWORK DETECTION
# ──────────────────────────────────────────────────────────────────────────────

class FrameworkDetector:
    """
    Deep project analyzer — handles every kind of Python project:
    FastAPI, Flask, Django, Tornado, aiohttp, Bottle, Falcon,
    plain scripts, bots, workers, scrapers, static/HTML sites, etc.
    """

    FRAMEWORK_SIGNATURES = {
        "fastapi":    ["fastapi"],
        "flask":      ["flask"],
        "django":     ["django", "djangorestframework"],
        "starlette":  ["starlette"],
        "tornado":    ["tornado"],
        "aiohttp":    ["aiohttp"],
        "bottle":     ["bottle"],
        "falcon":     ["falcon"],
        "sanic":      ["sanic"],
        "litestar":   ["litestar", "starlite"],
        "quart":      ["quart"],
        "blacksheep": ["blacksheep"],
        "grpc":       ["grpcio"],
        "celery":     ["celery"],
        "dramatiq":   ["dramatiq"],
        "rq":         ["rq"],
        "telegram":   ["python-telegram-bot", "aiogram", "telebot", "pyrogram", "telethon"],
        "discord":    ["discord.py", "nextcord", "disnake", "hikari"],
        "slack":      ["slack-sdk", "slack-bolt"],
        "twilio":     ["twilio"],
        "scrapy":     ["scrapy"],
        "playwright": ["playwright"],
        "selenium":   ["selenium"],
        "streamlit":  ["streamlit"],
        "gradio":     ["gradio"],
        "dash":       ["dash"],
        "panel":      ["panel"],
    }

    # Web frameworks that bind to a port  
    WEB_FRAMEWORKS = {"fastapi","flask","django","starlette","tornado","aiohttp",
                      "bottle","falcon","sanic","litestar","quart","blacksheep"}

    # Frameworks that just run as a process (no port binding needed)
    PROCESS_FRAMEWORKS = {"telegram","discord","slack","twilio","scrapy",
                          "playwright","selenium","celery","dramatiq","rq","grpc"}

    # Frameworks that have their own server command
    SERVER_COMMANDS = {
        "streamlit": "streamlit run {entry} --server.port {port} --server.address 0.0.0.0",
        "gradio":    "python {entry}",
        "dash":      "python {entry}",
        "panel":     "panel serve {entry} --address 0.0.0.0 --port {port}",
    }

    ENTRY_PATTERNS = [
        "main.py","app.py","server.py","bot.py","run.py","start.py",
        "wsgi.py","asgi.py","application.py","index.py","api.py",
        "worker.py","__main__.py","launcher.py","handler.py","service.py",
        "gateway.py","relay.py","proxy.py","app/__init__.py","src/main.py",
        "src/app.py","core.py","manage.py",
    ]

    @classmethod
    def detect(cls, project_dir: str) -> Dict[str, Any]:
        result = {
            "framework": "unknown",
            "entry_point": None,
            "startup_cmd": None,
            "has_frontend": False,
            "frontend_type": None,
            "is_frontend_only": False,
            "has_requirements": False,
            "requirements_path": None,
            "has_package_json": False,
            "package_json_path": None,
            "python_version": "3.11",
            "notes": [],
            "project_type": "python",   # python | frontend_only | fullstack
        }

        root = Path(project_dir)

        # ── Check frontend-only (HTML/CSS/JS no Python) ───────────────
        py_files = [f for f in root.rglob("*.py")
                    if not any(p in str(f) for p in [".venv","node_modules","__pycache__"])]
        html_files = list(root.rglob("*.html")) + list(root.rglob("*.htm"))

        if not py_files and html_files:
            result["is_frontend_only"] = True
            result["project_type"] = "frontend_only"
            result["framework"] = "static"
            index = next((f for f in html_files if f.name in ("index.html","index.htm")), html_files[0])
            result["entry_point"] = str(index.relative_to(root))
            result["startup_cmd"] = f"python -m http.server {{PORT:-8000}}"
            result["notes"].append("Frontend-only project detected — serving with Python HTTP server")
            cls._detect_frontend_type(root, result)
            return result

        # ── Requirements.txt ──────────────────────────────────────────
        for req_file in ["requirements.txt","requirements/base.txt",
                         "requirements/production.txt","requirements/main.txt"]:
            req_path = root / req_file
            if req_path.exists():
                result["has_requirements"] = True
                result["requirements_path"] = str(req_path)
                break

        # ── Python version ────────────────────────────────────────────
        for vf in ["runtime.txt",".python-version"]:
            vfile = root / vf
            if vfile.exists():
                txt = vfile.read_text().strip()
                m = re.search(r"(\d+\.\d+)", txt)
                if m:
                    result["python_version"] = m.group(1)
                break

        # ── Detect framework from requirements + imports ──────────────
        req_content = ""
        if result["has_requirements"]:
            try:
                req_content = Path(result["requirements_path"]).read_text().lower()
            except Exception:
                pass

        # Also scan imports in Python files for framework hints
        import_hints = ""
        for py in py_files[:20]:
            try:
                import_hints += py.read_text(errors="replace")[:2000] + "\n"
            except Exception:
                pass
        combined = req_content + "\n" + import_hints.lower()

        framework = "unknown"
        for fw, sigs in cls.FRAMEWORK_SIGNATURES.items():
            if any(sig.lower() in combined for sig in sigs):
                framework = fw
                break
        result["framework"] = framework

        # ── Find entry point ──────────────────────────────────────────
        entry = None
        # Exact filename matches
        for pattern in cls.ENTRY_PATTERNS:
            candidate = root / pattern
            if candidate.exists():
                entry = str(candidate.relative_to(root))
                break

        # One level deep
        if not entry:
            for candidate in root.glob("*/*.py"):
                if candidate.name in [p for p in cls.ENTRY_PATTERNS if "/" not in p]:
                    entry = str(candidate.relative_to(root))
                    break

        # Any .py with __main__ block
        if not entry:
            skip = {".venv","node_modules","__pycache__","test","tests","migrations"}
            for candidate in sorted(root.rglob("*.py")):
                if any(part in skip for part in candidate.parts):
                    continue
                try:
                    txt = candidate.read_text(errors="replace")
                    if '__name__' in txt and '__main__' in txt:
                        entry = str(candidate.relative_to(root))
                        result["notes"].append(f"Entry via __main__ scan: {entry}")
                        break
                except Exception:
                    pass

        # Framework-specific entry search
        if not entry and framework in ("telegram","discord","slack"):
            for pat in ["bot.py","main.py","run.py","client.py"]:
                c = root / pat
                if c.exists():
                    entry = str(c.relative_to(root))
                    break

        # Last resort: first .py at root
        if not entry:
            cands = sorted(f for f in root.glob("*.py")
                          if f.name not in ("setup.py","conftest.py","test_*.py"))
            if cands:
                entry = str(cands[0].relative_to(root))
                result["notes"].append(f"Entry fallback: {entry}")

        result["entry_point"] = entry

        # ── Build startup command ─────────────────────────────────────
        startup_cmd = cls._build_startup_cmd(framework, entry, root)
        result["startup_cmd"] = startup_cmd

        # ── Frontend detection ────────────────────────────────────────
        cls._detect_frontend_type(root, result)

        # ── Django-specific ───────────────────────────────────────────
        if framework == "django":
            result = cls._handle_django(root, result)

        result["project_type"] = "fullstack" if result["has_frontend"] else "python"
        return result

    @classmethod
    def _detect_frontend_type(cls, root: Path, result: Dict):
        """Detect Node/React/Vue/static frontend."""
        for pkg_json in root.rglob("package.json"):
            if "node_modules" in str(pkg_json):
                continue
            result["has_package_json"] = True
            result["package_json_path"] = str(pkg_json)
            try:
                pkg = json.loads(pkg_json.read_text())
                deps = {**pkg.get("dependencies",{}), **pkg.get("devDependencies",{})}
                if "next" in deps:
                    result["frontend_type"] = "nextjs"; result["has_frontend"] = True
                elif "react" in deps or "react-dom" in deps:
                    result["frontend_type"] = "react"; result["has_frontend"] = True
                elif "vue" in deps:
                    result["frontend_type"] = "vue"; result["has_frontend"] = True
                elif "svelte" in deps:
                    result["frontend_type"] = "svelte"; result["has_frontend"] = True
                elif "scripts" in pkg:
                    result["frontend_type"] = "node"; result["has_frontend"] = True
            except Exception:
                pass
            break
        # Static dirs
        for d in ["static","public","frontend","client","dist","build","www","html"]:
            if (root / d).is_dir() and not result["has_frontend"]:
                result["has_frontend"] = True
                result["frontend_type"] = "static"
                break
        # HTML at root
        if not result["has_frontend"] and list(root.glob("*.html")):
            result["has_frontend"] = True
            result["frontend_type"] = "static"

    @classmethod
    def _build_startup_cmd(cls, framework: str, entry: Optional[str], root: Path) -> str:
        port_var = "${PORT:-8000}"

        if framework in ("fastapi","starlette","litestar","blacksheep"):
            if entry:
                module = entry.replace("/",".").replace(".py","")
                app_var = cls._find_app_var(root / entry) or "app"
                return f"uvicorn {module}:{app_var} --host 0.0.0.0 --port {port_var}"
            return f"uvicorn main:app --host 0.0.0.0 --port {port_var}"

        elif framework in ("flask","quart"):
            if entry:
                module = entry.replace("/",".").replace(".py","")
                app_var = cls._find_app_var(root / entry) or "app"
                return f"gunicorn {module}:{app_var} --bind 0.0.0.0:{port_var} --workers 2"
            return f"gunicorn app:app --bind 0.0.0.0:{port_var} --workers 2"

        elif framework == "django":
            return f"gunicorn wsgi:application --bind 0.0.0.0:{port_var} --workers 2"

        elif framework == "tornado":
            return f"python {entry or 'main.py'}"

        elif framework == "aiohttp":
            if entry:
                module = entry.replace("/",".").replace(".py","")
                return f"python -m {module}"
            return "python main.py"

        elif framework == "sanic":
            if entry:
                module = entry.replace("/",".").replace(".py","")
                app_var = cls._find_app_var(root / entry) or "app"
                return f"sanic {module}:{app_var} --host 0.0.0.0 --port {port_var}"
            return f"sanic main:app --host 0.0.0.0 --port {port_var}"

        elif framework == "streamlit":
            return f"streamlit run {entry or 'app.py'} --server.port {port_var} --server.address 0.0.0.0"

        elif framework == "gradio":
            return f"python {entry or 'app.py'}"

        elif framework == "panel":
            return f"panel serve {entry or 'app.py'} --address 0.0.0.0 --port {port_var}"

        elif framework == "grpc":
            return f"python {entry or 'server.py'}"

        elif framework == "static":
            return f"python -m http.server {port_var}"

        else:
            # Generic: bots, workers, scrapers, scripts, etc.
            if entry:
                return f"python {entry}"
            return "python main.py"

    @classmethod
    def _find_app_var(cls, entry_path) -> Optional[str]:
        if not entry_path or not Path(str(entry_path)).exists():
            return None
        try:
            content = Path(str(entry_path)).read_text()
            for var in ["application","app","create_app","APP","server","api"]:
                if re.search(rf"^{var}\s*=", content, re.MULTILINE):
                    return var
        except Exception:
            pass
        return None

    @classmethod
    def _handle_django(cls, root: Path, result: Dict) -> Dict:
        manage_files = list(root.glob("**/manage.py"))
        if not manage_files:
            return result
        django_root = manage_files[0].parent
        wsgi_files = list(django_root.glob("**/wsgi.py"))
        if wsgi_files:
            wsgi = wsgi_files[0]
            module = ".".join(wsgi.relative_to(django_root).parts).replace(".py","")
            result["startup_cmd"] = f"gunicorn {module}:application --bind 0.0.0.0:${{PORT:-8000}} --workers 2"
        else:
            settings_files = list(django_root.glob("**/settings.py"))
            if settings_files:
                pname = settings_files[0].parent.name
                result["startup_cmd"] = f"gunicorn {pname}.wsgi:application --bind 0.0.0.0:${{PORT:-8000}} --workers 2"
        return result


# SECTION 8: SECURITY — ZIP VALIDATION
# ──────────────────────────────────────────────────────────────────────────────

class ZipValidator:
    """Validates uploaded ZIP files to prevent zip bombs and path traversal."""

    MAX_FILES = 5000
    MAX_SINGLE_FILE_MB = 100

    @classmethod
    def validate(cls, zip_bytes: bytes) -> Dict[str, Any]:
        """
        Returns {"ok": bool, "error": str|None, "file_count": int, "total_size": int}
        """
        max_zip_bytes = MAX_ZIP_SIZE_MB * 1024 * 1024
        if len(zip_bytes) > max_zip_bytes:
            return {"ok": False, "error": f"ZIP exceeds {MAX_ZIP_SIZE_MB} MB limit."}

        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                namelist = zf.namelist()

                if len(namelist) > cls.MAX_FILES:
                    return {"ok": False, "error": f"ZIP contains too many files ({len(namelist)} > {cls.MAX_FILES})."}

                total_size = 0
                for info in zf.infolist():
                    # Path traversal check
                    if ".." in info.filename or info.filename.startswith("/"):
                        return {"ok": False, "error": f"Path traversal detected: {info.filename}"}

                    # Zip bomb check: file_size is the UNCOMPRESSED size
                    if info.file_size > cls.MAX_SINGLE_FILE_MB * 1024 * 1024:
                        return {"ok": False, "error": f"Single file too large: {info.filename}"}

                    total_size += info.file_size

                max_unzipped = MAX_UNZIPPED_SIZE_MB * 1024 * 1024
                if total_size > max_unzipped:
                    return {
                        "ok": False,
                        "error": f"Unzipped size exceeds {MAX_UNZIPPED_SIZE_MB} MB limit.",
                    }

                return {
                    "ok": True,
                    "error": None,
                    "file_count": len(namelist),
                    "total_size": total_size,
                }

        except zipfile.BadZipFile:
            return {"ok": False, "error": "Invalid or corrupted ZIP file."}
        except Exception as e:
            return {"ok": False, "error": f"ZIP validation error: {e}"}


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 9: BUILD SYSTEM
# ──────────────────────────────────────────────────────────────────────────────

class BuildSystem:
    """
    Handles installation of Python and Node.js dependencies.
    Key features:
    - Chunked pip install: splits large requirements into batches to avoid timeouts
    - Process isolation: deployed apps run with a clean minimal env
    - npm install with retry and timeout management
    - Frontend-only static serving support
    """

    @classmethod
    async def run_command(
        cls,
        cmd: str,
        cwd: str,
        env: Optional[Dict] = None,
        timeout: int = 300,
    ) -> Dict[str, Any]:
        try:
            proc_env = {**os.environ, **(env or {})}
            proc = await asyncio.create_subprocess_shell(
                cmd, cwd=cwd, env=proc_env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            return {
                "returncode": proc.returncode or 0,
                "stdout": stdout.decode("utf-8", errors="replace"),
                "stderr": stderr.decode("utf-8", errors="replace"),
            }
        except asyncio.TimeoutError:
            try: proc.kill()
            except Exception: pass
            return {"returncode": -1, "stdout": "", "stderr": f"Timed out after {timeout}s"}
        except Exception as e:
            return {"returncode": -1, "stdout": "", "stderr": str(e)}

    @classmethod
    async def install_python_deps(
        cls,
        project_dir: str,
        requirements_path: str,
        deployment_id: str,
        project_id: str,
    ) -> bool:
        """
        Install Python deps into isolated venv.
        Splits requirements into CHUNKS of 15 packages each to avoid
        single-run timeouts on large projects (e.g. ML stacks, bots).
        """
        venv_dir = os.path.join(project_dir, ".venv")
        pip_path  = os.path.join(venv_dir, "bin", "pip")
        CHUNK_SIZE = 15   # packages per batch

        await db_add_log(deployment_id, project_id, "Creating isolated virtual environment...", source="build")

        result = await cls.run_command(f"python3 -m venv {venv_dir}", cwd=project_dir)
        if result["returncode"] != 0:
            await db_add_log(deployment_id, project_id, f"venv creation failed: {result['stderr']}", level="error", source="build")
            return False

        # Upgrade pip silently
        await cls.run_command(f"{pip_path} install --upgrade pip setuptools wheel -q", cwd=project_dir, timeout=120)

        # Parse requirements
        try:
            raw_lines = Path(requirements_path).read_text().splitlines()
        except Exception as exc:
            await db_add_log(deployment_id, project_id, f"Cannot read requirements.txt: {exc}", level="error", source="build")
            return False

        # Filter: skip comments, blank lines, -r includes (handle recursively), constraints
        packages = []
        for line in raw_lines:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("-c"):
                continue
            if line.startswith("-r "):
                # Inline recursive requirements — try to load them
                sub_req = os.path.join(os.path.dirname(requirements_path), line[3:].strip())
                if os.path.exists(sub_req):
                    try:
                        for sl in Path(sub_req).read_text().splitlines():
                            sl = sl.strip()
                            if sl and not sl.startswith("#"):
                                packages.append(sl)
                    except Exception:
                        pass
                continue
            packages.append(line)

        # Always ensure base servers are installed
        base_packages = ["gunicorn", "uvicorn[standard]"]
        for bp in base_packages:
            name = bp.split("[")[0].split("=")[0].split(">")[0].split("<")[0].lower()
            if not any(name in p.lower() for p in packages):
                packages.append(bp)

        total = len(packages)
        await db_add_log(deployment_id, project_id, f"Installing {total} packages in chunks of {CHUNK_SIZE}...", source="build")

        # ── Chunked install ────────────────────────────────────────────
        chunks = [packages[i:i+CHUNK_SIZE] for i in range(0, len(packages), CHUNK_SIZE)]
        failed_pkgs = []

        for idx, chunk in enumerate(chunks, 1):
            chunk_str = " ".join(f'"{p}"' for p in chunk)
            await db_add_log(deployment_id, project_id,
                f"  Chunk {idx}/{len(chunks)}: installing {len(chunk)} packages...", source="build")

            result = await cls.run_command(
                f"{pip_path} install {chunk_str} --no-cache-dir -q",
                cwd=project_dir, timeout=300,
            )

            if result["returncode"] != 0:
                # Try each package individually on failure
                await db_add_log(deployment_id, project_id,
                    f"  Chunk {idx} had errors — trying packages individually...", level="warning", source="build")
                for pkg in chunk:
                    r = await cls.run_command(
                        f'{pip_path} install "{pkg}" --no-cache-dir -q',
                        cwd=project_dir, timeout=180,
                    )
                    if r["returncode"] != 0:
                        failed_pkgs.append(pkg)
                        await db_add_log(deployment_id, project_id,
                            f"  ⚠ Failed: {pkg} — {r['stderr'][:200]}", level="warning", source="build")
                    else:
                        await db_add_log(deployment_id, project_id, f"  ✓ {pkg}", source="build")
            else:
                installed = [l for l in result["stdout"].splitlines() if l.strip().startswith("Successfully installed")]
                if installed:
                    await db_add_log(deployment_id, project_id, f"  ✓ {installed[-1]}", source="build")

        if failed_pkgs:
            await db_add_log(deployment_id, project_id,
                f"⚠ {len(failed_pkgs)} package(s) failed: {', '.join(failed_pkgs[:10])}. AI will attempt to fix.",
                level="warning", source="build")
            # Don't fail entirely — AI can retry at runtime

        await db_add_log(deployment_id, project_id, f"✅ Python dependencies installed ({total - len(failed_pkgs)}/{total} OK).", source="build")
        return True

    @classmethod
    async def setup_frontend_only(
        cls,
        project_dir: str,
        deployment_id: str,
        project_id: str,
    ) -> bool:
        """Serve a frontend-only project (HTML/CSS/JS) via Python HTTP server."""
        await db_add_log(deployment_id, project_id,
            "Static frontend detected — will serve with Python HTTP server.", source="build")
        # Check for index.html
        root = Path(project_dir)
        index = next((f for f in root.rglob("*.html") if f.name in ("index.html","index.htm")), None)
        if not index:
            index = next(root.rglob("*.html"), None)
        if index:
            await db_add_log(deployment_id, project_id,
                f"✅ Entry point: {index.relative_to(root)}", source="build")
        return True

    @classmethod
    async def build_frontend(
        cls,
        project_dir: str,
        frontend_type: str,
        package_json_path: str,
        deployment_id: str,
        project_id: str,
    ) -> bool:
        """Build React/Next/Vue/Node frontend with chunked npm install."""
        frontend_dir = str(Path(package_json_path).parent)

        pkg_json = os.path.join(frontend_dir, "package.json")
        if not os.path.exists(pkg_json):
            await db_add_log(deployment_id, project_id,
                f"⚠ No package.json in {frontend_dir} — skipping", level="warning", source="build")
            return False

        pkg_manager = "npm"
        if (Path(frontend_dir) / "yarn.lock").exists():
            pkg_manager = "yarn"
        elif (Path(frontend_dir) / "pnpm-lock.yaml").exists():
            pkg_manager = "pnpm"

        # Parse package.json to count deps
        try:
            pkg_data = json.loads(Path(pkg_json).read_text())
            all_deps = {**pkg_data.get("dependencies",{}), **pkg_data.get("devDependencies",{})}
            total_npm = len(all_deps)
        except Exception:
            total_npm = "?"

        await db_add_log(deployment_id, project_id,
            f"Installing {total_npm} npm packages ({frontend_type})...", source="build")

        # Try --prefer-offline first (fast path), then fresh
        for flags, label in [
            ("--legacy-peer-deps --no-audit --prefer-offline", "cached"),
            ("--legacy-peer-deps --no-audit", "fresh"),
        ]:
            result = await cls.run_command(
                f"CI=true {pkg_manager} install {flags}",
                cwd=frontend_dir, timeout=900,
                env={"CI": "true", "NODE_ENV": "production"},
            )
            if result["returncode"] == 0:
                await db_add_log(deployment_id, project_id, f"✅ npm install OK ({label})", source="build")
                break
            await db_add_log(deployment_id, project_id,
                f"npm install ({label}) failed, trying next method...", level="warning", source="build")
        else:
            tail = (result.get("stdout","") + result.get("stderr",""))[-600:]
            await db_add_log(deployment_id, project_id,
                f"❌ npm install failed:\n{tail}", level="error", source="build")
            return False

        # Build
        await db_add_log(deployment_id, project_id, f"Building {frontend_type} frontend...", source="build")
        build_cmd = "npm run build" if pkg_manager == "npm" else f"{pkg_manager} build"
        result = await cls.run_command(
            f"CI=true {build_cmd}", cwd=frontend_dir, timeout=900,
            env={"CI": "true"},
        )
        if result["returncode"] != 0:
            tail = (result.get("stdout","") + result.get("stderr",""))[-600:]
            await db_add_log(deployment_id, project_id,
                f"❌ Frontend build failed:\n{tail}", level="error", source="build")
            return False

        await db_add_log(deployment_id, project_id, "✅ Frontend build complete.", source="build")
        return True


# SECTION 10: RUNTIME MANAGER (Process Pool)
# ──────────────────────────────────────────────────────────────────────────────

# Global registry of running processes: {deployment_id: ProcessInfo}
_running_processes: Dict[str, Dict] = {}

# Port allocation: track used ports
_used_ports: set = set()


def allocate_port() -> int:
    """Allocate a free port for a new deployment.
    Skips PORT (the port PyDeploy itself is bound to) to avoid conflicts.
    """
    reserved = {PORT}  # never hand out our own listener port
    for port in range(BASE_APP_PORT, BASE_APP_PORT + 1000):
        if port not in _used_ports and port not in reserved:
            _used_ports.add(port)
            return port
    raise RuntimeError("No available ports for deployment.")


def release_port(port: int):
    """Release a port back to the pool."""
    _used_ports.discard(port)


def is_process_running(pid: int) -> bool:
    """Check if a process with given PID is still running."""
    try:
        proc = psutil.Process(pid)
        return proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return False


async def start_app_process(
    project_dir: str,
    startup_cmd: str,
    port: int,
    deployment_id: str,
    project_id: str,
    env_vars: Dict[str, str] = None,
) -> Optional[int]:
    """
    Start the user's application as an isolated subprocess.
    Key isolation guarantees:
    - Deployed app gets its OWN clean env (no PyDeploy secrets leaked)
    - Only venv/bin is on PATH — no access to system Python packages
    - Runs in a new session (setsid) so signals don't cascade to PyDeploy
    - PyDeploy's own SUPABASE_KEY, GEMINI_API_KEY etc are NOT inherited
    """
    venv_bin = os.path.join(project_dir, ".venv", "bin")

    # ── Minimal, clean environment for the deployed app ──────────────
    # Deliberately does NOT pass **os.environ — only safe system vars
    SAFE_SYSTEM_VARS = {"HOME","USER","LANG","LC_ALL","LC_CTYPE","TZ",
                        "TMPDIR","TEMP","TMP","XDG_RUNTIME_DIR"}
    clean_env = {k: v for k, v in os.environ.items() if k in SAFE_SYSTEM_VARS}
    clean_env.update({
        "PORT": str(port),
        "HOST": "0.0.0.0",
        "PATH": f"{venv_bin}:/usr/local/bin:/usr/bin:/bin",
        "VIRTUAL_ENV": os.path.join(project_dir, ".venv"),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONUNBUFFERED": "1",
        "PYTHONPATH": project_dir,
    })
    # User-defined env vars go on top (override safe vars if needed)
    clean_env.update(env_vars or {})

    env = clean_env

    # Replace ${PORT:-8000} or $PORT with actual port
    cmd = startup_cmd.replace("${PORT:-8000}", str(port)).replace("$PORT", str(port))

    log_path = os.path.join(project_dir, "deploy.log")

    try:
        # Ensure the deploy directory exists (may be missing on restart)
        os.makedirs(project_dir, exist_ok=True)
        log_file = open(log_path, "a")
        proc = subprocess.Popen(
            cmd,
            shell=True,
            cwd=project_dir,
            env=env,
            stdout=log_file,
            stderr=log_file,
            start_new_session=True,
        )

        # Store process info
        _running_processes[deployment_id] = {
            "process": proc,
            "pid": proc.pid,
            "port": port,
            "log_file": log_file,
            "log_path": log_path,
            "project_dir": project_dir,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }

        # Wait briefly to check if it started OK
        await asyncio.sleep(2)
        if proc.poll() is not None:
            log_file.close()
            return None

        await db_add_log(
            deployment_id, project_id,
            f"Process started. PID={proc.pid}, PORT={port}",
            source="runtime",
        )

        # Start background log tail task
        asyncio.create_task(
            tail_log_to_db(log_path, deployment_id, project_id, proc)
        )

        return proc.pid

    except Exception as e:
        await db_add_log(
            deployment_id, project_id,
            f"Failed to start process: {e}",
            level="error",
            source="runtime",
        )
        return None


_ERROR_WORDS  = {"error", "exception", "traceback", "critical", "fatal", "panic", "oserror", "ioerror", "syntaxerror", "importerror", "modulenotfounderror", "valueerror", "typeerror", "runtimeerror", "attributeerror", "keyerror", "indexerror", "filenotfounderror", "permissionerror", "connectionerror", "killed", "segfault", "abort"}
_WARNING_WORDS = {"warning", "warn", "deprecated", "deprecation", "userwarning", "runtimewarning", "futurewarn", "insecure", "caution"}


def _classify_log_line(line: str) -> tuple:
    """Return (level, source) for a raw log line."""
    low = line.lower()
    # Traceback lines
    if line.startswith("Traceback") or line.startswith("  File "):
        return "error", "stderr"
    if any(w in low for w in _ERROR_WORDS):
        return "error", "stderr"
    if any(w in low for w in _WARNING_WORDS):
        return "warning", "stdout"
    return "info", "stdout"


async def tail_log_to_db(
    log_path: str,
    deployment_id: str,
    project_id: str,
    proc: subprocess.Popen,
    interval: float = 0.5,          # poll every 500ms — much faster
):
    """Background task: tail log file and push ALL lines to Supabase."""
    position = 0

    while True:
        await asyncio.sleep(interval)

        # Read new log data first, then check if process died
        try:
            if os.path.exists(log_path):
                async with aiofiles.open(log_path, "r", errors="replace") as f:
                    await f.seek(position)
                    new_content = await f.read()
                    position = await f.tell()

                if new_content:
                    lines = new_content.splitlines()
                    for line in lines:
                        raw = line.rstrip()
                        if not raw:
                            continue
                        level, source = _classify_log_line(raw)
                        await db_add_log(
                            deployment_id, project_id,
                            raw,
                            level=level,
                            source=source,
                        )
        except Exception as e:
            logger.warning(f"Log tail error: {e}")

        process_alive = proc.poll() is None
        if not process_alive:
            # Drain any remaining log data
            try:
                if os.path.exists(log_path):
                    async with aiofiles.open(log_path, "r", errors="replace") as f:
                        await f.seek(position)
                        remainder = await f.read()
                    for line in remainder.splitlines():
                        if line.strip():
                            level, source = _classify_log_line(line.rstrip())
                            await db_add_log(deployment_id, project_id, line.rstrip(), level=level, source=source)
            except Exception:
                pass

            exit_code = proc.returncode
            await db_add_log(
                deployment_id, project_id,
                f"Process exited with code: {exit_code}",
                level="error" if exit_code and exit_code != 0 else "info",
                source="runtime",
            )
            try:
                await db_update_deployment(deployment_id, {
                    "status": DeployState.STOPPED,
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                })
            except Exception:
                pass

            await db_flush_logs(deployment_id)
            # Auto-trigger AI fix if process crashed unexpectedly
            if exit_code and exit_code != 0:
                asyncio.create_task(
                    ai_auto_fix(deployment_id, project_id, trigger="crash")
                )
            break


async def stop_deployment_process(deployment_id: str) -> bool:
    """Stop a running deployment process."""
    info = _running_processes.get(deployment_id)
    if not info:
        return False

    proc = info["process"]
    port = info["port"]

    try:
        if proc.poll() is None:
            # Try graceful SIGTERM first
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                # Force kill
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except Exception as e:
        logger.warning(f"Error stopping process: {e}")
        try:
            proc.kill()
        except Exception:
            pass

    # Cleanup
    try:
        info["log_file"].close()
    except Exception:
        pass

    release_port(port)
    del _running_processes[deployment_id]
    return True


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 11: DEPLOYMENT ENGINE
# ──────────────────────────────────────────────────────────────────────────────

async def run_deployment(
    project_id: str,
    user_id: str,
    zip_bytes: bytes,
    project_name: str,
    env_vars: Dict[str, str] = None,
):
    """
    Full deployment pipeline:
    1. Extract ZIP
    2. Detect framework
    3. Install dependencies
    4. Build frontend (if applicable)
    5. Start process
    6. Update DB
    """
    # Create deployment record
    deployment = await db_create_deployment(
        project_id=project_id,
        user_id=user_id,
        framework="detecting...",
        startup_cmd="",
    )
    deployment_id = deployment["id"]

    await db_add_log(deployment_id, project_id, f"Starting deployment for project: {project_name}", source="system")

    # Update project status
    await db_update_project(project_id, {"status": DeployState.BUILDING})
    await db_update_deployment(deployment_id, {"status": DeployState.BUILDING})

    # ── Step 1: Prepare deploy directory ──────────────────────────────
    deploy_dir = os.path.join(BASE_DEPLOY_DIR, project_id)
    os.makedirs(deploy_dir, exist_ok=True)

    try:
        # ── Step 2: Extract ZIP ────────────────────────────────────────
        await db_add_log(deployment_id, project_id, "Extracting project files...", source="build")
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            zf.extractall(deploy_dir)

        # If ZIP contained a single folder, move contents up
        entries = list(Path(deploy_dir).iterdir())
        if len(entries) == 1 and entries[0].is_dir():
            inner = entries[0]
            for item in inner.iterdir():
                shutil.move(str(item), str(deploy_dir))
            inner.rmdir()

        await db_add_log(deployment_id, project_id, "Files extracted successfully.", source="build")

        # ── Step 3: Framework detection ────────────────────────────────
        await db_add_log(deployment_id, project_id, "Analyzing project structure...", source="build")
        detection = FrameworkDetector.detect(deploy_dir)

        framework = detection["framework"]
        startup_cmd = detection["startup_cmd"] or "python main.py"

        await db_add_log(
            deployment_id, project_id,
            f"Framework detected: {framework} | Entry: {detection['entry_point']} | Command: {startup_cmd}",
            source="build",
        )

        # Update deployment with detected info
        await db_update_deployment(deployment_id, {
            "framework": framework,
            "startup_cmd": startup_cmd,
        })
        await db_update_project(project_id, {
            "framework": framework,
            "startup_cmd": startup_cmd,
            "deploy_dir": deploy_dir,
        })

        # ── Step 4: Install Python dependencies ───────────────────────
        if detection.get("is_frontend_only"):
            await BuildSystem.setup_frontend_only(deploy_dir, deployment_id, project_id)
        elif detection["has_requirements"]:
            success = await BuildSystem.install_python_deps(
                deploy_dir, detection["requirements_path"], deployment_id, project_id,
            )
            if not success:
                # Don't hard-fail — AI will fix missing packages
                await db_add_log(deployment_id, project_id,
                    "⚠ Some packages failed to install — AI agent will attempt fixes.",
                    level="warning", source="build")
        else:
            await db_add_log(deployment_id, project_id,
                "No requirements.txt — creating minimal venv with uvicorn/gunicorn.",
                level="warning", source="build")
            venv_dir = os.path.join(deploy_dir, ".venv")
            await BuildSystem.run_command(f"python3 -m venv {venv_dir}", cwd=deploy_dir)
            pip = os.path.join(venv_dir, "bin", "pip")
            await BuildSystem.run_command(f"{pip} install gunicorn uvicorn -q", cwd=deploy_dir, timeout=120)

        # ── Step 5: Build frontend ─────────────────────────────────────
        if not detection.get("is_frontend_only") and detection["has_frontend"] and detection["has_package_json"]:
            success = await BuildSystem.build_frontend(
                deploy_dir, detection["frontend_type"], detection["package_json_path"],
                deployment_id, project_id,
            )
            if not success:
                await db_add_log(deployment_id, project_id,
                    "Frontend build failed — continuing with backend only.", level="warning", source="build")

        # ── Step 5b: Frontend-only startup command ─────────────────────
        if detection.get("is_frontend_only"):
            # Find the best serve directory (prefer index.html location)
            serve_dir = deploy_dir
            for d in ["dist","build","public","www","html","static","."]:
                candidate = os.path.join(deploy_dir, d)
                if os.path.exists(os.path.join(candidate, "index.html")):
                    serve_dir = candidate
                    break
            if serve_dir != deploy_dir:
                startup_cmd = f"python -m http.server ${{PORT:-8000}}"
                await db_update_project(project_id, {"startup_cmd": startup_cmd, "deploy_dir": serve_dir})
                await db_update_deployment(deployment_id, {"startup_cmd": startup_cmd})
                deploy_dir = serve_dir  # serve from the right dir

        # ── Step 6: Allocate port & start process ──────────────────────
        port = allocate_port()
        await db_add_log(deployment_id, project_id, f"Allocated port {port} for app.", source="runtime")

        await db_update_deployment(deployment_id, {"port": port})
        await db_update_project(project_id, {"port": port})

        pid = await start_app_process(
            project_dir=deploy_dir,
            startup_cmd=startup_cmd,
            port=port,
            deployment_id=deployment_id,
            project_id=project_id,
            env_vars=env_vars,
        )

        if pid is None:
            # Read last 60 lines from the log to understand why it failed
            crash_log = ""
            log_path = os.path.join(deploy_dir, "deploy.log")
            if os.path.exists(log_path):
                with open(log_path, "r", errors="replace") as f:
                    lines = f.readlines()
                crash_log = "".join(lines[-60:])
            raise RuntimeError(f"Application process failed to start.\n{crash_log}")

        # ── Step 7: Mark as running ────────────────────────────────────
        await db_update_deployment(deployment_id, {
            "status": DeployState.RUNNING,
            "pid": pid,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })
        await db_update_project(project_id, {"status": DeployState.RUNNING})

        await db_add_log(
            deployment_id, project_id,
            f"🚀 Deployment successful! App running on port {port}",
            level="info",
            source="system",
        )
        await db_flush_logs(deployment_id)

    except Exception as e:
        error_msg = traceback.format_exc()
        logger.error(f"Deployment failed for project {project_id}: {error_msg}")

        await db_add_log(
            deployment_id, project_id,
            f"❌ Deployment failed: {str(e)[:500]}",
            level="error",
            source="system",
        )
        await db_update_deployment(deployment_id, {
            "status": DeployState.FAILED,
            "error_msg": str(e)[:2000],
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })
        await db_update_project(project_id, {
            "status": DeployState.FAILED,
            "deploy_dir": deploy_dir,   # keep dir so AI can fix files
        })

        await db_flush_logs(deployment_id)
        # Trigger AI auto-fix in background — it has full access to fix and redeploy
        asyncio.create_task(
            ai_auto_fix(deployment_id, project_id, trigger="build_failure")
        )


async def restart_deployment(project_id: str, deployment_id: str) -> bool:
    """Stop and restart an existing deployment."""
    project = await db_get_project(project_id)
    if not project:
        return False

    deployment = await db_get_deployment(deployment_id)
    if not deployment:
        return False

    # Stop current process
    await stop_deployment_process(deployment_id)
    await db_update_deployment(deployment_id, {"status": DeployState.RESTARTING})

    deploy_dir = project.get("deploy_dir")
    startup_cmd = project.get("startup_cmd")
    env_vars = project.get("env_vars") or {}

    if not deploy_dir or not startup_cmd:
        await db_update_deployment(deployment_id, {"status": DeployState.FAILED})
        return False

    # Guard: deploy dir must exist — it could have been wiped after a failed deploy
    if not os.path.isdir(deploy_dir):
        await db_add_log(deployment_id, project_id,
            f"Deploy directory missing ({deploy_dir}). Re-deploy the project to fix this.",
            level="error", source="runtime")
        await db_update_deployment(deployment_id, {"status": DeployState.FAILED})
        await db_update_project(project_id, {"status": DeployState.FAILED})
        return False

    port = allocate_port()

    pid = await start_app_process(
        project_dir=deploy_dir,
        startup_cmd=startup_cmd,
        port=port,
        deployment_id=deployment_id,
        project_id=project_id,
        env_vars=env_vars,
    )

    if pid:
        await db_update_deployment(deployment_id, {
            "status": DeployState.RUNNING,
            "pid": pid,
            "port": port,
        })
        await db_update_project(project_id, {
            "status": DeployState.RUNNING,
            "port": port,
        })
        return True

    await db_update_deployment(deployment_id, {"status": DeployState.FAILED})
    return False


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 11.5: AI ULTRA AGENT  ─ gemini-3.1-pro-preview-customtools
# ──────────────────────────────────────────────────────────────────────────────
# Primary:  Google Gemini API  (gemini-3.1-pro-preview-customtools)
#           purpose-built for agentic workflows with custom tools
# Fallback: gemini-3.1-pro-preview  →  OpenRouter models
#
# Features:
#  • Never stops — retries with exponential backoff, falls through all models
#  • Real-time timestamped log lines streamed every action
#  • Full filesystem & shell control over the project
#  • Bulletproof: every tool call wrapped in try/except, agent loop never crashes
# ──────────────────────────────────────────────────────────────────────────────

_ai_fixing: Dict[str, bool] = {}   # deployment_id → True while agent is running


# ── Timestamp helper ─────────────────────────────────────────────────────────

def _ts() -> str:
    """HH:MM:SS timestamp for real-time log prefixes."""
    return datetime.now().strftime("%H:%M:%S")


# ── Google Gemini API caller ──────────────────────────────────────────────────

def _gemini_tools_schema(tools: List[Dict]) -> List[Dict]:
    """Convert OpenAI-style tool list to Gemini functionDeclarations format."""
    decls = []
    for t in tools:
        fn = t.get("function", {})
        decls.append({
            "name": fn["name"],
            "description": fn.get("description", ""),
            "parameters": fn.get("parameters", {"type": "object", "properties": {}}),
        })
    return [{"functionDeclarations": decls}]


def _gemini_messages(messages: List[Dict]) -> tuple:
    """Split messages into systemInstruction + contents for Gemini format."""
    system_text = ""
    contents = []

    for m in messages:
        role = m.get("role", "user")

        if role == "system":
            system_text = m.get("content", "")
            continue

        if role == "user":
            content_val = m.get("content", "")
            if isinstance(content_val, str):
                contents.append({"role": "user", "parts": [{"text": content_val}]})
            continue

        if role == "assistant":
            parts = []
            text = m.get("content") or ""
            if text:
                parts.append({"text": text})
            for tc in (m.get("tool_calls") or []):
                fn = tc.get("function", {})
                try:
                    args = json.loads(fn.get("arguments", "{}"))
                except Exception:
                    args = {}
                parts.append({"functionCall": {"name": fn["name"], "args": args}})
            if parts:
                contents.append({"role": "model", "parts": parts})
            continue

        if role == "tool":
            # Gemini expects tool results as "functionResponse" in user turn
            # Group consecutive tool results together
            result_text = m.get("content", "")
            # Find the tool name from the call_id — embed it in a functionResponse
            # We use the tool_call_id as function name fallback
            tool_name = m.get("name") or m.get("tool_call_id", "tool_result")
            contents.append({
                "role": "user",
                "parts": [{"functionResponse": {
                    "name": tool_name,
                    "response": {"result": result_text[:8000]},
                }}],
            })
            continue

    return system_text, contents


async def _call_gemini(
    model: str,
    messages: List[Dict],
    tools: List[Dict],
) -> Optional[Dict]:
    """
    Call Google Gemini API.
    Returns normalised assistant message (OpenAI-style) or None.
    """
    if not GEMINI_API_KEY:
        return None

    system_text, contents = _gemini_messages(messages)
    gemini_tools = _gemini_tools_schema(tools)

    payload: Dict = {
        "contents": contents,
        "tools": gemini_tools,
        "toolConfig": {"functionCallingConfig": {"mode": "AUTO"}},
        "generationConfig": {
            "maxOutputTokens": 8192,
            "temperature": 0.1,
        },
    }
    if system_text:
        payload["systemInstruction"] = {"parts": [{"text": system_text}]}

    url = f"{GEMINI_BASE}/{model}:generateContent?key={GEMINI_API_KEY}"

    for attempt in range(1, AI_API_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as client:
                resp = await client.post(url, json=payload)

            if resp.status_code == 200:
                data = resp.json()
                candidate = (data.get("candidates") or [{}])[0]
                content_obj = candidate.get("content", {})
                parts = content_obj.get("parts", [])

                # Normalise to OpenAI assistant message format
                msg: Dict = {"role": "assistant", "content": None, "tool_calls": []}
                text_parts = []
                for p in parts:
                    if "text" in p:
                        text_parts.append(p["text"])
                    if "functionCall" in p:
                        fc = p["functionCall"]
                        msg["tool_calls"].append({
                            "id": f"gemini_{fc['name']}_{uuid.uuid4().hex[:6]}",
                            "type": "function",
                            "function": {
                                "name": fc["name"],
                                "arguments": json.dumps(fc.get("args", {})),
                            },
                        })
                if text_parts:
                    msg["content"] = "\n".join(text_parts)
                if not msg["tool_calls"]:
                    del msg["tool_calls"]  # cleaner

                finish = candidate.get("finishReason", "")
                usage = data.get("usageMetadata", {})
                logger.info(
                    f"Gemini {model}: finish={finish} "
                    f"in={usage.get('promptTokenCount',0)} "
                    f"out={usage.get('candidatesTokenCount',0)}"
                )
                return msg

            elif resp.status_code in (429, 503, 500, 502):
                wait = AI_RETRY_DELAY * (2 ** (attempt - 1))
                logger.warning(f"Gemini {model} {resp.status_code} (attempt {attempt}), retry in {wait}s")
                await asyncio.sleep(wait)
                continue
            else:
                err = resp.text[:300]
                logger.warning(f"Gemini {model} → {resp.status_code}: {err}")
                return None

        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            wait = AI_RETRY_DELAY * (2 ** (attempt - 1))
            logger.warning(f"Gemini {model} network error (attempt {attempt}): {exc}. Retry in {wait}s")
            await asyncio.sleep(wait)
        except Exception as exc:
            logger.warning(f"Gemini {model} unexpected error: {exc}")
            return None

    return None  # all retries exhausted


# ── OpenRouter fallback caller ────────────────────────────────────────────────

async def _call_openrouter(
    model: str,
    messages: List[Dict],
    tools: List[Dict],
) -> Optional[Dict]:
    """Call OpenRouter API (OpenAI-compatible). Returns assistant message or None."""
    if not OPENROUTER_API_KEY:
        return None

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://pydeploy.app",
        "X-Title": "PyDeploy Ultra Agent",
    }
    payload = {
        "model": model,
        "messages": messages,
        "tools": tools,
        "tool_choice": "auto",
        "max_tokens": 4096,
        "temperature": 0.1,
    }
    if model == "openrouter/auto":
        payload.pop("tool_choice", None)

    for attempt in range(1, AI_API_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(90.0)) as client:
                resp = await client.post(OPENROUTER_BASE, json=payload, headers=headers)

            if resp.status_code == 200:
                data = resp.json()
                msg = (data.get("choices") or [{}])[0].get("message", {})
                logger.info(f"OpenRouter {model}: finish={(data.get('choices') or [{}])[0].get('finish_reason')}")
                return msg
            elif resp.status_code in (429, 500, 502, 503):
                wait = AI_RETRY_DELAY * (2 ** (attempt - 1))
                await asyncio.sleep(wait)
                continue
            else:
                logger.warning(f"OpenRouter {model} → {resp.status_code}: {resp.text[:200]}")
                return None
        except Exception as exc:
            logger.warning(f"OpenRouter {model} error (attempt {attempt}): {exc}")
            await asyncio.sleep(AI_RETRY_DELAY)

    return None


# ── Master caller: Gemini first, then all OpenRouter fallbacks ────────────────

async def _ai_call(
    messages: List[Dict],
    tools: List[Dict],
    deployment_id: str,
    project_id: str,
) -> Optional[Dict]:
    """
    Try Gemini primary → Gemini fallback → each OpenRouter model.
    Logs which model is being tried. Returns first success or None.
    """
    async def _log(msg: str, level: str = "info"):
        await db_add_log(deployment_id, project_id, msg, level=level, source="ai")

    # 1. Gemini primary (customtools endpoint — best for our use case)
    if GEMINI_API_KEY:
        await _log(f"[{_ts()}] 🧠 Trying {GEMINI_PRIMARY}…")
        result = await _call_gemini(GEMINI_PRIMARY, messages, tools)
        if result is not None:
            await _log(f"[{_ts()}] ✅ {GEMINI_PRIMARY} responded")
            return result

        # 2. Gemini fallback
        await _log(f"[{_ts()}] ⚠️  {GEMINI_PRIMARY} failed, trying {GEMINI_FALLBACK}…", "warning")
        result = await _call_gemini(GEMINI_FALLBACK, messages, tools)
        if result is not None:
            await _log(f"[{_ts()}] ✅ {GEMINI_FALLBACK} responded")
            return result
        await _log(f"[{_ts()}] ❌ Both Gemini endpoints failed, switching to OpenRouter…", "warning")
    else:
        await _log(f"[{_ts()}] ⚠️  GEMINI_API_KEY not set — using OpenRouter fallbacks", "warning")

    # 3. OpenRouter fallbacks
    if OPENROUTER_API_KEY:
        for model in OPENROUTER_MODELS:
            await _log(f"[{_ts()}] 🔄 Trying OpenRouter/{model}…")
            result = await _call_openrouter(model, messages, tools)
            if result is not None:
                await _log(f"[{_ts()}] ✅ {model} responded")
                return result
            await _log(f"[{_ts()}] ❌ {model} failed, next…", "warning")

    await _log(f"[{_ts()}] 🚨 ALL AI MODELS FAILED — no API keys or all unreachable", "error")
    return None


# ── Tool schema ──────────────────────────────────────────────────────────────

_AI_TOOLS: List[Dict] = [
    {
        "type": "function",
        "function": {
            "name": "run_command",
            "description": (
                "Run ANY shell command in the project environment. "
                "The project venv is auto-activated. "
                "Use for: installing packages (.venv/bin/pip install X), "
                "checking syntax (python3 -m py_compile file.py), "
                "running tests, linting, checking processes, anything. "
                "Returns combined stdout+stderr with exit code."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "command":  {"type": "string", "description": "Shell command to run"},
                    "cwd":      {"type": "string", "description": "Working directory relative to project root (default: .)"},
                    "timeout":  {"type": "integer", "description": f"Max seconds (default {AI_CMD_TIMEOUT})"},
                },
                "required": ["command"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "Read a file. Returns content with LINE NUMBERS prepended (e.g. '   1│ import os'). Always read before editing.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "File path relative to project root"},
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_file",
            "description": "Create or fully overwrite a file. Use for new files or complete rewrites. For small changes use patch_file.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path":    {"type": "string", "description": "File path relative to project root"},
                    "content": {"type": "string", "description": "Complete new file content"},
                },
                "required": ["path", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "patch_file",
            "description": (
                "Surgically edit specific lines without rewriting the whole file. "
                "Read the file first to get accurate 1-indexed line numbers. "
                "Operations: replace (overwrite line range), insert (add lines before line N), "
                "delete (remove line range), append (add to end)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "path":       {"type": "string", "description": "File path relative to project root"},
                    "operation":  {"type": "string", "enum": ["replace", "insert", "delete", "append"]},
                    "start_line": {"type": "integer", "description": "First line (1-indexed). Required for replace/insert/delete."},
                    "end_line":   {"type": "integer", "description": "Last line inclusive. Required for replace/delete."},
                    "content":    {"type": "string", "description": "New lines for replace/insert/append (\\n separated)"},
                },
                "required": ["path", "operation"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "delete_file",
            "description": "Permanently delete a file.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "rename_file",
            "description": "Rename or move a file or directory.",
            "parameters": {
                "type": "object",
                "properties": {
                    "src":  {"type": "string", "description": "Source path (relative to project root)"},
                    "dest": {"type": "string", "description": "Destination path (relative to project root)"},
                },
                "required": ["src", "dest"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_dir",
            "description": "Create a directory (and all parents).",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "delete_dir",
            "description": "Recursively delete a directory. Cannot delete the project root.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_files",
            "description": "Recursively list project files with sizes. Skips .venv, node_modules, __pycache__.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path":        {"type": "string", "description": "Sub-path to list (default: .)"},
                    "show_hidden": {"type": "boolean", "description": "Include dotfiles (default false)"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_in_files",
            "description": "Grep for a string/regex across all project source files. Returns file:line:content matches.",
            "parameters": {
                "type": "object",
                "properties": {
                    "pattern":        {"type": "string", "description": "String or regex to search"},
                    "file_glob":      {"type": "string", "description": "Filename pattern e.g. '*.py' (default: all text files)"},
                    "case_sensitive": {"type": "boolean", "description": "Case sensitive? (default false)"},
                },
                "required": ["pattern"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_env",
            "description": "Show environment variables, Python path, system info available to the app.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "set_startup_cmd",
            "description": (
                "Update the startup command in the DB. "
                "Call this when the detected command is wrong (e.g. references main.py but real entry is bot.py). "
                "Call BEFORE restart_app."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "startup_cmd": {"type": "string", "description": "Correct startup command e.g. 'python bot.py'"},
                },
                "required": ["startup_cmd"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "notify_user",
            "description": "Show a prominent notice to the user. Use when the user must take action (e.g. set an env var).",
            "parameters": {
                "type": "object",
                "properties": {
                    "message": {"type": "string", "description": "Actionable message for the user"},
                    "level":   {"type": "string", "enum": ["info", "warning", "error"], "description": "default: warning"},
                },
                "required": ["message"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "restart_app",
            "description": "Restart the app after applying all fixes. Returns success/failure with details.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "mark_fixed",
            "description": "Signal the session is complete. ALWAYS call this at the end (success or failure).",
            "parameters": {
                "type": "object",
                "properties": {
                    "success": {"type": "boolean", "description": "True if issue resolved"},
                    "summary": {"type": "string", "description": "What was done / what the user still needs to do"},
                },
                "required": ["success", "summary"],
            },
        },
    },
]


# ── Path resolver ─────────────────────────────────────────────────────────────

def _safe_path(project_dir: str, rel: str) -> str:
    """Resolve path safely within project_dir. Prevents path traversal."""
    if os.path.isabs(rel) and rel.startswith(project_dir):
        return rel
    full = os.path.normpath(os.path.join(project_dir, rel.lstrip("/")))
    if not full.startswith(os.path.normpath(project_dir)):
        return os.path.join(project_dir, os.path.basename(rel))
    return full


# ── Tool executor ─────────────────────────────────────────────────────────────

async def _exec_tool(
    tool_name: str,
    tool_args: Dict,
    project_dir: str,
    deployment_id: str,
    project_id: str,
    restart_ctx: Dict,
) -> str:
    """Execute one tool call. Always returns a string result. Never raises."""

    async def _log(msg: str, level: str = "info"):
        ts = _ts()
        await db_add_log(deployment_id, project_id, f"[{ts}] {msg}", level=level, source="ai")

    # Log invocation (skip content for brevity)
    preview = ", ".join(
        f"{k}={repr(str(v))[:60]}"
        for k, v in tool_args.items()
        if k != "content"
    )
    await _log(f"🔧 {tool_name}({preview})")

    try:

        # ── run_command ────────────────────────────────────────────────────
        if tool_name == "run_command":
            cmd     = tool_args.get("command", "").strip()
            rel_cwd = tool_args.get("cwd", ".")
            cwd     = _safe_path(project_dir, rel_cwd) if rel_cwd not in (".", "") else project_dir
            timeout = int(tool_args.get("timeout", AI_CMD_TIMEOUT))

            venv_bin = os.path.join(project_dir, ".venv", "bin")
            env = {
                **os.environ,
                "PATH": f"{venv_bin}:{os.environ.get('PATH', '')}",
                "VIRTUAL_ENV": os.path.join(project_dir, ".venv"),
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONUNBUFFERED": "1",
            }

            try:
                proc = await asyncio.create_subprocess_shell(
                    cmd, cwd=cwd, env=env,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )
                try:
                    out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
                except asyncio.TimeoutError:
                    try: proc.kill()
                    except Exception: pass
                    out = b"[COMMAND TIMED OUT]"
                output = out.decode("utf-8", errors="replace").strip()
                rc = proc.returncode if proc.returncode is not None else -1
            except Exception as exc:
                output = f"[LAUNCH ERROR]: {exc}"
                rc = -1

            # Stream last 100 lines to logs
            for line in output.splitlines()[-100:]:
                if line.strip():
                    lvl, _ = _classify_log_line(line)
                    await db_add_log(deployment_id, project_id, f"   {line}", level=lvl, source="ai")

            return f"exit_code={rc}\n{output}"[-6000:]

        # ── read_file ──────────────────────────────────────────────────────
        elif tool_name == "read_file":
            full = _safe_path(project_dir, tool_args.get("path", ""))
            with open(full, "r", errors="replace") as f:
                lines = f.readlines()
            numbered = "".join(f"{i+1:4d}│ {l}" for i, l in enumerate(lines))
            await _log(f"   📄 {tool_args.get('path')}  ({len(lines)} lines)")
            return numbered[:40_000]

        # ── write_file ─────────────────────────────────────────────────────
        elif tool_name == "write_file":
            rel  = tool_args.get("path", "")
            full = _safe_path(project_dir, rel)
            text = tool_args.get("content", "")
            os.makedirs(os.path.dirname(full) or project_dir, exist_ok=True)
            with open(full, "w") as f:
                f.write(text)
            await _log(f"   ✏️  Wrote {rel} ({len(text)} bytes, {text.count(chr(10))+1} lines)")
            return f"OK — wrote {len(text)} bytes to {rel}"

        # ── patch_file ─────────────────────────────────────────────────────
        elif tool_name == "patch_file":
            rel  = tool_args.get("path", "")
            op   = tool_args.get("operation", "")
            s    = tool_args.get("start_line")
            e    = tool_args.get("end_line")
            text = tool_args.get("content", "")
            full = _safe_path(project_dir, rel)

            with open(full, "r", errors="replace") as f:
                lines = f.readlines()
            orig = len(lines)

            def to_lines(t: str):
                return [(l if l.endswith("\n") else l+"\n") for l in t.split("\n")]

            if op == "replace":
                si, ei = int(s)-1, int(e)
                lines = lines[:si] + to_lines(text) + lines[ei:]
            elif op == "insert":
                si = int(s)-1
                lines = lines[:si] + to_lines(text) + lines[si:]
            elif op == "delete":
                si, ei = int(s)-1, int(e)
                lines = lines[:si] + lines[ei:]
            elif op == "append":
                lines = lines + to_lines(text)
            else:
                return f"[ERROR]: unknown operation '{op}'"

            with open(full, "w") as f:
                f.writelines(lines)
            await _log(f"   ✂️  patch_file {op} on {rel}: {orig}→{len(lines)} lines")
            return f"OK — {op} on {rel} ({orig}→{len(lines)} lines)"

        # ── delete_file ────────────────────────────────────────────────────
        elif tool_name == "delete_file":
            full = _safe_path(project_dir, tool_args.get("path", ""))
            os.remove(full)
            await _log(f"   🗑️  Deleted {tool_args.get('path')}")
            return f"OK — deleted"

        # ── rename_file ────────────────────────────────────────────────────
        elif tool_name == "rename_file":
            src  = _safe_path(project_dir, tool_args.get("src", ""))
            dest = _safe_path(project_dir, tool_args.get("dest", ""))
            os.makedirs(os.path.dirname(dest) or project_dir, exist_ok=True)
            shutil.move(src, dest)
            await _log(f"   📦 Renamed {tool_args.get('src')} → {tool_args.get('dest')}")
            return "OK"

        # ── create_dir ─────────────────────────────────────────────────────
        elif tool_name == "create_dir":
            full = _safe_path(project_dir, tool_args.get("path", ""))
            os.makedirs(full, exist_ok=True)
            await _log(f"   📁 Created dir {tool_args.get('path')}")
            return "OK"

        # ── delete_dir ─────────────────────────────────────────────────────
        elif tool_name == "delete_dir":
            full = _safe_path(project_dir, tool_args.get("path", ""))
            if os.path.normpath(full) == os.path.normpath(project_dir):
                return "[ERROR]: refusing to delete project root"
            shutil.rmtree(full)
            await _log(f"   🗑️  Deleted dir {tool_args.get('path')}")
            return "OK"

        # ── list_files ─────────────────────────────────────────────────────
        elif tool_name == "list_files":
            rel  = tool_args.get("path", ".")
            base = _safe_path(project_dir, rel) if rel != "." else project_dir
            skip = {".venv", "node_modules", "__pycache__", ".git", "dist", "build", ".next"}
            show_hidden = tool_args.get("show_hidden", False)
            entries = []
            for root, dirs, files in os.walk(base):
                dirs[:] = [d for d in dirs if d not in skip and (show_hidden or not d.startswith("."))]
                for name in sorted(files):
                    if not show_hidden and name.startswith("."): continue
                    fp = os.path.join(root, name)
                    rp = os.path.relpath(fp, project_dir)
                    try:
                        sz = os.path.getsize(fp)
                        sz_s = f"{sz:,}B" if sz < 1024 else f"{sz//1024}KB"
                    except Exception:
                        sz_s = "?"
                    entries.append(f"{rp}  ({sz_s})")
                if len(entries) >= 300: break
            result = "\n".join(entries[:300])
            await _log(f"   📂 Listed {len(entries)} files")
            return result or "(empty)"

        # ── search_in_files ────────────────────────────────────────────────
        elif tool_name == "search_in_files":
            import fnmatch
            pattern    = tool_args.get("pattern", "")
            glob_pat   = tool_args.get("file_glob", "*")
            case_s     = tool_args.get("case_sensitive", False)
            rx         = re.compile(pattern, 0 if case_s else re.IGNORECASE)
            skip       = {".venv", "node_modules", "__pycache__", ".git"}
            matches    = []
            for root, dirs, files in os.walk(project_dir):
                dirs[:] = [d for d in dirs if d not in skip]
                for name in files:
                    if glob_pat != "*" and not fnmatch.fnmatch(name, glob_pat): continue
                    fp = os.path.join(root, name)
                    rp = os.path.relpath(fp, project_dir)
                    try:
                        with open(fp, "r", errors="replace") as f:
                            for i, line in enumerate(f, 1):
                                if rx.search(line):
                                    matches.append(f"{rp}:{i}: {line.rstrip()}")
                                    if len(matches) >= 200: break
                    except Exception:
                        continue
                if len(matches) >= 200: break
            await _log(f"   🔍 '{pattern}' → {len(matches)} matches")
            return "\n".join(matches) or f"No matches for '{pattern}'"

        # ── get_env ────────────────────────────────────────────────────────
        elif tool_name == "get_env":
            venv_py = os.path.join(project_dir, ".venv", "bin", "python3")
            env_lines = []
            for k in sorted(os.environ):
                v = "***" if any(s in k.upper() for s in ("KEY","SECRET","PASSWORD","TOKEN")) else os.environ[k][:120]
                env_lines.append(f"  {k}={v}")
            return "\n".join([
                f"Python: {venv_py} (exists={os.path.exists(venv_py)})",
                f"Project: {project_dir}",
                f"Platform: {sys.platform}",
                "", "Environment:",
                *env_lines,
            ])[:8000]

        # ── set_startup_cmd ────────────────────────────────────────────────
        elif tool_name == "set_startup_cmd":
            cmd = tool_args.get("startup_cmd", "").strip()
            if not cmd:
                return "[ERROR]: startup_cmd empty"
            await db_update_project(project_id, {"startup_cmd": cmd})
            await db_update_deployment(deployment_id, {"startup_cmd": cmd})
            await _log(f"   🔄 startup_cmd updated → {cmd}")
            return f"OK — startup_cmd = {cmd}"

        # ── notify_user ────────────────────────────────────────────────────
        elif tool_name == "notify_user":
            msg = tool_args.get("message", "")
            lvl = tool_args.get("level", "warning")
            sep = "═" * max(30, len(msg) + 2)
            banner = f"╔══ 🔔 ACTION REQUIRED ══╗\n║ {msg}\n╚{sep}╝"
            await db_add_log(deployment_id, project_id, banner, level=lvl, source="ai")
            return "notice delivered"

        # ── restart_app ────────────────────────────────────────────────────
        elif tool_name == "restart_app":
            project    = await db_get_project(project_id)
            deployment = await db_get_deployment(deployment_id)
            if not project or not deployment:
                return "ERROR: records not found"

            startup_cmd = project.get("startup_cmd") or deployment.get("startup_cmd", "")
            if not startup_cmd:
                return "ERROR: no startup_cmd — call set_startup_cmd first"

            env_vars = project.get("env_vars") or {}

            await stop_deployment_process(deployment_id)
            await asyncio.sleep(1)

            port = allocate_port()
            await db_update_deployment(deployment_id, {"port": port, "status": DeployState.RESTARTING})
            await db_update_project(project_id, {"port": port})

            pid = await start_app_process(
                project_dir=project_dir,
                startup_cmd=startup_cmd,
                port=port,
                deployment_id=deployment_id,
                project_id=project_id,
                env_vars=env_vars,
            )

            if pid:
                await db_update_deployment(deployment_id, {"status": DeployState.RUNNING, "pid": pid})
                await db_update_project(project_id, {"status": DeployState.RUNNING})
                restart_ctx["restarted"] = True
                restart_ctx["port"] = port
                await db_flush_logs(deployment_id)

                # Check still alive after 4s
                await asyncio.sleep(4)
                proc_info = _running_processes.get(deployment_id)
                if proc_info and proc_info["process"].poll() is not None:
                    restart_ctx["restarted"] = False
                    return f"FAILED — process started (PID {pid}) but crashed immediately. Check new logs."
                await _log(f"   ✅ App running on port {port} (PID {pid})")
                return f"OK — running on port {port} PID {pid}"
            else:
                restart_ctx["restarted"] = False
                return "FAILED — process exited immediately after launch"

        # ── mark_fixed ─────────────────────────────────────────────────────
        elif tool_name == "mark_fixed":
            success = tool_args.get("success", False)
            summary = tool_args.get("summary", "")
            icon = "✅" if success else "⚠️"
            await _log(f"🤖 Session complete {icon} — {summary}")
            return "acknowledged"

        else:
            return f"[Unknown tool: {tool_name}]"

    except Exception as exc:
        err = traceback.format_exc()
        logger.warning(f"Tool {tool_name} raised: {err}")
        await _log(f"   ⚠️  {tool_name} error: {exc} — continuing", "warning")
        return f"[TOOL ERROR]: {exc}\n{err[-1000:]}"


# ── Main agent loop ───────────────────────────────────────────────────────────

async def ai_auto_fix(
    deployment_id: str,
    project_id: str,
    trigger: str = "failure",
):
    """
    Bulletproof AI agent loop.
    • Tries Gemini 3.1 Pro first, falls back to OpenRouter models
    • Never stops on tool errors — wraps everything in try/except
    • Runs up to AI_MAX_ROUNDS rounds
    • Logs real-time timestamps on every action
    """
    # Check at least one API key is set
    if not GEMINI_API_KEY and not OPENROUTER_API_KEY:
        await db_add_log(deployment_id, project_id,
            "⚠️  AI agent disabled — set GEMINI_API_KEY (or OPENROUTER_API_KEY) env var",
            level="warning", source="ai")
        return

    if _ai_fixing.get(deployment_id):
        return
    _ai_fixing[deployment_id] = True

    async def _log(msg: str, level: str = "info"):
        ts = _ts()
        await db_add_log(deployment_id, project_id, f"[{ts}] {msg}", level=level, source="ai")

    try:
        await _log("🤖 ╔═══════════════════════════════════════════╗")
        await _log(f"🤖 ║  AI ULTRA AGENT  trigger={trigger}")
        await _log(f"🤖 ║  Primary: {GEMINI_PRIMARY}")
        await _log("🤖 ╚═══════════════════════════════════════════╝")
        await db_flush_logs(deployment_id)

        # ── Gather context ─────────────────────────────────────────────────
        project    = await db_get_project(project_id)
        deployment = await db_get_deployment(deployment_id)
        logs       = await db_get_logs(deployment_id, limit=250)

        if not project or not deployment:
            await _log("❌ Cannot find project/deployment records", "error")
            return

        project_dir = (project or {}).get("deploy_dir", "")
        if not project_dir or not os.path.isdir(project_dir):
            await _log(f"❌ Project directory not found: {project_dir}", "error")
            return

        # Build log tail
        log_tail = "\n".join(
            f"[{l.get('level','info').upper():7s}][{l.get('source','?'):8s}] {l.get('message','')}"
            for l in logs[-250:]
        )

        # Build project tree
        skip_dirs = {".venv", "node_modules", "__pycache__", ".git", "dist", "build"}
        tree = []
        for root, dirs, files in os.walk(project_dir):
            dirs[:] = [d for d in dirs if d not in skip_dirs and not d.startswith(".")]
            depth = root.replace(project_dir, "").count(os.sep)
            pad = "  " * depth
            if depth > 0:
                tree.append(f"{pad}📁 {os.path.basename(root)}/")
            for f in sorted(files):
                tree.append(f"{'  '*(depth+1)}📄 {f}")
            if len(tree) > 80: break

        req_content = ""
        req_path = os.path.join(project_dir, "requirements.txt")
        if os.path.exists(req_path):
            with open(req_path, errors="replace") as f:
                req_content = f.read(3000)

        system_prompt = f"""You are PyDeploy Ultra Agent — an elite, fully autonomous DevOps AI.
You have TOTAL control over the project. Your job: diagnose and fix whatever is broken.

PRIMARY MODEL: {GEMINI_PRIMARY} — optimised for agentic tool use.

TOOLS AVAILABLE:
- run_command: any shell command (venv auto-activated)
- read_file / write_file / patch_file: read and edit any file
- delete_file / rename_file: manage files
- create_dir / delete_dir: manage directories
- list_files / search_in_files: explore the project
- get_env: see environment variables
- set_startup_cmd: fix wrong startup command in DB
- notify_user: tell user when they need to act (e.g. set env vars)
- restart_app: restart after fixes
- mark_fixed: end the session

METHODOLOGY (follow this precisely):
1. Read logs → identify ROOT CAUSE (be specific)
2. list_files → understand project structure
3. read_file on relevant files (with line numbers)
4. Diagnose: syntax error? wrong command? missing package? missing env var?
5. Fix:
   - Wrong startup command → set_startup_cmd + restart_app
   - Missing package → run_command ".venv/bin/pip install X" then restart_app
   - Syntax error → read_file → patch_file (replace bad lines) → verify with run_command "python3 -m py_compile file.py" → restart_app
   - Missing env var → notify_user with exact var name, patch code to handle missing gracefully → mark_fixed(success=False, summary="User must set X=...")
   - Port binding → ensure app uses PORT env var and binds to 0.0.0.0
6. After restart: wait 3s, check if app is alive
7. If still broken: read new logs, iterate — you have {AI_MAX_ROUNDS} rounds
8. ALWAYS call mark_fixed at the end

CRITICAL RULES:
- NEVER give up without calling mark_fixed
- NEVER invent credentials or fake API tokens
- ALWAYS read a file before patching it
- Install packages with: .venv/bin/pip install <pkg>  (NEVER bare pip)
- Fix port: ensure startup uses --port $PORT or --port {"{PORT:-8000}"}
- After each restart_app, check if it survived (tool returns FAILED if it crashed again)
- For missing env vars: patch the code to fail gracefully + notify_user

Framework: {deployment.get("framework", "unknown")}
Startup cmd: {deployment.get("startup_cmd", "unknown")}
Trigger: {trigger}
Max rounds: {AI_MAX_ROUNDS}

requirements.txt:
{req_content or "(not found)"}

Project tree:
{chr(10).join(tree[:80])}
"""

        user_msg = f"""DEPLOYMENT FAILED. Diagnose and fix it completely.

=== RECENT LOGS (last 250 lines) ===
{log_tail[-10000:]}
=== END LOGS ===

Start by identifying the exact error, then fix everything. Go."""

        # Gemini format uses separate systemInstruction + contents
        messages: List[Dict] = [
            {"role": "system",    "content": system_prompt},
            {"role": "user",      "content": user_msg},
        ]

        restart_ctx: Dict = {}
        done = False
        consecutive_empty = 0    # track rounds with no tool calls

        for round_num in range(1, AI_MAX_ROUNDS + 1):
            await _log(f"─── Round {round_num}/{AI_MAX_ROUNDS} ───")
            await db_flush_logs(deployment_id)

            # ── Call the AI ────────────────────────────────────────────────
            assistant_msg = None
            try:
                assistant_msg = await _ai_call(messages, _AI_TOOLS, deployment_id, project_id)
            except Exception as exc:
                await _log(f"⚠️  _ai_call raised: {exc} — retrying round", "warning")
                await asyncio.sleep(3)
                continue

            if assistant_msg is None:
                await _log("⚠️  All models returned None — waiting 10s before retry", "warning")
                await asyncio.sleep(10)
                # Don't count this as a round — but cap retries
                if round_num > 5:
                    await _log("❌ AI unresponsive after 5+ rounds — stopping", "error")
                    break
                continue

            messages.append(assistant_msg)

            # ── Handle text-only response (no tool calls) ──────────────────
            tool_calls = assistant_msg.get("tool_calls") or []
            if not tool_calls:
                text = (assistant_msg.get("content") or "").strip()
                if text:
                    await _log(f"🤖 {text[:800]}")
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    await _log("⚠️  3 rounds with no tool calls — prompting AI to act", "warning")
                    messages.append({
                        "role": "user",
                        "content": (
                            "You must use your tools to actually implement the fix now. "
                            "Don't just describe — call run_command, read_file, write_file, "
                            "patch_file, set_startup_cmd, restart_app, etc. "
                            "If the issue requires user action, call notify_user then mark_fixed."
                        ),
                    })
                    consecutive_empty = 0
                else:
                    messages.append({
                        "role": "user",
                        "content": "Continue. Use tools to implement the fix.",
                    })
                continue

            consecutive_empty = 0

            # ── Execute tools ──────────────────────────────────────────────
            tool_results: List[Dict] = []
            for tc in tool_calls:
                fn        = tc.get("function", {})
                tool_name = fn.get("name", "")
                try:
                    args = json.loads(fn.get("arguments", "{}"))
                except Exception:
                    args = {}

                # Execute — never raises (wrapped internally)
                result = await _exec_tool(
                    tool_name, args,
                    project_dir, deployment_id, project_id,
                    restart_ctx,
                )

                # Store result for message history
                tool_results.append({
                    "role": "tool",
                    "name": tool_name,                    # Gemini needs the name here
                    "tool_call_id": tc.get("id", tool_name),
                    "content": str(result)[:8000],
                })

                if tool_name == "mark_fixed":
                    done = True

            messages.extend(tool_results)
            await db_flush_logs(deployment_id)

            if done:
                icon = "✅" if restart_ctx.get("restarted") else "⚠️"
                await _log(f"🤖 ═══ SESSION COMPLETE {icon} ═══")
                await db_flush_logs(deployment_id)
                break

            # Small breathing room between rounds
            await asyncio.sleep(0.5)

        else:
            await _log(f"⏱️  Reached max rounds ({AI_MAX_ROUNDS}). Session ending.", "warning")

    except Exception as exc:
        tb = traceback.format_exc()
        logger.error(f"AI agent outer exception: {tb}")
        try:
            await db_add_log(deployment_id, project_id,
                f"[{_ts()}] 🤖 Agent crashed: {exc}", level="error", source="ai")
        except Exception:
            pass
    finally:
        try:
            await db_flush_logs(deployment_id)
        except Exception:
            pass
        _ai_fixing.pop(deployment_id, None)


# SECTION 12: HTML UI TEMPLATES
# ──────────────────────────────────────────────────────────────────────────────

def _base_html(title: str, body: str, extra_head: str = "") -> str:
    """Render the base HTML layout with Tailwind CDN and custom styles."""
    return f"""<!DOCTYPE html>
<html lang="en" class="h-full">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{title} — {PLATFORM_TITLE}</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Space+Mono:ital,wght@0,400;0,700;1,400&family=Syne:wght@400;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root {{
      --brand:   #00ff88;
      --brand2:  #00cfff;
      --dark:    #080c10;
      --surface: #0d1117;
      --card:    #161b22;
      --border:  #21262d;
      --muted:   #8b949e;
      --text:    #e6edf3;
    }}
    * {{ box-sizing: border-box; }}
    html, body {{ height: 100%; margin: 0; background: var(--dark); color: var(--text); }}
    body {{ font-family: 'Syne', sans-serif; }}
    code, pre, .mono {{ font-family: 'Space Mono', monospace; }}
    
    .logo-text {{ 
      font-family: 'Space Mono', monospace; font-weight: 700; letter-spacing: -1px;
      background: linear-gradient(135deg, var(--brand) 0%, var(--brand2) 100%);
      -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;
    }}
    .btn-primary {{
      background: linear-gradient(135deg, var(--brand) 0%, var(--brand2) 100%);
      color: #080c10; font-weight: 700; border: none; cursor: pointer;
      transition: opacity .15s, transform .1s;
    }}
    .btn-primary:hover {{ opacity: .88; transform: translateY(-1px); }}
    .btn-secondary {{
      background: transparent; color: var(--text); border: 1px solid var(--border);
      cursor: pointer; transition: background .15s, border-color .15s;
    }}
    .btn-secondary:hover {{ background: var(--card); border-color: var(--muted); }}
    .btn-danger {{
      background: transparent; color: #ff6b6b; border: 1px solid #ff6b6b44;
      cursor: pointer; transition: background .15s;
    }}
    .btn-danger:hover {{ background: #ff6b6b1a; }}
    
    .card {{
      background: var(--card); border: 1px solid var(--border);
      border-radius: 12px; transition: border-color .2s;
    }}
    .card:hover {{ border-color: #30363d; }}
    
    .status-badge {{
      display: inline-flex; align-items: center; gap: 6px;
      font-family: 'Space Mono', monospace; font-size: 11px;
      padding: 3px 10px; border-radius: 20px; font-weight: 700; letter-spacing: .5px;
    }}
    .status-running  {{ background: #00ff8822; color: #00ff88; border: 1px solid #00ff8833; }}
    .status-building {{ background: #ffb30022; color: #ffb300; border: 1px solid #ffb30033; }}
    .status-failed   {{ background: #ff3b3b22; color: #ff6b6b; border: 1px solid #ff3b3b33; }}
    .status-pending  {{ background: #8b949e22; color: #8b949e; border: 1px solid #8b949e33; }}
    .status-stopped  {{ background: #8b949e22; color: #8b949e; border: 1px solid #8b949e33; }}
    
    .dot {{ width: 8px; height: 8px; border-radius: 50%; display: inline-block; }}
    .dot-running  {{ background: #00ff88; box-shadow: 0 0 8px #00ff88; animation: pulse 2s infinite; }}
    .dot-building {{ background: #ffb300; animation: pulse 1s infinite; }}
    .dot-failed   {{ background: #ff6b6b; }}
    .dot-pending  {{ background: #8b949e; }}
    .dot-stopped  {{ background: #8b949e; }}

    @keyframes pulse {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: .4; }} }}
    
    .nav-link {{
      color: var(--muted); text-decoration: none; padding: 6px 12px;
      border-radius: 6px; transition: color .15s, background .15s;
      font-size: 14px;
    }}
    .nav-link:hover {{ color: var(--text); background: var(--card); }}
    
    .log-container {{
      background: #0a0e12; border: 1px solid var(--border); border-radius: 8px;
      font-family: 'Space Mono', monospace; font-size: 12px; line-height: 1.7;
      height: 500px; overflow-y: auto; padding: 16px;
    }}
    .log-line {{ padding: 1px 0; }}
    .log-info    {{ color: #b3c3d6; }}
    .log-error   {{ color: #ff6b6b; }}
    .log-warning {{ color: #ffb300; }}
    .log-success {{ color: #00ff88; }}
    .log-build   {{ color: #00cfff; }}
    .log-ai      {{ color: #c084fc; }}
    
    .upload-zone {{
      border: 2px dashed var(--border); border-radius: 12px;
      transition: border-color .2s, background .2s; cursor: pointer;
      text-align: center; padding: 48px 24px;
    }}
    .upload-zone.drag-over {{
      border-color: var(--brand); background: #00ff8808;
    }}
    
    .form-input {{
      background: var(--surface); border: 1px solid var(--border); color: var(--text);
      border-radius: 8px; padding: 10px 14px; width: 100%; font-family: 'Syne', sans-serif;
      font-size: 14px; transition: border-color .15s, box-shadow .15s; outline: none;
    }}
    .form-input:focus {{
      border-color: var(--brand2); box-shadow: 0 0 0 3px #00cfff15;
    }}
    .form-label {{
      font-size: 13px; font-weight: 600; color: var(--muted);
      display: block; margin-bottom: 6px; letter-spacing: .3px;
    }}
    
    .grid-dots {{
      background-image: radial-gradient(circle, #ffffff08 1px, transparent 1px);
      background-size: 28px 28px;
    }}
    .glow-line {{
      height: 1px;
      background: linear-gradient(90deg, transparent, var(--brand), var(--brand2), transparent);
    }}
    
    .framework-chip {{
      font-family: 'Space Mono', monospace; font-size: 11px;
      padding: 2px 8px; border-radius: 4px; background: #00cfff11;
      color: var(--brand2); border: 1px solid #00cfff22;
    }}
    
    /* Scrollbar styling */
    ::-webkit-scrollbar {{ width: 6px; height: 6px; }}
    ::-webkit-scrollbar-track {{ background: transparent; }}
    ::-webkit-scrollbar-thumb {{ background: #30363d; border-radius: 3px; }}
    ::-webkit-scrollbar-thumb:hover {{ background: #484f58; }}
    
    .animate-in {{
      animation: slideUp .35s ease both;
    }}
    @keyframes slideUp {{
      from {{ opacity: 0; transform: translateY(16px); }}
      to   {{ opacity: 1; transform: translateY(0); }}
    }}
  </style>
  {extra_head}
</head>
<body class="h-full">
{body}
</body>
</html>"""


def _nav_html(user: Optional[Dict] = None) -> str:
    username = user.get("username") or user.get("email", "User") if user else "Guest"
    return f"""
<nav style="background: rgba(13,17,23,0.95); backdrop-filter: blur(12px); border-bottom: 1px solid var(--border); position: sticky; top: 0; z-index: 100;">
  <div style="max-width: 1200px; margin: 0 auto; padding: 0 24px; display: flex; align-items: center; height: 60px; gap: 24px;">
    <a href="/dashboard" style="text-decoration: none;">
      <span class="logo-text" style="font-size: 20px;">&#x25B6; PyDeploy</span>
    </a>
    <div style="flex: 1;"></div>
    {"" if not user else f'''
    <a href="/dashboard" class="nav-link">Dashboard</a>
    <a href="/upload" class="nav-link" style="background: linear-gradient(135deg,var(--brand),var(--brand2)); color:#080c10; font-weight:700; padding: 7px 16px; border-radius: 8px;">+ Deploy</a>
    <div style="width:1px; height:24px; background: var(--border);"></div>
    <span style="font-size: 13px; color: var(--muted);">{username}</span>
    <a href="/logout" class="nav-link" style="font-size:13px;">Logout</a>
    '''}
  </div>
</nav>"""


def render_login_page(error: str = "") -> str:
    body = f"""
{_nav_html()}
<div class="grid-dots" style="min-height: calc(100vh - 60px); display: flex; align-items: center; justify-content: center; padding: 40px 20px;">
  <div class="animate-in" style="width: 100%; max-width: 420px;">
    <div style="text-align: center; margin-bottom: 40px;">
      <div class="logo-text" style="font-size: 48px; margin-bottom: 8px;">&#x25B6;</div>
      <h1 style="font-size: 28px; font-weight: 800; margin: 0 0 8px;">Welcome back</h1>
      <p style="color: var(--muted); margin: 0; font-size: 14px;">Deploy your Python apps in seconds.</p>
    </div>
    
    <div class="card" style="padding: 32px;">
      {'<div style="background:#ff3b3b22;border:1px solid #ff3b3b44;border-radius:8px;padding:12px;margin-bottom:20px;color:#ff6b6b;font-size:13px;">⚠ ' + error + '</div>' if error else ''}
      
      <form method="POST" action="/login" id="loginForm">
        <div style="margin-bottom: 20px;">
          <label class="form-label">Email</label>
          <input class="form-input" type="email" name="email" placeholder="you@example.com" required>
        </div>
        <div style="margin-bottom: 24px;">
          <label class="form-label">Password</label>
          <input class="form-input" type="password" name="password" placeholder="••••••••" required>
        </div>
        <button class="btn-primary" style="width:100%;padding:12px;border-radius:8px;font-size:15px;" type="submit">
          Sign In →
        </button>
      </form>
      
      <div style="margin-top: 20px; text-align: center;">
        <span style="color: var(--muted); font-size: 13px;">Don't have an account? </span>
        <a href="/register" style="color: var(--brand2); font-size: 13px; text-decoration: none; font-weight: 600;">Register</a>
      </div>
    </div>
  </div>
</div>"""
    return _base_html("Login", body)


def render_register_page(error: str = "") -> str:
    body = f"""
{_nav_html()}
<div class="grid-dots" style="min-height: calc(100vh - 60px); display: flex; align-items: center; justify-content: center; padding: 40px 20px;">
  <div class="animate-in" style="width: 100%; max-width: 420px;">
    <div style="text-align: center; margin-bottom: 40px;">
      <div class="logo-text" style="font-size: 48px; margin-bottom: 8px;">&#x25B6;</div>
      <h1 style="font-size: 28px; font-weight: 800; margin: 0 0 8px;">Create account</h1>
      <p style="color: var(--muted); margin: 0; font-size: 14px;">Start deploying in minutes.</p>
    </div>
    
    <div class="card" style="padding: 32px;">
      {'<div style="background:#ff3b3b22;border:1px solid #ff3b3b44;border-radius:8px;padding:12px;margin-bottom:20px;color:#ff6b6b;font-size:13px;">⚠ ' + error + '</div>' if error else ''}
      
      <form method="POST" action="/register">
        <div style="margin-bottom: 20px;">
          <label class="form-label">Username</label>
          <input class="form-input" type="text" name="username" placeholder="yourname" required>
        </div>
        <div style="margin-bottom: 20px;">
          <label class="form-label">Email</label>
          <input class="form-input" type="email" name="email" placeholder="you@example.com" required>
        </div>
        <div style="margin-bottom: 24px;">
          <label class="form-label">Password</label>
          <input class="form-input" type="password" name="password" placeholder="••••••••" minlength="8" required>
        </div>
        <button class="btn-primary" style="width:100%;padding:12px;border-radius:8px;font-size:15px;" type="submit">
          Create Account →
        </button>
      </form>
      
      <div style="margin-top: 20px; text-align: center;">
        <span style="color: var(--muted); font-size: 13px;">Already have an account? </span>
        <a href="/login" style="color: var(--brand2); font-size: 13px; text-decoration: none; font-weight: 600;">Sign in</a>
      </div>
    </div>
  </div>
</div>"""
    return _base_html("Register", body)


def render_dashboard_page(user: Dict, projects: List[Dict]) -> str:
    username = user.get("username") or user.get("email", "User")

    # Project cards HTML
    if not projects:
        projects_html = """
        <div style="text-align:center; padding: 80px 20px; color: var(--muted);">
          <div style="font-size: 48px; margin-bottom: 16px; opacity:.4;">⬡</div>
          <div style="font-size: 18px; font-weight: 600; margin-bottom: 8px; color: var(--text);">No deployments yet</div>
          <p style="font-size: 14px; margin: 0 0 24px;">Upload your first Python project to get started.</p>
          <a href="/upload" class="btn-primary" style="display:inline-block;text-decoration:none;padding:12px 28px;border-radius:8px;font-size:14px;">
            + Deploy First App
          </a>
        </div>"""
    else:
        cards = []
        for p in projects:
            status = p.get("status", "unknown")
            fw = p.get("framework") or "unknown"
            port = p.get("port") or "—"
            created = p.get("created_at", "")[:10]
            name = p.get("name", "Unnamed")
            pid = p.get("id")

            cards.append(f"""
            <div class="card animate-in" style="padding: 24px; position: relative; overflow: hidden;">
              <div style="position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,var(--brand),var(--brand2));opacity:{'.8' if status=='running' else '.2'};"></div>
              <div style="display: flex; align-items: flex-start; justify-content: space-between; margin-bottom: 16px;">
                <div>
                  <h3 style="margin:0 0 6px; font-size:16px; font-weight:700;">{name}</h3>
                  <span class="framework-chip">{fw}</span>
                </div>
                <span class="status-badge status-{status}">
                  <span class="dot dot-{status}"></span>
                  {status.upper()}
                </span>
              </div>
              <div style="display: flex; gap: 20px; margin-bottom: 20px; font-size: 13px; color: var(--muted);">
                <span>PORT: <strong style="color:var(--text);">{port}</strong></span>
                <span>CREATED: <strong style="color:var(--text);">{created}</strong></span>
              </div>
              <div style="display: flex; gap: 8px;">
                <a href="/project/{pid}" class="btn-secondary" style="text-decoration:none;padding:7px 14px;border-radius:6px;font-size:13px;">View</a>
                <a href="/logs/{pid}" class="btn-secondary" style="text-decoration:none;padding:7px 14px;border-radius:6px;font-size:13px;">Logs</a>
                <a href="/p/{pid}" target="_blank" rel="noopener" class="btn-secondary" style="text-decoration:none;padding:7px 14px;border-radius:6px;font-size:13px;color:var(--brand);border-color:#00ff8833;">Open ↗</a>
                <button onclick="deleteProject('{pid}')" class="btn-danger" style="padding:7px 14px;border-radius:6px;font-size:13px;margin-left:auto;">Delete</button>
              </div>
            </div>""")
        projects_html = f"""<div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(340px, 1fr)); gap: 16px;">
            {"".join(cards)}
        </div>"""

    stats_html = f"""
    <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 12px; margin-bottom: 32px;">
      {_stat_card("Total Apps", str(len(projects)), "⬡")}
      {_stat_card("Running", str(sum(1 for p in projects if p.get('status') == 'running')), "▶", "var(--brand)")}
      {_stat_card("Failed", str(sum(1 for p in projects if p.get('status') == 'failed')), "⬡", "#ff6b6b")}
    </div>"""

    body = f"""
{_nav_html(user)}
<div style="max-width: 1200px; margin: 0 auto; padding: 40px 24px;">
  <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:32px;">
    <div>
      <h1 style="margin:0 0 4px; font-size:28px; font-weight:800;">
        Good to see you, <span style="color:var(--brand)">{username}</span>
      </h1>
      <p style="margin:0; color:var(--muted); font-size:14px;">Manage and monitor your deployed applications.</p>
    </div>
    <a href="/upload" class="btn-primary" style="text-decoration:none;padding:12px 24px;border-radius:8px;font-size:14px;display:flex;align-items:center;gap:8px;">
      + New Deployment
    </a>
  </div>
  
  {stats_html}
  
  <div class="glow-line" style="margin-bottom: 28px;"></div>
  
  <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:20px;">
    <h2 style="margin:0; font-size:18px; font-weight:700;">Your Applications</h2>
    <span style="font-size:13px; color:var(--muted);">{len(projects)} total</span>
  </div>
  
  {projects_html}
</div>

<script>
async function deleteProject(id) {{
  if (!confirm('Delete this project and all its data?')) return;
  const r = await fetch('/api/projects/' + id, {{method: 'DELETE'}});
  if (r.ok) location.reload();
  else alert('Delete failed: ' + (await r.text()));
}}
</script>"""
    return _base_html("Dashboard", body)


def _stat_card(label: str, value: str, icon: str, color: str = "var(--brand2)") -> str:
    return f"""
    <div class="card" style="padding:20px;">
      <div style="color:{color};font-size:20px;margin-bottom:8px;">{icon}</div>
      <div style="font-size:26px;font-weight:800;font-family:'Space Mono',monospace;margin-bottom:2px;">{value}</div>
      <div style="font-size:12px;color:var(--muted);font-weight:600;letter-spacing:.5px;text-transform:uppercase;">{label}</div>
    </div>"""


def render_upload_page(user: Dict, error: str = "", success: str = "") -> str:
    body = f"""
{_nav_html(user)}
<div style="max-width: 700px; margin: 0 auto; padding: 48px 24px;">
  <div class="animate-in">
    <div style="margin-bottom: 32px;">
      <h1 style="margin:0 0 8px; font-size:28px; font-weight:800;">Deploy a New App</h1>
      <p style="margin:0; color:var(--muted); font-size:14px;">Upload any ZIP — Python, static HTML, bots, workers, full-stack. We handle the rest.</p>
    </div>

    {{'<div style="background:#00ff8811;border:1px solid #00ff8833;border-radius:8px;padding:14px;margin-bottom:24px;color:#00ff88;font-size:13px;">✓ ' + success + '</div>' if success else ''}}
    {{'<div style="background:#ff3b3b22;border:1px solid #ff3b3b44;border-radius:8px;padding:14px;margin-bottom:24px;color:#ff6b6b;font-size:13px;">⚠ ' + error + '</div>' if error else ''}}

    <form id="uploadForm" enctype="multipart/form-data">

      <!-- Project type tabs -->
      <div style="display:flex;gap:8px;margin-bottom:20px;">
        <button type="button" class="type-tab active" onclick="setType('python')" id="tab-python"
                style="flex:1;padding:10px;border-radius:8px;font-size:13px;font-weight:600;cursor:pointer;border:1.5px solid #7c3aed;background:#7c3aed22;color:#c084fc;">
          🐍 Python App
        </button>
        <button type="button" class="type-tab" onclick="setType('frontend')" id="tab-frontend"
                style="flex:1;padding:10px;border-radius:8px;font-size:13px;font-weight:600;cursor:pointer;border:1.5px solid var(--border);background:transparent;color:var(--muted);">
          🌐 Frontend Only
        </button>
      </div>

      <div class="card" style="padding:28px;margin-bottom:20px;">
        <h3 style="margin:0 0 20px;font-size:15px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;">Project Details</h3>
        <div style="margin-bottom:20px;">
          <label class="form-label">Project Name *</label>
          <input class="form-input" type="text" name="name" id="projectName" placeholder="my-awesome-app" required>
        </div>
        <div style="margin-bottom:0;">
          <label class="form-label">Description</label>
          <input class="form-input" type="text" name="description" placeholder="Brief description">
        </div>
      </div>

      <!-- Python hints (hidden for frontend-only) -->
      <div id="pythonHints" class="card" style="padding:20px;margin-bottom:20px;border-color:#7c3aed44;background:#7c3aed08;">
        <div style="font-size:12px;color:#a78bfa;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:10px;">📋 Supported Python Projects</div>
        <div style="display:flex;flex-wrap:wrap;gap:6px;">
          {{''.join(f'<span class="framework-chip">{fw}</span>' for fw in ["FastAPI","Flask","Django","Tornado","aiohttp","Streamlit","Gradio","Telegram Bot","Discord Bot","gRPC","Celery","Workers","Scripts"])}}
        </div>
      </div>

      <!-- Frontend hints -->
      <div id="frontendHints" style="display:none;" class="card" style="padding:20px;margin-bottom:20px;border-color:#06b6d444;background:#06b6d408;">
        <div style="font-size:12px;color:#22d3ee;font-weight:700;letter-spacing:.5px;text-transform:uppercase;margin-bottom:10px;">🌐 Supported Frontend Projects</div>
        <div style="display:flex;flex-wrap:wrap;gap:6px;">
          {{''.join(f'<span class="framework-chip">{fw}</span>' for fw in ["HTML / CSS / JS","React","Vue","Svelte","Next.js","Vite","Static Site","Landing Page","Dashboard"])}}
        </div>
        <div style="margin-top:10px;font-size:12px;color:var(--muted);">No backend needed — your files are served directly. Just include an <code style="color:#22d3ee;">index.html</code> in your ZIP root.</div>
      </div>

      <div class="card" style="padding:28px;margin-bottom:20px;">
        <h3 style="margin:0 0 20px;font-size:15px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;">Project ZIP</h3>
        <div class="upload-zone" id="dropzone" onclick="document.getElementById('zipInput').click()">
          <div id="dropzoneContent">
            <div style="font-size:40px;margin-bottom:16px;opacity:.5;">⬡</div>
            <div style="font-size:16px;font-weight:700;margin-bottom:8px;">Drop your ZIP here</div>
            <div style="font-size:13px;color:var(--muted);">or click to browse · Max {MAX_ZIP_SIZE_MB}MB</div>
          </div>
        </div>
        <input type="file" id="zipInput" name="file" accept=".zip" style="display:none;" onchange="handleFileSelect(this)">
        <input type="hidden" id="projectType" name="project_type" value="python">
      </div>

      <button class="btn-primary" type="button" onclick="submitDeployment()" id="deployBtn"
              style="width:100%;padding:14px;border-radius:10px;font-size:16px;" disabled>
        🚀 Deploy Application
      </button>
    </form>

    <!-- Progress section -->
    <div id="progressSection" style="display:none;margin-top:28px;">
      <div class="card" style="padding:24px;">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;">
          <div class="dot dot-building" style="width:12px;height:12px;"></div>
          <span style="font-weight:700;" id="progressTitle">Deploying...</span>
        </div>
        <div class="log-container" id="progressLog" style="height:300px;">
          <div class="log-line log-build">Uploading ZIP file...</div>
        </div>
      </div>
    </div>

    <!-- ═══ ENV VARS DETECTION MODAL ═══ -->
    <div id="envModal" style="display:none;position:fixed;inset:0;background:#000000cc;z-index:1000;display:none;align-items:center;justify-content:center;padding:20px;">
      <div style="background:#0d1117;border:1.5px solid #f472b655;border-radius:16px;max-width:560px;width:100%;max-height:90vh;overflow-y:auto;box-shadow:0 0 60px #f472b622;">

        <!-- Header -->
        <div style="padding:20px 24px;border-bottom:1px solid #f472b633;background:linear-gradient(135deg,#f472b615,#7c3aed15);">
          <div style="display:flex;align-items:center;gap:12px;">
            <span style="font-size:24px;">🔑</span>
            <div>
              <div style="font-weight:800;color:#f472b6;font-size:15px;">Environment Variables Detected</div>
              <div style="font-size:12px;color:#9b8ec4;margin-top:2px;">Your project references these secrets. Fill in what you know — you can always add more later.</div>
            </div>
          </div>
        </div>

        <!-- Vars list -->
        <div style="padding:20px 24px;" id="envModalVarsList"></div>

        <!-- Actions -->
        <div style="padding:0 24px 20px;display:flex;gap:10px;align-items:center;">
          <button onclick="saveEnvAndContinue()" id="envSaveBtn"
                  style="background:linear-gradient(135deg,#7c3aed,#f472b6);border:none;color:#fff;font-weight:700;padding:10px 24px;border-radius:8px;cursor:pointer;font-size:14px;flex:1;">
            💾 Save & Continue Deployment
          </button>
          <button onclick="skipEnvAndContinue()"
                  style="background:transparent;border:1px solid #ffffff22;color:var(--muted);font-size:13px;padding:10px 16px;border-radius:8px;cursor:pointer;white-space:nowrap;">
            I'll do it later
          </button>
        </div>
        <div style="padding:0 24px 16px;font-size:11px;color:#64748b;text-align:center;">
          Empty fields are skipped — your app will still deploy. You can set vars anytime from the project page.
        </div>
      </div>
    </div>

    <!-- Requirements box -->
    <div style="margin-top:32px;border:1.5px solid #7c3aed55;border-radius:14px;overflow:hidden;background:linear-gradient(135deg,#0d0a1a,#0a0f18);">
      <div style="display:flex;align-items:center;gap:12px;padding:18px 24px;background:linear-gradient(135deg,#7c3aed22,#06b6d422);border-bottom:1px solid #7c3aed33;">
        <span style="font-size:22px;">📋</span>
        <div>
          <div style="font-size:15px;font-weight:800;color:#e2d9ff;">Deployment Requirements</div>
          <div style="font-size:12px;color:#9b8ec4;margin-top:2px;">Follow these to deploy with zero errors</div>
        </div>
        <span style="margin-left:auto;font-size:11px;font-weight:700;background:#7c3aed33;color:#c084fc;padding:4px 10px;border-radius:20px;">READ FIRST</span>
      </div>
      <div style="padding:20px 24px;display:grid;gap:14px;">
        <div style="background:#ffffff07;border-radius:8px;padding:14px 16px;border-left:3px solid #00cfff;font-size:13px;color:#cbd5e1;line-height:1.8;">
          <strong style="color:#00cfff;">📁 ZIP Structure:</strong> Files in root, max {MAX_ZIP_SIZE_MB}MB. Don't include <code style="color:#ff6b6b;">.venv/</code> or <code style="color:#ff6b6b;">node_modules/</code>
        </div>
        <div style="background:#ffffff07;border-radius:8px;padding:14px 16px;border-left:3px solid #facc15;font-size:13px;color:#cbd5e1;line-height:1.8;">
          <strong style="color:#facc15;">🔌 Port:</strong> Your app <strong>MUST</strong> bind to <code style="color:#facc15;">0.0.0.0</code> and use <code style="color:#facc15;">os.environ["PORT"]</code>
        </div>
        <div style="background:#ffffff07;border-radius:8px;padding:14px 16px;border-left:3px solid #00ff88;font-size:13px;color:#cbd5e1;line-height:1.8;">
          <strong style="color:#00ff88;">📦 Dependencies:</strong> Include <code style="color:#00ff88;">requirements.txt</code> for Python, or <code style="color:#00ff88;">package.json</code> for Node.js
        </div>
        <div style="background:linear-gradient(135deg,#7c3aed15,#06b6d415);border:1px solid #7c3aed33;border-radius:8px;padding:14px 16px;display:flex;gap:10px;">
          <span style="font-size:18px;">🤖</span>
          <div style="font-size:12px;color:#94a3b8;line-height:1.7;">
            <strong style="color:#c084fc;">AI Auto-Fix (Gemini 3.1 Pro):</strong> If deployment fails, the AI agent automatically diagnoses and fixes wrong commands, missing packages, syntax errors, port issues, and more.
          </div>
        </div>
      </div>
    </div>
  </div>
</div>

<script>
let _projectType = 'python';
let _deployedProjectId = null;
let _detectedEnvKeys = [];
let _deployFormData = null;

function setType(type) {{
  _projectType = type;
  document.getElementById('projectType').value = type;
  document.querySelectorAll('.type-tab').forEach(t => {{
    t.style.borderColor = 'var(--border)';
    t.style.background = 'transparent';
    t.style.color = 'var(--muted)';
  }});
  const tab = document.getElementById('tab-' + type);
  tab.style.borderColor = '#7c3aed';
  tab.style.background = '#7c3aed22';
  tab.style.color = '#c084fc';
  document.getElementById('pythonHints').style.display = type === 'python' ? 'block' : 'none';
  document.getElementById('frontendHints').style.display = type === 'frontend' ? 'block' : 'none';
  const btn = document.getElementById('deployBtn');
  btn.textContent = type === 'frontend' ? '🌐 Deploy Frontend' : '🚀 Deploy Application';
}}

const dropzone = document.getElementById('dropzone');
const zipInput = document.getElementById('zipInput');
const deployBtn = document.getElementById('deployBtn');

dropzone.addEventListener('dragover', e => {{ e.preventDefault(); dropzone.classList.add('drag-over'); }});
dropzone.addEventListener('dragleave', () => dropzone.classList.remove('drag-over'));
dropzone.addEventListener('drop', e => {{
  e.preventDefault(); dropzone.classList.remove('drag-over');
  if (e.dataTransfer.files[0]) {{ zipInput.files = e.dataTransfer.files; handleFileSelect(zipInput); }}
}});

function handleFileSelect(input) {{
  if (!input.files[0]) return;
  const f = input.files[0];
  const mb = (f.size / 1024 / 1024).toFixed(1);
  document.getElementById('dropzoneContent').innerHTML = `
    <div style="font-size:32px;margin-bottom:12px;">📦</div>
    <div style="font-size:15px;font-weight:700;margin-bottom:4px;color:var(--brand)">${{f.name}}</div>
    <div style="font-size:13px;color:var(--muted)">${{mb}} MB</div>
  `;
  deployBtn.disabled = !document.getElementById('projectName').value.trim();
}}

document.getElementById('projectName').addEventListener('input', function() {{
  deployBtn.disabled = !this.value.trim() || !zipInput.files[0];
}});

// ── Step 1: Click Deploy → scan ZIP for env vars then show modal ─────────
async function submitDeployment() {{
  const name = document.getElementById('projectName').value.trim();
  const file = zipInput.files[0];
  if (!name || !file) return;

  deployBtn.disabled = true;
  deployBtn.textContent = '🔍 Scanning project...';

  // First: scan the ZIP on the server for env var keys
  const scanFd = new FormData();
  scanFd.append('file', file);
  let envKeys = [];
  try {{
    const sr = await fetch('/api/scan-zip-env', {{method: 'POST', body: scanFd}});
    if (sr.ok) {{
      const sd = await sr.json();
      envKeys = sd.keys || [];
    }}
  }} catch(e) {{}}

  // Build FormData for actual deploy (save for after modal)
  const fd = new FormData();
  fd.append('name', name);
  fd.append('description', document.querySelector('input[name=description]').value);
  fd.append('file', file);
  fd.append('project_type', _projectType);
  _deployFormData = fd;
  _detectedEnvKeys = envKeys;

  if (envKeys.length > 0) {{
    // Show env modal before deploying
    showEnvModal(envKeys);
  }} else {{
    // No env vars detected — deploy directly
    await startActualDeploy({{}});
  }}
}}

function showEnvModal(keys) {{
  const list = document.getElementById('envModalVarsList');
  list.innerHTML = keys.map(k => `
    <div style="display:flex;gap:8px;margin-bottom:10px;align-items:center;">
      <div style="width:40%;font-family:'Space Mono',monospace;font-size:12px;color:#c084fc;
                  background:#7c3aed1a;padding:8px 10px;border-radius:6px;border:1px solid #7c3aed33;
                  overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${{k}}">${{k}}</div>
      <div style="flex:1;position:relative;">
        <input type="password" placeholder="value (optional)"
               style="width:100%;background:#0a0e12;border:1px solid #ffffff22;color:#e2e8f0;
                      border-radius:6px;padding:8px 36px 8px 10px;font-size:12px;font-family:'Space Mono',monospace;box-sizing:border-box;"
               data-env-key="${{k}}"
               oninput="this.style.borderColor=this.value?'#00ff8855':'#ffffff22'">
        <button type="button" onclick="togglePw(this)"
                style="position:absolute;right:8px;top:50%;transform:translateY(-50%);background:none;border:none;color:#64748b;cursor:pointer;font-size:13px;padding:0;">👁</button>
      </div>
    </div>
  `).join('');
  const modal = document.getElementById('envModal');
  modal.style.display = 'flex';
  // Focus first input
  setTimeout(() => {{ const f = modal.querySelector('input[data-env-key]'); if(f) f.focus(); }}, 100);
}}

function togglePw(btn) {{
  const inp = btn.previousElementSibling;
  inp.type = inp.type==='password' ? 'text' : 'password';
  btn.textContent = inp.type==='password' ? '👁' : '🙈';
}}

async function saveEnvAndContinue() {{
  const envVars = {{}};
  document.querySelectorAll('[data-env-key]').forEach(inp => {{
    const k = inp.getAttribute('data-env-key');
    const v = inp.value.trim();
    if (v) envVars[k] = v;
  }});
  document.getElementById('envModal').style.display = 'none';
  await startActualDeploy(envVars);
}}

async function skipEnvAndContinue() {{
  document.getElementById('envModal').style.display = 'none';
  await startActualDeploy({{}});
}}

async function startActualDeploy(envVars) {{
  deployBtn.disabled = true;
  deployBtn.textContent = 'Deploying...';
  document.getElementById('progressSection').style.display = 'block';
  document.getElementById('progressSection').scrollIntoView({{behavior:'smooth', block:'nearest'}});

  const log = document.getElementById('progressLog');
  const addLog = (msg, cls='log-info') => {{
    const d = document.createElement('div');
    d.className = 'log-line ' + cls;
    d.textContent = new Date().toLocaleTimeString() + '  ' + msg;
    log.appendChild(d);
    log.scrollTop = log.scrollHeight;
  }};

  _deployFormData.set('env_vars', JSON.stringify(envVars));

  try {{
    addLog('Uploading ZIP...', 'log-build');
    const r = await fetch('/api/deploy', {{method:'POST', body:_deployFormData}});
    const data = await r.json();
    if (!r.ok) throw new Error(data.detail || 'Upload failed');

    addLog('Project created! Build starting...', 'log-success');
    _deployedProjectId = data.project_id;

    let lastCount = 0;
    const poll = async () => {{
      try {{
        const lr = await fetch('/api/projects/' + _deployedProjectId + '/logs');
        const logs = await lr.json();
        for (let i = lastCount; i < logs.length; i++) {{
          const l = logs[i];
          const cls = l.source==='ai'?'log-ai': l.level==='error'?'log-error': l.source==='build'?'log-build': l.level==='warning'?'log-warning':'log-info';
          addLog(l.message, cls);
        }}
        lastCount = logs.length;

        const pr = await fetch('/api/projects/' + _deployedProjectId);
        const proj = await pr.json();
        if (proj.status === 'running') {{
          addLog('🎉 Deployment successful! Redirecting...', 'log-success');
          setTimeout(() => window.location.href = '/project/' + _deployedProjectId, 1500);
        }} else if (proj.status === 'failed') {{
          addLog('❌ Build failed — AI agent is trying to fix it automatically. Check logs.', 'log-error');
          setTimeout(() => window.location.href = '/project/' + _deployedProjectId, 3000);
        }} else {{
          setTimeout(poll, 1500);
        }}
      }} catch(e) {{ setTimeout(poll, 2000); }}
    }};
    setTimeout(poll, 1500);

  }} catch(e) {{
    addLog('Error: ' + e.message, 'log-error');
    deployBtn.disabled = false;
    deployBtn.textContent = _projectType==='frontend' ? '🌐 Deploy Frontend' : '🚀 Deploy Application';
  }}
}}

// Close modal on backdrop click
document.getElementById('envModal').addEventListener('click', function(e) {{
  if (e.target === this) skipEnvAndContinue();
}});
</script>"""
    return _base_html("Upload", body)



def render_project_page(user: Dict, project: Dict, deployments: List[Dict]) -> str:
    name = project.get("name", "Unknown")
    fw = project.get("framework") or "unknown"
    status = project.get("status", "unknown")
    port = project.get("port") or "—"
    startup_cmd = project.get("startup_cmd") or "—"
    pid = project.get("id")
    created = project.get("created_at", "")[:19].replace("T", " ")

    deployments_html = ""
    for d in deployments[:10]:
        ds = d.get("status", "unknown")
        dcreated = d.get("created_at", "")[:19].replace("T", "  ")
        did = d.get("id")
        deployments_html += f"""
        <div style="display:flex;align-items:center;padding:12px 16px;border-bottom:1px solid var(--border);">
          <div style="flex:1;">
            <span class="mono" style="font-size:12px;color:var(--muted);">{did[:8]}...</span>
            <span style="font-size:12px;color:var(--muted);margin-left:12px;">{dcreated}</span>
          </div>
          <span class="status-badge status-{ds}" style="margin-right:12px;">{ds.upper()}</span>
          <a href="/logs/{did}" class="btn-secondary" style="text-decoration:none;padding:5px 12px;border-radius:6px;font-size:12px;">Logs</a>
          <button onclick="restartDeployment('{did}')" class="btn-secondary" style="padding:5px 12px;border-radius:6px;font-size:12px;margin-left:6px;">Restart</button>
        </div>"""

    body = f"""
{_nav_html(user)}
<div style="max-width: 1000px; margin: 0 auto; padding: 40px 24px;">
  <div class="animate-in">
    <div style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:32px;">
      <div>
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:8px;">
          <a href="/dashboard" style="color:var(--muted);text-decoration:none;font-size:14px;">← Dashboard</a>
        </div>
        <div style="display:flex;align-items:center;gap:16px;">
          <h1 style="margin:0;font-size:28px;font-weight:800;">{name}</h1>
          <span class="status-badge status-{status}">
            <span class="dot dot-{status}"></span>
            {status.upper()}
          </span>
        </div>
      </div>
      <div style="display:flex;gap:8px;">
        <a href="/logs/{pid}" class="btn-secondary" style="text-decoration:none;padding:10px 18px;border-radius:8px;font-size:13px;">📋 Logs</a>
        <button onclick="restartLatest()" class="btn-secondary" style="padding:10px 18px;border-radius:8px;font-size:13px;">⟳ Restart</button>
        <button onclick="stopProject()" class="btn-danger" style="padding:10px 18px;border-radius:8px;font-size:13px;">■ Stop</button>
      </div>
    </div>
    
    <div class="card" style="padding:20px 24px;margin-bottom:24px;display:flex;align-items:center;gap:16px;background:linear-gradient(135deg,#00ff8808,#00cfff08);border-color:#00ff8833;">
      <div style="font-size:28px;">🌐</div>
      <div style="flex:1;min-width:0;">
        <div style="font-size:11px;font-weight:700;color:var(--muted);letter-spacing:.6px;text-transform:uppercase;margin-bottom:5px;">Live Preview URL</div>
        <div class="mono" style="font-size:14px;color:var(--brand);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">/p/{pid}</div>
      </div>
      <a href="/p/{pid}" target="_blank" rel="noopener"
         class="btn-primary" style="text-decoration:none;padding:10px 22px;border-radius:8px;font-size:13px;white-space:nowrap;flex-shrink:0;">
        Open App ↗
      </a>
    </div>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:28px;">
      <div class="card" style="padding:24px;">
        <div style="font-size:12px;color:var(--muted);font-weight:600;letter-spacing:.5px;text-transform:uppercase;margin-bottom:16px;">Configuration</div>
        <div style="display:flex;flex-direction:column;gap:14px;">
          {_detail_row("Framework", f'<span class="framework-chip">{fw}</span>')}
          {_detail_row("Port", f'<span class="mono" style="font-size:13px;">{port}</span>')}
          {_detail_row("Created", created)}
          {_detail_row("Status", f'<span class="status-badge status-{status}">{status.upper()}</span>')}
        </div>
      </div>
      <div class="card" style="padding:24px;">
        <div style="font-size:12px;color:var(--muted);font-weight:600;letter-spacing:.5px;text-transform:uppercase;margin-bottom:16px;">Startup Command</div>
        <pre style="margin:0;font-family:'Space Mono',monospace;font-size:12px;color:var(--brand2);white-space:pre-wrap;word-break:break-all;background:#0a0e12;padding:14px;border-radius:6px;border:1px solid var(--border);">{startup_cmd}</pre>
      </div>
    </div>
    
    <!-- ENV VARS PANEL -->
    <div class="card" style="padding:0;overflow:hidden;margin-bottom:24px;">
      <div style="padding:18px 24px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:12px;">
        <span style="font-size:18px;">🔑</span>
        <div style="flex:1;">
          <h3 style="margin:0;font-size:15px;font-weight:700;">Environment Variables</h3>
          <p style="margin:2px 0 0;font-size:12px;color:var(--muted);">Changes take effect immediately — app restarts automatically</p>
        </div>
        <button onclick="scanAndDetectEnvVars()" class="btn-secondary"
                style="padding:8px 14px;border-radius:7px;font-size:12px;display:flex;align-items:center;gap:6px;">
          🔍 Detect from Code
        </button>
      </div>

      <!-- Existing vars list -->
      <div style="padding:20px 24px;" id="envVarsPanel">
        <div id="envVarsList">
          <!-- populated by JS -->
        </div>
        <button onclick="addProjectEnvVar()" class="btn-secondary"
                style="padding:7px 14px;border-radius:6px;font-size:12px;margin-top:12px;">
          + Add Variable
        </button>
        <button onclick="saveEnvVars()" id="saveEnvBtn"
                class="btn-primary"
                style="padding:7px 18px;border-radius:6px;font-size:12px;margin-top:12px;margin-left:10px;display:none;">
          💾 Save & Restart
        </button>
        <span id="envSaveStatus" style="font-size:12px;color:var(--muted);margin-left:10px;"></span>
      </div>

      <!-- Detected missing vars box (hidden by default) -->
      <div id="detectedEnvBox" style="display:none;margin:0 24px 20px;border:1.5px solid #f472b633;border-radius:10px;overflow:hidden;background:#0d0a18;">
        <div style="background:linear-gradient(135deg,#f472b622,#7c3aed22);padding:14px 18px;display:flex;align-items:center;gap:10px;border-bottom:1px solid #f472b633;">
          <span style="font-size:18px;">🤖</span>
          <div>
            <div style="font-weight:700;color:#f472b6;font-size:13px;">Missing Environment Variables Detected</div>
            <div style="font-size:11px;color:#9b8ec4;margin-top:2px;">Your code references these vars but they're not set. Fill them in and click Apply.</div>
          </div>
          <button onclick="document.getElementById('detectedEnvBox').style.display='none'"
                  style="margin-left:auto;background:none;border:none;color:#9b8ec4;cursor:pointer;font-size:18px;padding:0;">×</button>
        </div>
        <div style="padding:16px 18px;" id="detectedVarsList"></div>
        <div style="padding:0 18px 16px;display:flex;align-items:center;gap:10px;">
          <button onclick="applyDetectedVars()" id="applyDetectedBtn"
                  style="background:linear-gradient(135deg,#7c3aed,#f472b6);border:none;color:#fff;font-weight:700;padding:9px 22px;border-radius:7px;cursor:pointer;font-size:13px;">
            ✅ Apply & Restart
          </button>
          <span id="applyStatus" style="font-size:12px;color:var(--muted);"></span>
        </div>
      </div>
    </div>

    <div class="card" style="padding:0; overflow:hidden;">
      <div style="padding:20px 24px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;">
        <h3 style="margin:0;font-size:15px;font-weight:700;">Deployment History</h3>
        <span style="font-size:13px;color:var(--muted);">{len(deployments)} total</span>
      </div>
      {deployments_html if deployments_html else '<div style="padding:32px;text-align:center;color:var(--muted);font-size:14px;">No deployments yet.</div>'}
    </div>
  </div>
</div>

<script>
const projectId = '{pid}';
// Existing env vars from DB
let _currentEnvVars = {json.dumps(project.get("env_vars") or {})};
let _dirty = false;

// ── Render current env vars ─────────────────────────────────────────────────
function renderEnvVars() {{
  const list = document.getElementById('envVarsList');
  const entries = Object.entries(_currentEnvVars);
  if (!entries.length) {{
    list.innerHTML = '<div style="font-size:13px;color:var(--muted);padding:8px 0;">No environment variables set yet.</div>';
    return;
  }}
  list.innerHTML = entries.map(([k, v]) => `
    <div style="display:flex;gap:8px;margin-bottom:8px;align-items:center;" data-envrow>
      <input class="form-input mono-input" value="${{k}}" placeholder="KEY"
             style="width:35%;font-family:'Space Mono',monospace;font-size:12px;"
             oninput="markEnvDirty()" data-key-input>
      <div style="flex:1;position:relative;">
        <input class="form-input mono-input" value="${{v}}" placeholder="value" type="password"
               style="width:100%;font-family:'Space Mono',monospace;font-size:12px;padding-right:36px;"
               oninput="markEnvDirty()" data-val-input>
        <button type="button" onclick="toggleReveal(this)"
                style="position:absolute;right:8px;top:50%;transform:translateY(-50%);background:none;border:none;color:var(--muted);cursor:pointer;font-size:14px;padding:0;" title="Show/hide">👁</button>
      </div>
      <button type="button" onclick="removeEnvRow(this)"
              style="background:#ff3b3b22;border:1px solid #ff3b3b44;color:#ff6b6b;border-radius:6px;padding:6px 10px;cursor:pointer;font-size:13px;">✕</button>
    </div>
  `).join('');
}}

function toggleReveal(btn) {{
  const inp = btn.previousElementSibling;
  inp.type = inp.type === 'password' ? 'text' : 'password';
  btn.textContent = inp.type === 'password' ? '👁' : '🙈';
}}

function markEnvDirty() {{
  _dirty = true;
  document.getElementById('saveEnvBtn').style.display = 'inline-flex';
}}

function removeEnvRow(btn) {{
  btn.closest('[data-envrow]').remove();
  markEnvDirty();
}}

function addProjectEnvVar() {{
  const list = document.getElementById('envVarsList');
  // Clear "no vars" message if present
  if (list.querySelector(':not([data-envrow])')) list.innerHTML = '';
  const row = document.createElement('div');
  row.setAttribute('data-envrow', '');
  row.style.cssText = 'display:flex;gap:8px;margin-bottom:8px;align-items:center;';
  row.innerHTML = `
    <input class="form-input mono-input" placeholder="KEY"
           style="width:35%;font-family:'Space Mono',monospace;font-size:12px;"
           oninput="markEnvDirty()" data-key-input>
    <div style="flex:1;position:relative;">
      <input class="form-input mono-input" placeholder="value" type="password"
             style="width:100%;font-family:'Space Mono',monospace;font-size:12px;padding-right:36px;"
             oninput="markEnvDirty()" data-val-input>
      <button type="button" onclick="toggleReveal(this)"
              style="position:absolute;right:8px;top:50%;transform:translateY(-50%);background:none;border:none;color:var(--muted);cursor:pointer;font-size:14px;padding:0;">👁</button>
    </div>
    <button type="button" onclick="removeEnvRow(this)"
            style="background:#ff3b3b22;border:1px solid #ff3b3b44;color:#ff6b6b;border-radius:6px;padding:6px 10px;cursor:pointer;font-size:13px;">✕</button>
  `;
  list.appendChild(row);
  row.querySelector('[data-key-input]').focus();
  markEnvDirty();
}}

function collectEnvRows() {{
  const vars = {{}};
  document.querySelectorAll('[data-envrow]').forEach(row => {{
    const k = (row.querySelector('[data-key-input]').value || '').trim();
    const v = row.querySelector('[data-val-input]').value;
    if (k) vars[k] = v;
  }});
  return vars;
}}

async function saveEnvVars() {{
  const btn = document.getElementById('saveEnvBtn');
  const status = document.getElementById('envSaveStatus');
  btn.disabled = true;
  btn.textContent = 'Saving...';
  status.textContent = '';
  try {{
    const vars = collectEnvRows();
    const r = await fetch('/api/projects/' + projectId + '/env-vars', {{
      method: 'POST',
      headers: {{'Content-Type': 'application/json'}},
      body: JSON.stringify({{ env_vars: vars, restart: true }})
    }});
    const data = await r.json();
    if (r.ok) {{
      _currentEnvVars = vars;
      status.textContent = '✅ Saved! App restarting…';
      status.style.color = '#00ff88';
      btn.style.display = 'none';
      _dirty = false;
      setTimeout(() => {{ status.textContent = ''; }}, 4000);
    }} else {{
      status.textContent = '❌ ' + (data.detail || 'Save failed');
      status.style.color = '#ff6b6b';
    }}
  }} catch(e) {{
    status.textContent = '❌ ' + e.message;
    status.style.color = '#ff6b6b';
  }}
  btn.disabled = false;
  btn.textContent = '💾 Save & Restart';
}}

// ── Detect env vars from code ────────────────────────────────────────────────
async function scanAndDetectEnvVars() {{
  const btn = event.target;
  btn.disabled = true;
  btn.textContent = '🔍 Scanning...';
  try {{
    const r = await fetch('/api/projects/' + projectId + '/scan-env');
    const data = await r.json();
    const allKeys = data.keys || [];
    const existing = data.existing || {{}};
    _currentEnvVars = existing;
    renderEnvVars();

    // Find missing = referenced in code but not set
    const missing = allKeys.filter(k => !(k in existing) || existing[k] === '');

    if (!missing.length) {{
      btn.textContent = '✅ All set!';
      setTimeout(() => {{ btn.disabled = false; btn.textContent = '🔍 Detect from Code'; }}, 2000);
      return;
    }}

    // Show detected box
    const box  = document.getElementById('detectedEnvBox');
    const list = document.getElementById('detectedVarsList');
    list.innerHTML = missing.map(k => `
      <div style="display:flex;gap:8px;margin-bottom:8px;align-items:center;" data-detected-row data-key="${{k}}">
        <div style="width:38%;font-family:'Space Mono',monospace;font-size:12px;color:#c084fc;font-weight:700;
                    background:#ffffff08;padding:8px 10px;border-radius:6px;border:1px solid #7c3aed33;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;"
             title="${{k}}">${{k}}</div>
        <div style="flex:1;position:relative;">
          <input class="form-input" type="password" placeholder="Enter value…"
                 style="width:100%;font-family:'Space Mono',monospace;font-size:12px;
                        border-color:#7c3aed55;padding-right:36px;"
                 data-detected-val>
          <button type="button" onclick="toggleReveal(this)"
                  style="position:absolute;right:8px;top:50%;transform:translateY(-50%);background:none;border:none;color:var(--muted);cursor:pointer;font-size:14px;padding:0;">👁</button>
        </div>
      </div>
    `).join('');
    box.style.display = 'block';
    box.scrollIntoView({{behavior:'smooth', block:'nearest'}});
    document.getElementById('applyStatus').textContent = '';
  }} catch(e) {{
    alert('Scan failed: ' + e.message);
  }}
  btn.disabled = false;
  btn.textContent = '🔍 Detect from Code';
}}

async function applyDetectedVars() {{
  const btn   = document.getElementById('applyDetectedBtn');
  const status = document.getElementById('applyStatus');
  btn.disabled = true;
  btn.textContent = 'Applying...';
  status.textContent = '';

  const newVars = {{}};
  let anyFilled = false;
  document.querySelectorAll('[data-detected-row]').forEach(row => {{
    const k = row.getAttribute('data-key');
    const v = row.querySelector('[data-detected-val]').value;
    if (v.trim()) {{ newVars[k] = v; anyFilled = true; }}
  }});

  if (!anyFilled) {{
    status.textContent = '⚠️ Fill in at least one value';
    status.style.color = '#facc15';
    btn.disabled = false;
    btn.textContent = '✅ Apply & Restart';
    return;
  }}

  try {{
    const r = await fetch('/api/projects/' + projectId + '/env-vars', {{
      method: 'POST',
      headers: {{'Content-Type': 'application/json'}},
      body: JSON.stringify({{ env_vars: newVars, restart: true }})
    }});
    const data = await r.json();
    if (r.ok) {{
      // Merge into current display
      Object.assign(_currentEnvVars, newVars);
      renderEnvVars();
      status.textContent = `✅ ${{Object.keys(newVars).length}} var(s) saved — app restarting…`;
      status.style.color = '#00ff88';
      // Hide box after 3s
      setTimeout(() => {{
        document.getElementById('detectedEnvBox').style.display = 'none';
      }}, 3000);
    }} else {{
      status.textContent = '❌ ' + (data.detail || 'Failed');
      status.style.color = '#ff6b6b';
    }}
  }} catch(e) {{
    status.textContent = '❌ ' + e.message;
    status.style.color = '#ff6b6b';
  }}
  btn.disabled = false;
  btn.textContent = '✅ Apply & Restart';
}}

// ── Warn on unsaved changes ──────────────────────────────────────────────────
window.addEventListener('beforeunload', e => {{
  if (_dirty) {{ e.preventDefault(); e.returnValue = ''; }}
}});

// ── Init ─────────────────────────────────────────────────────────────────────
renderEnvVars();

// Auto-scan for missing env vars on page load (only if project has deploy_dir)
(async () => {{
  try {{
    const r = await fetch('/api/projects/' + projectId + '/scan-env');
    const data = await r.json();
    const allKeys = data.keys || [];
    const existing = data.existing || {{}};
    const missing = allKeys.filter(k => !(k in existing) || existing[k] === '');
    if (missing.length) {{
      // Show a gentle badge on the detect button
      const btn = document.querySelector('button[onclick="scanAndDetectEnvVars()"]');
      if (btn) {{
        btn.innerHTML = `🔍 Detect from Code <span style="background:#f472b6;color:#fff;border-radius:10px;padding:1px 7px;font-size:11px;font-weight:700;margin-left:4px;">${{missing.length}} missing</span>`;
      }}
    }}
  }} catch(e) {{}}
}})();

async function restartDeployment(deploymentId) {{
  if (!confirm('Restart this deployment?')) return;
  const r = await fetch('/api/deployments/' + deploymentId + '/restart', {{method: 'POST'}});
  if (r.ok) {{ alert('Restarting...'); setTimeout(() => location.reload(), 2000); }}
  else alert('Failed: ' + (await r.text()));
}}

async function restartLatest() {{
  const deployments = {json.dumps(deployments[:1])};
  if (!deployments.length) return alert('No deployments to restart.');
  await restartDeployment(deployments[0].id);
}}

async function stopProject() {{
  if (!confirm('Stop this project?')) return;
  const r = await fetch('/api/projects/' + projectId + '/stop', {{method: 'POST'}});
  if (r.ok) location.reload();
  else alert('Failed: ' + (await r.text()));
}}
</script>"""
    return _base_html(f"Project: {name}", body)


def _detail_row(label: str, value: str) -> str:
    return f"""
    <div style="display:flex;justify-content:space-between;align-items:center;">
      <span style="font-size:13px;color:var(--muted);">{label}</span>
      <span style="font-size:13px;">{value}</span>
    </div>"""


def render_logs_page(user: Dict, deployment: Dict, logs: List[Dict], project: Dict) -> str:
    project_name = project.get("name", "Unknown")
    deployment_id = deployment.get("id", "")
    ds = deployment.get("status", "unknown")
    project_id = deployment.get("project_id", "")

    logs_html = ""
    for log in logs:
        level = log.get("level", "info")
        source = log.get("source", "system")
        msg = log.get("message", "").replace("<", "&lt;").replace(">", "&gt;")
        ts = log.get("created_at", "")[:19].replace("T", " ")

        css_class = "log-error" if level == "error" else \
                    "log-warning" if level == "warning" else \
                    "log-build" if source in ("build", "pip") else \
                    "log-success" if "success" in msg.lower() or "running" in msg.lower() or "🚀" in msg else \
                    "log-info"

        logs_html += f'<div class="log-line {css_class}"><span style="color:var(--muted);user-select:none;">{ts}  </span>{msg}</div>\n'

    body = f"""
{_nav_html(user)}
<div style="max-width: 1100px; margin: 0 auto; padding: 40px 24px;">
  <div class="animate-in">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:24px;">
      <div>
        <a href="/project/{project_id}" style="color:var(--muted);text-decoration:none;font-size:14px;">← {project_name}</a>
        <div style="display:flex;align-items:center;gap:12px;margin-top:8px;">
          <h1 style="margin:0;font-size:24px;font-weight:800;">Deployment Logs</h1>
          <span class="status-badge status-{ds}">
            <span class="dot dot-{ds}"></span> {ds.upper()}
          </span>
        </div>
        <div class="mono" style="font-size:12px;color:var(--muted);margin-top:4px;">{deployment_id}</div>
      </div>
      <div style="display:flex;gap:8px;flex-wrap:wrap;">
        <button onclick="downloadLogs()" class="btn-secondary" style="padding:9px 16px;border-radius:7px;font-size:13px;">↓ Download</button>
        <button onclick="toggleAutoRefresh()" id="refreshBtn" class="btn-secondary" style="padding:9px 16px;border-radius:7px;font-size:13px;">⟳ Live</button>
        <button onclick="triggerAiFix()" id="aiFixBtn"
          style="padding:9px 16px;border-radius:7px;font-size:13px;background:linear-gradient(135deg,#7c3aed,#c084fc);color:#fff;border:none;cursor:pointer;font-weight:700;">
          🤖 AI Fix
        </button>
      </div>
    </div>
    
    <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px;" id="filterBtns">
      <button class="btn-secondary filter-btn active-filter" data-filter="all" onclick="filterLogs('all')" style="padding:5px 14px;border-radius:5px;font-size:12px;">All</button>
      <button class="btn-secondary filter-btn" data-filter="error" onclick="filterLogs('error')" style="padding:5px 14px;border-radius:5px;font-size:12px;color:#ff6b6b;">Errors</button>
      <button class="btn-secondary filter-btn" data-filter="build" onclick="filterLogs('build')" style="padding:5px 14px;border-radius:5px;font-size:12px;color:var(--brand2);">Build</button>
      <button class="btn-secondary filter-btn" data-filter="stdout" onclick="filterLogs('stdout')" style="padding:5px 14px;border-radius:5px;font-size:12px;">Stdout</button>
      <button class="btn-secondary filter-btn" data-filter="ai" onclick="filterLogs('ai')" style="padding:5px 14px;border-radius:5px;font-size:12px;color:#c084fc;">🤖 AI</button>
    </div>
    
    <div class="log-container" id="logContainer">
      {logs_html if logs_html else '<div class="log-line log-info">No logs yet. Deployment may still be starting...</div>'}
    </div>
    
    <div style="margin-top:12px;display:flex;justify-content:space-between;color:var(--muted);font-size:12px;font-family:'Space Mono',monospace;">
      <span id="logCount">{len(logs)} lines</span>
      <span id="lastUpdate">Last updated: just now</span>
    </div>
  </div>
</div>

<script>
const deploymentId = '{deployment_id}';
let autoRefresh = false;
let refreshInterval = null;
let allLogs = {json.dumps(logs)};
let currentFilter = 'all';

function filterLogs(filter) {{
  currentFilter = filter;
  document.querySelectorAll('.filter-btn').forEach(b => {{
    b.style.background = b.dataset.filter === filter ? 'var(--card)' : '';
    b.style.borderColor = b.dataset.filter === filter ? 'var(--muted)' : '';
  }});
  renderLogs(allLogs);
}}

function logCls(l) {{
  if (l.source === 'ai') return 'log-ai';
  if (l.level === 'error') return 'log-error';
  if (l.level === 'warning') return 'log-warning';
  if (l.source === 'build' || l.source === 'pip') return 'log-build';
  if (l.source === 'stderr') return 'log-error';
  if ((l.message||'').match(/🚀|✅|success/i)) return 'log-success';
  return 'log-info';
}}

function renderLogs(logs) {{
  const container = document.getElementById('logContainer');
  const filtered = currentFilter === 'all' ? logs :
    logs.filter(l => l.source === currentFilter || l.level === currentFilter);
  let html = '';
  filtered.forEach(l => {{
    const cls = logCls(l);
    const ts = (l.created_at||'').slice(0,19).replace('T',' ');
    const msg = (l.message||'').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    html += `<div class="log-line ${{cls}}"><span style="color:var(--muted);user-select:none;font-size:11px;">${{ts}}  </span>${{msg}}</div>`;
  }});
  container.innerHTML = html || '<div class="log-line log-info">No logs for this filter.</div>';
  container.scrollTop = container.scrollHeight;
  document.getElementById('logCount').textContent = filtered.length + ' lines';
}}
async function refreshLogs() {{
  try {{
    const r = await fetch('/api/deployments/' + deploymentId + '/logs');
    allLogs = await r.json();
    renderLogs(allLogs);
    document.getElementById('lastUpdate').textContent = 'Last updated: ' + new Date().toLocaleTimeString();
  }} catch(e) {{}}
}}

function toggleAutoRefresh() {{
  autoRefresh = !autoRefresh;
  const btn = document.getElementById('refreshBtn');
  if (autoRefresh) {{
    btn.textContent = '⟳ Auto-refresh: ON';
    btn.style.color = 'var(--brand)';
    refreshInterval = setInterval(refreshLogs, 3000);
  }} else {{
    btn.textContent = '⟳ Auto-refresh: OFF';
    btn.style.color = '';
    clearInterval(refreshInterval);
  }}
}}

function downloadLogs() {{
  const text = allLogs.map(l => `${{(l.created_at||'').slice(0,19)}} [${{l.level}}] ${{l.message}}`).join('\\n');
  const a = document.createElement('a');
  a.href = 'data:text/plain,' + encodeURIComponent(text);
  a.download = 'deployment-' + deploymentId.slice(0,8) + '.log';
  a.click();
}}

// Auto-scroll on load + auto-start live refresh
document.getElementById('logContainer').scrollTop = document.getElementById('logContainer').scrollHeight;
toggleAutoRefresh();

async function triggerAiFix() {{
  const btn = document.getElementById('aiFixBtn');
  btn.textContent = '🤖 Starting...';
  btn.disabled = true;
  try {{
    const r = await fetch('/api/deployments/' + deploymentId + '/ai-fix', {{method:'POST'}});
    const data = await r.json();
    if (r.ok) {{
      btn.textContent = '🤖 AI Running…';
    }} else {{
      alert('AI Fix: ' + (data.detail || JSON.stringify(data)));
      btn.textContent = '🤖 AI Fix'; btn.disabled = false;
    }}
  }} catch(e) {{
    alert('Error: ' + e); btn.textContent = '🤖 AI Fix'; btn.disabled = false;
  }}
}}
</script>"""
    return _base_html(f"Logs — {project_name}", body)


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 13: FASTAPI APPLICATION & LIFESPAN
# ──────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown tasks."""
    logger.info("PyDeploy starting up...")

    # Ensure deploy directory exists
    os.makedirs(BASE_DEPLOY_DIR, exist_ok=True)

    # Pre-seed the used-ports set so PyDeploy's own port is never handed out
    _used_ports.add(PORT)

    # Try to init DB tables (silently fail if Supabase isn't configured yet)
    try:
        await db_init_tables()
        logger.info("Supabase DB tables initialized.")
    except Exception as e:
        logger.warning(f"DB init skipped: {e}")

    # Ensure storage bucket exists
    try:
        await storage_ensure_bucket()
        logger.info("Storage bucket ready.")
    except Exception as e:
        logger.warning(f"Storage init skipped: {e}")

    logger.info(f"PyDeploy ready on port {PORT}")
    yield

    # Shutdown: stop all running processes
    logger.info("Shutting down — stopping all running deployments...")
    for dep_id in list(_running_processes.keys()):
        try:
            await stop_deployment_process(dep_id)
        except Exception:
            pass
    logger.info("PyDeploy shutdown complete.")


app = FastAPI(
    title="PyDeploy",
    description="Mini Python deployment platform",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 14: WEB UI ROUTES
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """Redirect root to dashboard or login."""
    user = await get_current_user(request)
    if user:
        return RedirectResponse(url="/dashboard", status_code=302)
    return RedirectResponse(url="/login", status_code=302)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = ""):
    user = await get_current_user(request)
    if user:
        return RedirectResponse(url="/dashboard", status_code=302)
    return HTMLResponse(render_login_page(error=error))


@app.post("/login")
async def login_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
):
    try:
        user = await db_get_user_by_email(email.lower().strip())
        if not user or not verify_password(password, user["password"]):
            return HTMLResponse(render_login_page(error="Invalid email or password."))

        token = await db_create_session(user["id"])
        response = RedirectResponse(url="/dashboard", status_code=302)
        response.set_cookie(
            key="session_token",
            value=token,
            httponly=True,
            max_age=7 * 24 * 3600,
            samesite="lax",
        )
        return response

    except Exception as e:
        logger.error(f"Login error: {e}")
        return HTMLResponse(render_login_page(error="Login service unavailable. Check Supabase config."))


@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    return HTMLResponse(render_register_page())


@app.post("/register")
async def register_submit(
    request: Request,
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
):
    try:
        email = email.lower().strip()
        existing = await db_get_user_by_email(email)
        if existing:
            return HTMLResponse(render_register_page(error="Email already registered."))

        if len(password) < 8:
            return HTMLResponse(render_register_page(error="Password must be at least 8 characters."))

        pw_hash = hash_password(password)
        user = await db_create_user(email, pw_hash, username.strip())

        token = await db_create_session(user["id"])
        response = RedirectResponse(url="/dashboard", status_code=302)
        response.set_cookie(
            key="session_token",
            value=token,
            httponly=True,
            max_age=7 * 24 * 3600,
            samesite="lax",
        )
        return response

    except Exception as e:
        logger.error(f"Register error: {e}")
        return HTMLResponse(render_register_page(error=f"Registration failed: {str(e)}"))


@app.get("/logout")
async def logout(request: Request):
    token = request.cookies.get("session_token")
    if token:
        await db_delete_session(token)
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie("session_token")
    return response


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)

    try:
        projects = await db_list_projects(user["id"])
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        projects = []

    return HTMLResponse(render_dashboard_page(user, projects))


@app.get("/upload", response_class=HTMLResponse)
async def upload_page(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    return HTMLResponse(render_upload_page(user))


@app.get("/project/{project_id}", response_class=HTMLResponse)
async def project_page(request: Request, project_id: str):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)

    project = await db_get_project(project_id)
    if not project or project.get("user_id") != user["id"]:
        raise HTTPException(status_code=404, detail="Project not found.")

    deployments = await db_list_deployments(project_id)
    return HTMLResponse(render_project_page(user, project, deployments))


@app.get("/logs/{deployment_id}", response_class=HTMLResponse)
async def logs_page(request: Request, deployment_id: str):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)

    deployment = await db_get_deployment(deployment_id)
    if not deployment:
        raise HTTPException(status_code=404, detail="Deployment not found.")

    project = await db_get_project(deployment["project_id"])
    if not project or project.get("user_id") != user["id"]:
        raise HTTPException(status_code=403, detail="Access denied.")

    logs = await db_get_logs(deployment_id, limit=500)
    return HTMLResponse(render_logs_page(user, deployment, logs, project))


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 15: API ROUTES
# ──────────────────────────────────────────────────────────────────────────────

@app.post("/api/deploy")
@app.post("/api/scan-zip-env")
async def api_scan_zip_env(
    request: Request,
    file: UploadFile = File(...),
):
    """
    Scan an uploaded ZIP for environment variable references WITHOUT deploying.
    Used by the upload page to show the env vars modal before deployment.
    """
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401)

    try:
        zip_bytes = await file.read()
        import tempfile, zipfile as _zipfile

        with tempfile.TemporaryDirectory() as tmpdir:
            with _zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                # Only extract .py and .env* files — fast and safe
                for member in zf.namelist():
                    name_lower = os.path.basename(member).lower()
                    if (name_lower.endswith(".py") or
                        name_lower.startswith(".env") or
                        name_lower in ("requirements.txt","pyproject.toml")):
                        # Guard against path traversal
                        dest = os.path.normpath(os.path.join(tmpdir, member))
                        if dest.startswith(tmpdir):
                            try:
                                os.makedirs(os.path.dirname(dest), exist_ok=True)
                                with zf.open(member) as src, open(dest, "wb") as dst:
                                    dst.write(src.read(500_000))  # max 500KB per file
                            except Exception:
                                pass

            keys = scan_project_env_vars(tmpdir)
    except Exception as exc:
        logger.warning(f"scan-zip-env error: {exc}")
        keys = []

    return JSONResponse({"keys": keys})


async def api_deploy(
    request: Request,
    background_tasks: BackgroundTasks,
    name: str = Form(...),
    description: str = Form(""),
    file: UploadFile = File(...),
    env_vars: str = Form("{}"),
    project_type: str = Form("python"),
):
    """Upload a ZIP and trigger deployment pipeline."""
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated.")

    # Validate file type
    if not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only .zip files are accepted.")

    # Read file
    zip_bytes = await file.read()

    # Validate ZIP
    validation = ZipValidator.validate(zip_bytes)
    if not validation["ok"]:
        raise HTTPException(status_code=400, detail=validation["error"])

    # Parse env vars
    try:
        env_vars_dict = json.loads(env_vars)
    except Exception:
        env_vars_dict = {}

    # Create project record
    project = await db_create_project(user["id"], name.strip(), description.strip())
    project_id = project["id"]

    # Upload ZIP to Supabase Storage (background, don't block deploy)
    try:
        storage_path = await storage_upload_zip(project_id, zip_bytes, file.filename)
        await db_update_project(project_id, {"zip_path": storage_path})
    except Exception as e:
        logger.warning(f"Storage upload failed (continuing anyway): {e}")

    # Save env vars to project
    await db_update_project(project_id, {"env_vars": env_vars_dict})

    # Kick off deployment in background
    background_tasks.add_task(
        run_deployment,
        project_id=project_id,
        user_id=user["id"],
        zip_bytes=zip_bytes,
        project_name=name,
        env_vars=env_vars_dict,
    )

    return JSONResponse({
        "project_id": project_id,
        "status": "deploying",
        "message": "Deployment started.",
    })


@app.get("/api/projects/{project_id}")
async def api_get_project(request: Request, project_id: str):
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated.")

    project = await db_get_project(project_id)
    if not project or project.get("user_id") != user["id"]:
        raise HTTPException(status_code=404, detail="Not found.")

    return JSONResponse(project)


# ── ENV VAR SCANNER ─────────────────────────────────────────────────────────

def scan_project_env_vars(project_dir: str) -> List[str]:
    """
    Scan all Python files + .env.example in the project directory and
    extract every referenced environment variable key.
    Detects: os.environ["KEY"], os.environ.get("KEY"), os.getenv("KEY"),
             config("KEY"), settings.KEY patterns, dotenv keys, etc.
    Returns deduplicated sorted list of key names.
    """
    import ast as _ast

    SKIP_DIRS = {".venv", "node_modules", "__pycache__", ".git", "dist", "build"}
    found: set = set()

    # ── Pattern-based regex scan on all .py files ────────────────────────
    # Covers os.environ["K"], os.environ.get("K"), os.getenv("K"),
    # getenv("K"), environ["K"], config("K"), settings("K"), Secret("K")
    env_patterns = [
        re.compile(r'os\.environ\s*\[\s*["\']([A-Z][A-Z0-9_]{1,60})["\']\s*\]'),
        re.compile(r'os\.environ\.get\s*\(\s*["\']([A-Z][A-Z0-9_]{1,60})["\']'),
        re.compile(r'os\.getenv\s*\(\s*["\']([A-Z][A-Z0-9_]{1,60})["\']'),
        re.compile(r'getenv\s*\(\s*["\']([A-Z][A-Z0-9_]{1,60})["\']'),
        re.compile(r'environ\s*\[\s*["\']([A-Z][A-Z0-9_]{1,60})["\']\s*\]'),
        re.compile(r'environ\.get\s*\(\s*["\']([A-Z][A-Z0-9_]{1,60})["\']'),
        re.compile(r'config\s*\(\s*["\']([A-Z][A-Z0-9_]{1,60})["\']'),
        re.compile(r'settings\s*\[\s*["\']([A-Z][A-Z0-9_]{1,60})["\']\s*\]'),
        re.compile(r'Secret\s*\(\s*["\']([A-Z][A-Z0-9_]{1,60})["\']'),
        re.compile(r'BaseSettings.*\n.*([A-Z][A-Z0-9_]{2,60})\s*:\s*str\s*='),
    ]

    # Keys that are definitely internal/platform — skip them
    SKIP_KEYS = {
        "PATH", "HOME", "USER", "SHELL", "LANG", "LC_ALL", "LC_CTYPE",
        "PYTHONDONTWRITEBYTECODE", "PYTHONUNBUFFERED", "VIRTUAL_ENV",
        "PORT", "HOST", "DEBUG", "ENVIRONMENT", "ENV", "NODE_ENV",
        "PWD", "TERM", "COLORTERM", "TMPDIR", "TEMP", "TMP",
    }

    for root, dirs, files in os.walk(project_dir):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS and not d.startswith(".")]
        for fname in files:
            fpath = os.path.join(root, fname)

            # .py files — regex scan
            if fname.endswith(".py"):
                try:
                    text = open(fpath, errors="replace").read()
                    for pat in env_patterns:
                        for m in pat.finditer(text):
                            key = m.group(1).strip()
                            if key and key not in SKIP_KEYS and len(key) >= 3:
                                found.add(key)
                except Exception:
                    pass

            # .env / .env.example / .env.sample / .env.template
            if fname in (".env", ".env.example", ".env.sample", ".env.template",
                         ".env.local", "example.env", "sample.env"):
                try:
                    for line in open(fpath, errors="replace"):
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            key = line.split("=", 1)[0].strip()
                            if key and key not in SKIP_KEYS and re.match(r'^[A-Z][A-Z0-9_]{1,60}$', key):
                                found.add(key)
                except Exception:
                    pass

    return sorted(found)


@app.get("/api/projects/{project_id}/scan-env")
async def api_scan_env(request: Request, project_id: str):
    """Scan the deployed project for all referenced env var keys."""
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401)

    project = await db_get_project(project_id)
    if not project or project.get("user_id") != user["id"]:
        raise HTTPException(status_code=404)

    project_dir = project.get("deploy_dir", "")
    if not project_dir or not os.path.isdir(project_dir):
        return JSONResponse({"keys": [], "existing": {}})

    keys = scan_project_env_vars(project_dir)
    existing = project.get("env_vars") or {}

    return JSONResponse({
        "keys": keys,
        "existing": {k: v for k, v in existing.items()},  # redact nothing — user owns it
    })


@app.post("/api/projects/{project_id}/env-vars")
async def api_update_env_vars(request: Request, project_id: str):
    """Update env vars for a project and restart the latest deployment."""
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401)

    project = await db_get_project(project_id)
    if not project or project.get("user_id") != user["id"]:
        raise HTTPException(status_code=404)

    body = await request.json()
    new_vars: Dict[str, str] = body.get("env_vars", {})

    # Merge with existing (new values override)
    existing = project.get("env_vars") or {}
    merged = {**existing, **{k: v for k, v in new_vars.items() if k and v != "__KEEP__"}}
    # Allow deleting: if value is empty string sent explicitly, keep it (user may want blank)

    await db_update_project(project_id, {"env_vars": merged})

    # Also update the running process env vars by restarting if running
    should_restart = body.get("restart", True)
    restarted = False
    if should_restart:
        # Find latest deployment
        deps = await db_list_deployments(project_id)
        if deps:
            latest = deps[0]
            dep_id = latest["id"]
            if latest.get("status") in (DeployState.RUNNING, DeployState.RESTARTING):
                # Restart in background
                asyncio.create_task(_restart_with_new_env(project_id, dep_id, merged))
                restarted = True

    return JSONResponse({"success": True, "merged_count": len(merged), "restarted": restarted})


async def _restart_with_new_env(project_id: str, deployment_id: str, env_vars: Dict[str, str]):
    """Background task: restart deployment with updated env vars."""
    try:
        project    = await db_get_project(project_id)
        deployment = await db_get_deployment(deployment_id)
        if not project or not deployment:
            return

        project_dir = project.get("deploy_dir", "")
        startup_cmd = project.get("startup_cmd") or deployment.get("startup_cmd", "")
        if not project_dir or not startup_cmd:
            return

        await stop_deployment_process(deployment_id)
        await asyncio.sleep(1)

        port = allocate_port()
        await db_update_deployment(deployment_id, {"port": port, "status": DeployState.RESTARTING})
        await db_update_project(project_id, {"port": port})

        pid = await start_app_process(
            project_dir=project_dir,
            startup_cmd=startup_cmd,
            port=port,
            deployment_id=deployment_id,
            project_id=project_id,
            env_vars=env_vars,
        )
        if pid:
            await db_update_deployment(deployment_id, {"status": DeployState.RUNNING, "pid": pid})
            await db_update_project(project_id, {"status": DeployState.RUNNING})
            await db_add_log(deployment_id, project_id,
                f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Restarted with updated env vars (PID {pid})",
                level="success", source="build")
        else:
            await db_update_deployment(deployment_id, {"status": DeployState.FAILED})
            await db_update_project(project_id, {"status": DeployState.FAILED})
    except Exception as exc:
        logger.error(f"_restart_with_new_env error: {exc}")


@app.delete("/api/projects/{project_id}")
async def api_delete_project(request: Request, project_id: str):
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated.")

    project = await db_get_project(project_id)
    if not project or project.get("user_id") != user["id"]:
        raise HTTPException(status_code=404, detail="Not found.")

    # Stop running process
    for dep_id, info in list(_running_processes.items()):
        dep = await db_get_deployment(dep_id)
        if dep and dep.get("project_id") == project_id:
            await stop_deployment_process(dep_id)

    # Delete deploy directory
    deploy_dir = project.get("deploy_dir")
    if deploy_dir:
        shutil.rmtree(deploy_dir, ignore_errors=True)

    # Delete from DB
    await db_delete_project(project_id)

    return JSONResponse({"success": True})


@app.post("/api/projects/{project_id}/stop")
async def api_stop_project(request: Request, project_id: str):
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated.")

    project = await db_get_project(project_id)
    if not project or project.get("user_id") != user["id"]:
        raise HTTPException(status_code=404, detail="Not found.")

    # Stop all running deployments for this project
    for dep_id, info in list(_running_processes.items()):
        dep = await db_get_deployment(dep_id)
        if dep and dep.get("project_id") == project_id:
            await stop_deployment_process(dep_id)
            await db_update_deployment(dep_id, {"status": DeployState.STOPPED})

    await db_update_project(project_id, {"status": DeployState.STOPPED})
    return JSONResponse({"success": True})


@app.get("/api/projects/{project_id}/logs")
async def api_project_logs(request: Request, project_id: str, limit: int = 200):
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated.")

    project = await db_get_project(project_id)
    if not project or project.get("user_id") != user["id"]:
        raise HTTPException(status_code=404, detail="Not found.")

    logs = await db_get_project_logs(project_id, limit=limit)
    return JSONResponse(logs)


@app.get("/api/deployments/{deployment_id}/logs")
async def api_deployment_logs(request: Request, deployment_id: str, limit: int = 300):
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated.")

    deployment = await db_get_deployment(deployment_id)
    if not deployment:
        raise HTTPException(status_code=404, detail="Not found.")

    project = await db_get_project(deployment["project_id"])
    if not project or project.get("user_id") != user["id"]:
        raise HTTPException(status_code=403, detail="Access denied.")

    logs = await db_get_logs(deployment_id, limit=limit)
    return JSONResponse(logs)


@app.get("/api/deployments/{deployment_id}/logs/stream")
async def api_logs_stream(request: Request, deployment_id: str, since: int = 0):
    """
    Server-Sent Events endpoint for real-time log streaming.
    Client sends ?since=N to get only new logs after N already seen.
    Streams: data: <json>\n\n  for each batch, heartbeat every 2s if no new logs.
    """
    user = await get_current_user(request)
    if not user:
        return Response(status_code=401)

    deployment = await db_get_deployment(deployment_id)
    if not deployment:
        return Response(status_code=404)
    project = await db_get_project(deployment["project_id"])
    if not project or project.get("user_id") != user["id"]:
        return Response(status_code=403)

    async def event_generator():
        offset = since          # how many logs the client already has
        idle_ticks = 0
        max_idle = 600          # stop after 10min of no activity (300 * 2s)

        while True:
            if await request.is_disconnected():
                break

            try:
                all_logs = await db_get_logs(deployment_id, limit=2000)
                new_logs = all_logs[offset:]
                if new_logs:
                    idle_ticks = 0
                    offset += len(new_logs)
                    payload = json.dumps({"logs": new_logs, "offset": offset})
                    yield f"data: {payload}\n\n"
                else:
                    idle_ticks += 1
                    # Heartbeat to keep connection alive
                    yield f": heartbeat {idle_ticks}\n\n"
                    if idle_ticks >= max_idle:
                        break
            except Exception as e:
                logger.warning(f"SSE log stream error: {e}")
                yield f": error\n\n"

            await asyncio.sleep(0.8)   # poll every 800ms

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",     # disable nginx buffering
            "Connection": "keep-alive",
        },
    )


@app.post("/api/deployments/{deployment_id}/restart")
async def api_restart_deployment(
    request: Request,
    background_tasks: BackgroundTasks,
    deployment_id: str,
):
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated.")

    deployment = await db_get_deployment(deployment_id)
    if not deployment:
        raise HTTPException(status_code=404, detail="Deployment not found.")

    project = await db_get_project(deployment["project_id"])
    if not project or project.get("user_id") != user["id"]:
        raise HTTPException(status_code=403, detail="Access denied.")

    background_tasks.add_task(
        restart_deployment,
        project_id=project["id"],
        deployment_id=deployment_id,
    )

    return JSONResponse({"success": True, "message": "Restarting deployment..."})


@app.post("/api/deployments/{deployment_id}/ai-fix")
async def api_ai_fix(request: Request, background_tasks: BackgroundTasks, deployment_id: str):
    """Manually trigger the AI auto-fix agent for a deployment."""
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated.")

    deployment = await db_get_deployment(deployment_id)
    if not deployment:
        raise HTTPException(status_code=404, detail="Deployment not found.")

    project = await db_get_project(deployment["project_id"])
    if not project or project.get("user_id") != user["id"]:
        raise HTTPException(status_code=403, detail="Access denied.")

    if not GEMINI_API_KEY and not OPENROUTER_API_KEY:
        raise HTTPException(status_code=400, detail="No AI API key configured. Set GEMINI_API_KEY or OPENROUTER_API_KEY.")

    if _ai_fixing.get(deployment_id):
        return JSONResponse({"status": "already_running", "message": "AI fix already running — check the logs."})

    background_tasks.add_task(ai_auto_fix, deployment_id, deployment["project_id"], "manual")
    return JSONResponse({"status": "started", "message": "AI auto-fix agent started."})


@app.get("/api/health")
async def health_check():
    """Health check endpoint for Koyeb."""
    return JSONResponse({
        "status": "ok",
        "platform": "PyDeploy",
        "version": "1.0.0",
        "running_deployments": len(_running_processes),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


@app.get("/api/status")
async def platform_status(request: Request):
    """Returns platform stats for admin view."""
    return JSONResponse({
        "running": len(_running_processes),
        "base_port": BASE_APP_PORT,
        "max_concurrent": MAX_CONCURRENT_DEPLOYMENTS,
        "deploy_dir": BASE_DEPLOY_DIR,
    })


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 16: APP PROXY — /p/{project_id}[/{path}]
# ──────────────────────────────────────────────────────────────────────────────

def _proxy_not_running_html(project_id: str, status: str) -> str:
    """HTML page shown when the app is not reachable."""
    status_label = status.upper() if status else "UNKNOWN"
    status_css = (
        "status-building" if status in ("building", "pending", "restarting")
        else "status-failed" if status in ("failed", "stopped")
        else "status-pending"
    )
    hint = {
        "building":   "Your app is still being built. This page will refresh automatically.",
        "pending":    "Deployment is queued. Hang tight…",
        "restarting": "Deployment is restarting. Refreshing shortly…",
        "failed":     "The deployment failed. Check the logs for details.",
        "stopped":    "This app has been stopped. Restart it from the dashboard.",
    }.get(status, "The app is not currently running.")

    auto_refresh = "true" if status in ("building", "pending", "restarting") else "false"

    body = f"""
<div class="grid-dots" style="min-height:100vh;display:flex;align-items:center;justify-content:center;padding:40px 20px;">
  <div class="animate-in" style="text-align:center;max-width:480px;">
    <div class="logo-text" style="font-size:52px;margin-bottom:20px;">&#x25B6;</div>
    <h1 style="margin:0 0 12px;font-size:26px;font-weight:800;">App Not Reachable</h1>

    <span class="status-badge {status_css}" style="font-size:13px;margin-bottom:20px;display:inline-flex;">
      <span class="dot dot-{'building' if status in ('building','pending','restarting') else 'failed'}"></span>
      {status_label}
    </span>

    <p style="margin:20px 0 28px;color:var(--muted);font-size:14px;line-height:1.7;">{hint}</p>

    <div style="display:flex;gap:10px;justify-content:center;flex-wrap:wrap;">
      <a href="/project/{project_id}" class="btn-secondary"
         style="text-decoration:none;padding:10px 20px;border-radius:8px;font-size:13px;">
        📋 View Project
      </a>
      <button onclick="location.reload()" class="btn-primary"
              style="padding:10px 20px;border-radius:8px;font-size:13px;">
        ⟳ Retry
      </button>
    </div>

    <div id="countdown" style="margin-top:24px;font-size:12px;color:var(--muted);font-family:'Space Mono',monospace;"></div>
  </div>
</div>

<script>
const autoRefresh = {auto_refresh};
if (autoRefresh) {{
  let secs = 5;
  const el = document.getElementById('countdown');
  const tick = () => {{
    el.textContent = 'Auto-refreshing in ' + secs + 's…';
    if (--secs < 0) location.reload();
    else setTimeout(tick, 1000);
  }};
  tick();
}}
</script>"""
    return _base_html("App Preview", body)


async def _proxy_request(
    request: Request,
    target_url: str,
    project_id: str = "",
) -> Response:
    """
    Forward an HTTP request to *target_url* and return the response.
    - Rewrites Location headers so redirects stay inside /p/<project_id>/
    - Injects <base> tag into HTML so relative links resolve correctly
    """
    # Build forwarded headers (drop hop-by-hop)
    skip_headers = {
        "host", "connection", "keep-alive", "transfer-encoding",
        "te", "trailer", "upgrade", "proxy-authorization",
        "proxy-authenticate", "content-length",
    }
    fwd_headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in skip_headers
    }
    fwd_headers["x-forwarded-for"] = request.client.host if request.client else "unknown"
    fwd_headers["x-forwarded-proto"] = request.url.scheme
    fwd_headers["x-real-ip"] = request.client.host if request.client else "unknown"

    body = await request.body()

    try:
        async with httpx.AsyncClient(
            follow_redirects=False,
            timeout=httpx.Timeout(30.0),
        ) as client:
            resp = await client.request(
                method=request.method,
                url=target_url,
                headers=fwd_headers,
                content=body,
                # query string is already embedded in target_url
            )

        # Forward response headers (skip hop-by-hop)
        skip_resp = {"transfer-encoding", "connection", "keep-alive", "content-length", "content-encoding"}
        resp_headers = {
            k: v for k, v in resp.headers.items()
            if k.lower() not in skip_resp
        }

        content_type = resp.headers.get("content-type", "")
        resp_body = resp.content

        # ── Rewrite redirect Location to stay inside /p/<id>/ ────────────
        if project_id and resp.status_code in (301, 302, 303, 307, 308):
            loc = resp_headers.get("location", "")
            if loc and loc.startswith("/") and not loc.startswith(f"/p/{project_id}"):
                resp_headers["location"] = f"/p/{project_id}{loc}"

        # ── HTML: rewrite absolute paths so they go through the proxy ──
        # <base> tags only fix *relative* URLs; absolute paths like href="/about"
        # bypass the proxy entirely.  We rewrite them in the HTML body directly.
        if project_id and "text/html" in content_type:
            prefix = "/p/" + project_id
            try:
                html = resp_body.decode("utf-8", errors="replace")

                def _skip(path: str) -> bool:
                    # Don't rewrite already-proxied, protocol-relative, or scheme paths
                    return (
                        path.startswith("/p/" + project_id)
                        or path.startswith("//")
                        or (":" in path and path.index(":") < path.index("/") + 1)
                    )

                # ── Rewrite HTML attribute values: href, src, action etc. ──
                ATTR_RE = re.compile(
                    r'((?:href|src|action|data-src|data-url|data-href))'
                    r'=([\'"])'
                    r'(/[^\'">\s]*)'
                    r'(\2)'
                )
                def rewrite_attr(m: re.Match) -> str:
                    attr, q, path, q2 = m.group(1), m.group(2), m.group(3), m.group(4)
                    if _skip(path):
                        return m.group(0)
                    return attr + "=" + q + prefix + path + q2

                html = ATTR_RE.sub(rewrite_attr, html)

                # ── Rewrite JS string literals that are absolute paths ────
                # Catches fetch("/api"), axios.get("/path"), location.href = "/x"
                JS_RE = re.compile(r'([\'"])(/[^\'"\s>]{1,500})\1')
                def rewrite_js(m: re.Match) -> str:
                    q, path = m.group(1), m.group(2)
                    if _skip(path):
                        return m.group(0)
                    return q + prefix + path + q

                html = JS_RE.sub(rewrite_js, html)

                resp_body = html.encode("utf-8")
            except Exception:
                pass  # never crash the proxy over a rewrite failure

        return Response(
            content=resp_body,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )

    except (httpx.ConnectError, httpx.ConnectTimeout, ConnectionRefusedError):
        return None  # caller will show not-running page
    except Exception as e:
        logger.warning(f"Proxy error → {target_url}: {e}")
        return None


@app.get("/p/{project_id}", response_class=HTMLResponse)
@app.post("/p/{project_id}")
@app.put("/p/{project_id}")
@app.patch("/p/{project_id}")
@app.delete("/p/{project_id}")
async def proxy_app_root(request: Request, project_id: str):
    """Proxy the root path of a deployed app."""
    return await _proxy_app(request, project_id, "")


@app.api_route(
    "/p/{project_id}/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
)
async def proxy_app_path(request: Request, project_id: str, path: str):
    """Proxy any sub-path of a deployed app."""
    return await _proxy_app(request, project_id, path)


async def _proxy_app(request: Request, project_id: str, path: str):
    """
    Core proxy logic:
      1. Look up the project → get its port & status.
      2. If not running, show a friendly status page.
      3. Otherwise forward the request to http://localhost:{port}/{path}.
    """
    # ── Fetch project (no auth required — public proxy) ──────────────
    try:
        project = await db_get_project(project_id)
    except Exception:
        project = None

    if not project:
        raise HTTPException(status_code=404, detail="Project not found.")

    status = project.get("status", "unknown")
    port = project.get("port")

    # ── Not ready to proxy ────────────────────────────────────────────
    if status != DeployState.RUNNING or not port:
        return HTMLResponse(
            content=_proxy_not_running_html(project_id, status),
            status_code=503 if status in ("failed", "stopped") else 202,
        )

    # Safety: never proxy to our own port (would cause an infinite loop)
    if port == PORT:
        logger.error(f"Project {project_id} has port={port} which is PyDeploy's own port. Re-deploy to fix.")
        return HTMLResponse(
            content=_proxy_not_running_html(project_id, "failed"),
            status_code=503,
        )

    # ── Build target URL ──────────────────────────────────────────────
    clean_path = path.lstrip("/") if path else ""
    qs = str(request.url.query)
    target = f"http://127.0.0.1:{port}/{clean_path}"
    if qs:
        target += f"?{qs}"

    # ── Proxy the request ─────────────────────────────────────────────
    proxied = await _proxy_request(request, target, project_id=project_id)

    if proxied is None:
        # Process crashed or not listening yet
        return HTMLResponse(
            content=_proxy_not_running_html(project_id, "stopped"),
            status_code=503,
        )

    return proxied


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 17: ENTRYPOINT
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=PORT,
        reload=False,
        log_level="info",
        access_log=True,
        workers=1,  # Single worker to share in-memory state
    )
