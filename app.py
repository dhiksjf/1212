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
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
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
    sb = get_supabase_service()
    result = sb.table("users").insert({
        "email": email,
        "password": password_hash,
        "username": username,
    }).execute()
    return result.data[0] if result.data else {}


async def db_get_user_by_email(email: str) -> Optional[Dict]:
    sb = get_supabase_service()
    result = sb.table("users").select("*").eq("email", email).limit(1).execute()
    return result.data[0] if result.data else None


async def db_get_user_by_id(user_id: str) -> Optional[Dict]:
    sb = get_supabase_service()
    result = sb.table("users").select("*").eq("id", user_id).limit(1).execute()
    return result.data[0] if result.data else None


async def db_create_session(user_id: str) -> str:
    sb = get_supabase_service()
    token = uuid.uuid4().hex + uuid.uuid4().hex
    expires = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
    sb.table("sessions").insert({
        "user_id": user_id,
        "token": token,
        "expires_at": expires,
    }).execute()
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

    # 2. Cache miss — hit Supabase
    sb = get_supabase_service()
    result = (
        sb.table("sessions")
        .select("*, users(*)")
        .eq("token", token)
        .gt("expires_at", datetime.now(timezone.utc).isoformat())
        .limit(1)
        .execute()
    )
    session_data = result.data[0] if result.data else None

    # 3. Populate cache (even None, so we dont hammer Supabase for invalid tokens)
    _session_cache[token] = (session_data, time.monotonic())
    return session_data


async def db_delete_session(token: str):
    # Evict from cache immediately so logout takes effect right away
    _session_cache.pop(token, None)
    sb = get_supabase_service()
    sb.table("sessions").delete().eq("token", token).execute()


async def db_create_project(user_id: str, name: str, description: str = "") -> Dict:
    sb = get_supabase_service()
    result = sb.table("projects").insert({
        "user_id": user_id,
        "name": name,
        "description": description,
        "status": DeployState.PENDING,
    }).execute()
    return result.data[0] if result.data else {}


async def db_update_project(project_id: str, updates: Dict) -> Dict:
    sb = get_supabase_service()
    updates["updated_at"] = datetime.now(timezone.utc).isoformat()
    result = sb.table("projects").update(updates).eq("id", project_id).execute()
    return result.data[0] if result.data else {}


async def db_get_project(project_id: str) -> Optional[Dict]:
    sb = get_supabase_service()
    result = sb.table("projects").select("*").eq("id", project_id).limit(1).execute()
    return result.data[0] if result.data else None


async def db_list_projects(user_id: str) -> List[Dict]:
    sb = get_supabase_service()
    result = (
        sb.table("projects")
        .select("*")
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .execute()
    )
    return result.data or []


async def db_delete_project(project_id: str):
    sb = get_supabase_service()
    sb.table("projects").delete().eq("id", project_id).execute()


async def db_create_deployment(project_id: str, user_id: str, framework: str, startup_cmd: str) -> Dict:
    sb = get_supabase_service()
    result = sb.table("deployments").insert({
        "project_id": project_id,
        "user_id": user_id,
        "framework": framework,
        "startup_cmd": startup_cmd,
        "status": DeployState.PENDING,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }).execute()
    return result.data[0] if result.data else {}


async def db_update_deployment(deployment_id: str, updates: Dict) -> Dict:
    sb = get_supabase_service()
    result = sb.table("deployments").update(updates).eq("id", deployment_id).execute()
    return result.data[0] if result.data else {}


async def db_get_deployment(deployment_id: str) -> Optional[Dict]:
    sb = get_supabase_service()
    result = sb.table("deployments").select("*").eq("id", deployment_id).limit(1).execute()
    return result.data[0] if result.data else None


async def db_list_deployments(project_id: str) -> List[Dict]:
    sb = get_supabase_service()
    result = (
        sb.table("deployments")
        .select("*")
        .eq("project_id", project_id)
        .order("created_at", desc=True)
        .limit(20)
        .execute()
    )
    return result.data or []


async def db_add_log(deployment_id: str, project_id: str, message: str,
                     level: str = "info", source: str = "system"):
    """Insert a single log line into Supabase."""
    try:
        sb = get_supabase_service()
        sb.table("logs").insert({
            "deployment_id": deployment_id,
            "project_id": project_id,
            "level": level,
            "message": message[:4000],  # Truncate very long lines
            "source": source,
        }).execute()
    except Exception as e:
        logger.warning(f"Failed to write log to Supabase: {e}")


async def db_get_logs(deployment_id: str, limit: int = 200) -> List[Dict]:
    sb = get_supabase_service()
    result = (
        sb.table("logs")
        .select("*")
        .eq("deployment_id", deployment_id)
        .order("created_at", desc=False)
        .limit(limit)
        .execute()
    )
    return result.data or []


async def db_get_project_logs(project_id: str, limit: int = 200) -> List[Dict]:
    sb = get_supabase_service()
    result = (
        sb.table("logs")
        .select("*")
        .eq("project_id", project_id)
        .order("created_at", desc=False)
        .limit(limit)
        .execute()
    )
    return result.data or []


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
    Analyzes a project directory and determines:
    - The backend framework (FastAPI, Flask, Django, etc.)
    - Whether there's a frontend (React, Next.js, static)
    - The recommended startup command
    - Entry point file
    """

    FRAMEWORK_SIGNATURES = {
        "fastapi": ["fastapi", "uvicorn"],
        "flask":   ["flask"],
        "django":  ["django", "djangorestframework"],
        "starlette": ["starlette"],
        "tornado": ["tornado"],
        "aiohttp": ["aiohttp"],
        "bottle":  ["bottle"],
        "falcon":  ["falcon"],
    }

    ENTRY_PATTERNS = [
        "main.py", "app.py", "server.py", "wsgi.py",
        "asgi.py", "run.py", "application.py",
    ]

    @classmethod
    def detect(cls, project_dir: str) -> Dict[str, Any]:
        """
        Returns a dict with keys:
          framework, entry_point, startup_cmd, has_frontend,
          frontend_type, has_requirements, requirements_path,
          has_package_json, package_json_path, python_version
        """
        result = {
            "framework": "unknown",
            "entry_point": None,
            "startup_cmd": None,
            "has_frontend": False,
            "frontend_type": None,
            "has_requirements": False,
            "requirements_path": None,
            "has_package_json": False,
            "package_json_path": None,
            "python_version": "3.11",
            "notes": [],
        }

        root = Path(project_dir)

        # ── Requirements.txt ──────────────────────────────────────────
        for req_file in ["requirements.txt", "requirements/base.txt",
                         "requirements/production.txt"]:
            req_path = root / req_file
            if req_path.exists():
                result["has_requirements"] = True
                result["requirements_path"] = str(req_path)
                break

        # ── Python version ────────────────────────────────────────────
        runtime_file = root / "runtime.txt"
        if runtime_file.exists():
            content = runtime_file.read_text().strip()
            match = re.search(r"python-(\d+\.\d+)", content, re.I)
            if match:
                result["python_version"] = match.group(1)

        # ── Detect framework from requirements ────────────────────────
        framework = "unknown"
        if result["has_requirements"]:
            req_content = Path(result["requirements_path"]).read_text().lower()
            for fw, sigs in cls.FRAMEWORK_SIGNATURES.items():
                if any(sig in req_content for sig in sigs):
                    framework = fw
                    break
        result["framework"] = framework

        # ── Find entry point ──────────────────────────────────────────
        entry = None
        # Check common patterns
        for pattern in cls.ENTRY_PATTERNS:
            candidate = root / pattern
            if candidate.exists():
                entry = str(candidate.relative_to(root))
                break

        # If not found at root, search one level deep
        if not entry:
            for candidate in root.glob("*/*.py"):
                if candidate.name in cls.ENTRY_PATTERNS:
                    entry = str(candidate.relative_to(root))
                    break

        result["entry_point"] = entry

        # ── Build startup command ─────────────────────────────────────
        startup_cmd = cls._build_startup_cmd(framework, entry, root)
        result["startup_cmd"] = startup_cmd

        # ── Frontend detection ────────────────────────────────────────
        for pkg_json in root.rglob("package.json"):
            # Skip node_modules
            if "node_modules" in str(pkg_json):
                continue
            result["has_package_json"] = True
            result["package_json_path"] = str(pkg_json)

            try:
                pkg_data = json.loads(pkg_json.read_text())
                deps = {
                    **pkg_data.get("dependencies", {}),
                    **pkg_data.get("devDependencies", {}),
                }

                if "next" in deps:
                    result["frontend_type"] = "nextjs"
                    result["has_frontend"] = True
                elif "react" in deps or "react-dom" in deps:
                    result["frontend_type"] = "react"
                    result["has_frontend"] = True
                elif "vue" in deps:
                    result["frontend_type"] = "vue"
                    result["has_frontend"] = True
            except Exception:
                pass
            break  # Only check first package.json

        # ── Static frontend ───────────────────────────────────────────
        for static_dir in ["static", "public", "frontend", "client", "dist", "build"]:
            if (root / static_dir).is_dir():
                if not result["has_frontend"]:
                    result["has_frontend"] = True
                    result["frontend_type"] = "static"
                break

        # ── Django-specific: detect wsgi/asgi ─────────────────────────
        if framework == "django":
            result = cls._handle_django(root, result)

        return result

    @classmethod
    def _build_startup_cmd(cls, framework: str, entry: Optional[str], root: Path) -> str:
        """Generate the appropriate startup command."""
        port_var = "${PORT:-8000}"

        if framework in ("fastapi", "starlette"):
            if entry:
                module = entry.replace("/", ".").replace(".py", "")
                # Try to find the app variable name
                app_var = cls._find_app_var(root / entry if entry else None) or "app"
                return f"uvicorn {module}:{app_var} --host 0.0.0.0 --port {port_var}"
            return f"uvicorn main:app --host 0.0.0.0 --port {port_var}"

        elif framework == "flask":
            if entry:
                module = entry.replace("/", ".").replace(".py", "")
                app_var = cls._find_app_var(root / entry if entry else None) or "app"
                return f"gunicorn {module}:{app_var} --bind 0.0.0.0:{port_var} --workers 2"
            return f"gunicorn app:app --bind 0.0.0.0:{port_var} --workers 2"

        elif framework == "django":
            # Will be refined in _handle_django
            return f"gunicorn wsgi:application --bind 0.0.0.0:{port_var} --workers 2"

        elif framework == "aiohttp":
            if entry:
                module = entry.replace("/", ".").replace(".py", "")
                return f"python -m {module}"
            return f"python main.py"

        else:
            # Generic Python runner
            if entry:
                return f"python {entry}"
            return "python main.py"

    @classmethod
    def _find_app_var(cls, entry_path: Optional[Path]) -> Optional[str]:
        """Scan a Python file for common WSGI/ASGI app variable names."""
        if not entry_path or not entry_path.exists():
            return None
        try:
            content = entry_path.read_text()
            for var in ["application", "app", "create_app", "APP"]:
                if re.search(rf"^{var}\s*=", content, re.MULTILINE):
                    return var
        except Exception:
            pass
        return None

    @classmethod
    def _handle_django(cls, root: Path, result: Dict) -> Dict:
        """Refine detection for Django projects."""
        # Find manage.py
        manage_files = list(root.glob("**/manage.py"))
        if not manage_files:
            return result

        django_root = manage_files[0].parent

        # Find wsgi.py
        wsgi_files = list(django_root.glob("**/wsgi.py"))
        if wsgi_files:
            wsgi = wsgi_files[0]
            module_parts = wsgi.relative_to(django_root).parts
            module = ".".join(module_parts).replace(".py", "")
            result["startup_cmd"] = (
                f"gunicorn {module}:application --bind 0.0.0.0:${{PORT:-8000}} --workers 2"
            )
        else:
            # Fallback: find settings module
            settings_files = list(django_root.glob("**/settings.py"))
            if settings_files:
                settings = settings_files[0]
                project_name = settings.parent.name
                result["startup_cmd"] = (
                    f"gunicorn {project_name}.wsgi:application "
                    f"--bind 0.0.0.0:${{PORT:-8000}} --workers 2"
                )

        return result


# ──────────────────────────────────────────────────────────────────────────────
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
    Handles installation of Python and Node.js dependencies,
    and frontend builds.
    """

    @classmethod
    async def run_command(
        cls,
        cmd: str,
        cwd: str,
        env: Optional[Dict] = None,
        timeout: int = 300,
    ) -> Dict[str, Any]:
        """
        Run a shell command asynchronously.
        Returns {"returncode": int, "stdout": str, "stderr": str}
        """
        try:
            proc_env = {**os.environ, **(env or {})}
            proc = await asyncio.create_subprocess_shell(
                cmd,
                cwd=cwd,
                env=proc_env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            return {
                "returncode": proc.returncode,
                "stdout": stdout.decode("utf-8", errors="replace"),
                "stderr": stderr.decode("utf-8", errors="replace"),
            }
        except asyncio.TimeoutError:
            return {"returncode": -1, "stdout": "", "stderr": "Command timed out."}
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
        """Install Python dependencies into the project virtualenv."""
        venv_dir = os.path.join(project_dir, ".venv")
        await db_add_log(deployment_id, project_id, "Creating virtual environment...", source="build")

        # Create venv
        result = await cls.run_command(
            f"python3 -m venv {venv_dir}",
            cwd=project_dir,
        )
        if result["returncode"] != 0:
            msg = f"Failed to create venv: {result['stderr']}"
            await db_add_log(deployment_id, project_id, msg, level="error", source="build")
            return False

        # Install pip packages
        pip_path = os.path.join(venv_dir, "bin", "pip")
        await db_add_log(deployment_id, project_id, "Installing Python dependencies...", source="build")
        result = await cls.run_command(
            f"{pip_path} install -r {requirements_path} --no-cache-dir",
            cwd=project_dir,
            timeout=600,
        )

        if result["stdout"]:
            for line in result["stdout"].splitlines()[-20:]:
                await db_add_log(deployment_id, project_id, line, source="pip")

        if result["returncode"] != 0:
            msg = f"pip install failed: {result['stderr'][:500]}"
            await db_add_log(deployment_id, project_id, msg, level="error", source="build")
            return False

        await db_add_log(deployment_id, project_id, "Python dependencies installed.", source="build")

        # Also install gunicorn for non-FastAPI apps
        await cls.run_command(
            f"{pip_path} install gunicorn uvicorn --no-cache-dir",
            cwd=project_dir,
            timeout=120,
        )

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
        """Build the frontend (React, Next.js, Vue, etc.)."""
        frontend_dir = str(Path(package_json_path).parent)

        await db_add_log(
            deployment_id, project_id,
            f"Installing Node.js dependencies ({frontend_type})...",
            source="build",
        )

        # Check for npm/yarn/pnpm
        pkg_manager = "npm"
        if (Path(frontend_dir) / "yarn.lock").exists():
            pkg_manager = "yarn"
        elif (Path(frontend_dir) / "pnpm-lock.yaml").exists():
            pkg_manager = "pnpm"

        # npm install
        result = await cls.run_command(
            f"{pkg_manager} install --legacy-peer-deps",
            cwd=frontend_dir,
            timeout=300,
        )

        if result["returncode"] != 0:
            msg = f"npm install failed: {result['stderr'][:500]}"
            await db_add_log(deployment_id, project_id, msg, level="error", source="build")
            return False

        # Build
        await db_add_log(deployment_id, project_id, f"Building {frontend_type} frontend...", source="build")

        build_cmd = "npm run build" if pkg_manager == "npm" else f"{pkg_manager} build"
        result = await cls.run_command(
            build_cmd,
            cwd=frontend_dir,
            timeout=600,
        )

        if result["returncode"] != 0:
            msg = f"Frontend build failed: {result['stderr'][:500]}"
            await db_add_log(deployment_id, project_id, msg, level="error", source="build")
            return False

        await db_add_log(deployment_id, project_id, "Frontend build complete.", source="build")
        return True


# ──────────────────────────────────────────────────────────────────────────────
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
    Start the user's application as a subprocess.
    Returns the PID, or None on failure.
    """
    venv_python = os.path.join(project_dir, ".venv", "bin")
    env = {
        **os.environ,
        "PORT": str(port),
        "HOST": "0.0.0.0",
        "PATH": f"{venv_python}:{os.environ.get('PATH', '')}",
        "VIRTUAL_ENV": os.path.join(project_dir, ".venv"),
        **(env_vars or {}),
    }

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


async def tail_log_to_db(
    log_path: str,
    deployment_id: str,
    project_id: str,
    proc: subprocess.Popen,
    interval: float = 3.0,
):
    """Background task: tail log file and push lines to Supabase."""
    position = 0
    batch_size = 20

    while True:
        await asyncio.sleep(interval)

        # Check if process died
        process_alive = proc.poll() is None

        try:
            if os.path.exists(log_path):
                async with aiofiles.open(log_path, "r") as f:
                    await f.seek(position)
                    new_content = await f.read()
                    position = await f.tell()

                if new_content.strip():
                    lines = new_content.strip().splitlines()
                    for i in range(0, len(lines), batch_size):
                        chunk = lines[i : i + batch_size]
                        for line in chunk:
                            if line.strip():
                                level = "error" if any(
                                    w in line.lower()
                                    for w in ["error", "exception", "traceback", "critical"]
                                ) else "info"
                                await db_add_log(
                                    deployment_id, project_id,
                                    line,
                                    level=level,
                                    source="stdout",
                                )
        except Exception as e:
            logger.warning(f"Log tail error: {e}")

        if not process_alive:
            await db_add_log(
                deployment_id, project_id,
                f"Process exited with code: {proc.returncode}",
                level="warning" if proc.returncode != 0 else "info",
                source="runtime",
            )
            # Update deployment status
            try:
                await db_update_deployment(deployment_id, {
                    "status": DeployState.STOPPED,
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                })
            except Exception:
                pass
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
        if detection["has_requirements"]:
            success = await BuildSystem.install_python_deps(
                deploy_dir,
                detection["requirements_path"],
                deployment_id,
                project_id,
            )
            if not success:
                raise RuntimeError("Python dependency installation failed.")
        else:
            await db_add_log(
                deployment_id, project_id,
                "No requirements.txt found. Skipping pip install.",
                level="warning",
                source="build",
            )

        # ── Step 5: Build frontend ─────────────────────────────────────
        if detection["has_frontend"] and detection["has_package_json"]:
            success = await BuildSystem.build_frontend(
                deploy_dir,
                detection["frontend_type"],
                detection["package_json_path"],
                deployment_id,
                project_id,
            )
            if not success:
                await db_add_log(
                    deployment_id, project_id,
                    "Frontend build failed. Continuing with backend only.",
                    level="warning",
                    source="build",
                )

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
            raise RuntimeError("Application process failed to start.")

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

    except Exception as e:
        error_msg = traceback.format_exc()
        logger.error(f"Deployment failed for project {project_id}: {error_msg}")

        await db_add_log(
            deployment_id, project_id,
            f"❌ Deployment failed: {str(e)}",
            level="error",
            source="system",
        )
        await db_update_deployment(deployment_id, {
            "status": DeployState.FAILED,
            "error_msg": str(e)[:2000],
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })
        # Clear deploy_dir in DB so restart doesn't reference a deleted path
        await db_update_project(project_id, {
            "status": DeployState.FAILED,
            "deploy_dir": None,
        })

        # Cleanup on failure — only wipe the dir if process never started
        # (if it started but crashed, keep logs for debugging)
        try:
            shutil.rmtree(deploy_dir, ignore_errors=True)
        except Exception:
            pass


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
      <p style="margin:0; color:var(--muted); font-size:14px;">Upload a ZIP file containing your Python project. We'll handle the rest.</p>
    </div>
    
    {'<div style="background:#00ff8811;border:1px solid #00ff8833;border-radius:8px;padding:14px;margin-bottom:24px;color:#00ff88;font-size:13px;">✓ ' + success + '</div>' if success else ''}
    {'<div style="background:#ff3b3b22;border:1px solid #ff3b3b44;border-radius:8px;padding:14px;margin-bottom:24px;color:#ff6b6b;font-size:13px;">⚠ ' + error + '</div>' if error else ''}
    
    <form id="uploadForm" enctype="multipart/form-data">
      <div class="card" style="padding: 28px; margin-bottom: 20px;">
        <h3 style="margin:0 0 20px; font-size:15px; font-weight:700; color:var(--muted); text-transform:uppercase; letter-spacing:.8px;">Project Details</h3>
        
        <div style="margin-bottom: 20px;">
          <label class="form-label">Project Name *</label>
          <input class="form-input" type="text" name="name" id="projectName" placeholder="my-awesome-api" required>
        </div>
        <div style="margin-bottom: 0;">
          <label class="form-label">Description</label>
          <input class="form-input" type="text" name="description" placeholder="Brief description of your app">
        </div>
      </div>
      
      <div class="card" style="padding: 28px; margin-bottom: 20px;">
        <h3 style="margin:0 0 20px; font-size:15px; font-weight:700; color:var(--muted); text-transform:uppercase; letter-spacing:.8px;">Project ZIP</h3>
        
        <div class="upload-zone" id="dropzone" onclick="document.getElementById('zipInput').click()">
          <div id="dropzoneContent">
            <div style="font-size: 40px; margin-bottom: 16px; opacity: .5;">⬡</div>
            <div style="font-size: 16px; font-weight: 700; margin-bottom: 8px;">Drop your ZIP here</div>
            <div style="font-size: 13px; color: var(--muted);">or click to browse · Max {MAX_ZIP_SIZE_MB}MB</div>
          </div>
        </div>
        <input type="file" id="zipInput" name="file" accept=".zip" style="display:none;" onchange="handleFileSelect(this)">
      </div>
      
      <div class="card" style="padding: 28px; margin-bottom: 24px;">
        <h3 style="margin:0 0 16px; font-size:15px; font-weight:700; color:var(--muted); text-transform:uppercase; letter-spacing:.8px;">
          Environment Variables <span style="font-size:11px; font-weight:400; color:var(--muted);">(optional)</span>
        </h3>
        <div id="envVarsContainer"></div>
        <button type="button" class="btn-secondary" onclick="addEnvVar()" style="padding:8px 16px;border-radius:6px;font-size:13px;margin-top:8px;">
          + Add Variable
        </button>
      </div>
      
      <button class="btn-primary" type="button" onclick="submitDeployment()" id="deployBtn" 
              style="width:100%;padding:14px;border-radius:10px;font-size:16px;" disabled>
        🚀 Deploy Application
      </button>
    </form>
    
    <div id="progressSection" style="display:none; margin-top: 28px;">
      <div class="card" style="padding: 24px;">
        <div style="display:flex; align-items:center; gap:12px; margin-bottom:16px;">
          <div class="dot dot-building" style="width:12px;height:12px;"></div>
          <span style="font-weight:700;">Deploying...</span>
        </div>
        <div class="log-container" id="progressLog" style="height:300px;">
          <div class="log-line log-build">Uploading ZIP file...</div>
        </div>
      </div>
    </div>
    
    <div style="margin-top: 28px;" class="card" style="padding:20px;">
      <div style="padding:20px;">
        <h3 style="margin:0 0 12px; font-size:14px; font-weight:700; color:var(--muted); text-transform:uppercase; letter-spacing:.8px;">Supported Frameworks</h3>
        <div style="display:flex; flex-wrap:wrap; gap:8px;">
          {''.join(f'<span class="framework-chip">{fw}</span>' for fw in ["FastAPI", "Flask", "Django", "Starlette", "Tornado", "aiohttp", "+ React", "+ Next.js", "+ Static"])}
        </div>
      </div>
    </div>
  </div>
</div>

<script>
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

function addEnvVar() {{
  const c = document.getElementById('envVarsContainer');
  const row = document.createElement('div');
  row.style.cssText = 'display:flex;gap:8px;margin-bottom:8px;';
  row.innerHTML = `
    <input class="form-input" type="text" placeholder="KEY" style="width:40%">
    <input class="form-input" type="text" placeholder="value" style="flex:1">
    <button type="button" class="btn-danger" onclick="this.parentNode.remove()" style="padding:8px 12px;border-radius:6px;font-size:13px;">✕</button>
  `;
  c.appendChild(row);
}}

async function submitDeployment() {{
  const name = document.getElementById('projectName').value.trim();
  const file = zipInput.files[0];
  if (!name || !file) return;
  
  // Collect env vars
  const envVars = {{}};
  document.querySelectorAll('#envVarsContainer > div').forEach(row => {{
    const inputs = row.querySelectorAll('input');
    if (inputs[0].value.trim()) envVars[inputs[0].value.trim()] = inputs[1].value;
  }});
  
  const fd = new FormData();
  fd.append('name', name);
  fd.append('description', document.querySelector('input[name=description]').value);
  fd.append('file', file);
  fd.append('env_vars', JSON.stringify(envVars));
  
  deployBtn.disabled = true;
  deployBtn.textContent = 'Deploying...';
  document.getElementById('progressSection').style.display = 'block';
  
  const log = document.getElementById('progressLog');
  const addLog = (msg, cls='log-info') => {{
    const d = document.createElement('div');
    d.className = 'log-line ' + cls;
    d.textContent = new Date().toLocaleTimeString() + '  ' + msg;
    log.appendChild(d);
    log.scrollTop = log.scrollHeight;
  }};
  
  try {{
    addLog('Uploading ZIP...', 'log-build');
    const r = await fetch('/api/deploy', {{method: 'POST', body: fd}});
    const data = await r.json();
    
    if (!r.ok) throw new Error(data.detail || 'Upload failed');
    
    addLog('Project created! Starting build...', 'log-success');
    const projectId = data.project_id;
    
    // Poll for logs
    let lastCount = 0;
    const poll = async () => {{
      const lr = await fetch('/api/projects/' + projectId + '/logs');
      const logs = await lr.json();
      for (let i = lastCount; i < logs.length; i++) {{
        const l = logs[i];
        const cls = l.level === 'error' ? 'log-error' : (l.source === 'build' ? 'log-build' : (l.level === 'warning' ? 'log-warning' : 'log-info'));
        addLog(l.message, cls);
      }}
      lastCount = logs.length;
      
      // Check project status
      const pr = await fetch('/api/projects/' + projectId);
      const proj = await pr.json();
      if (proj.status === 'running') {{
        addLog('🎉 Deployment successful! Redirecting...', 'log-success');
        setTimeout(() => window.location.href = '/project/' + projectId, 1500);
      }} else if (proj.status === 'failed') {{
        addLog('❌ Deployment failed. See logs above.', 'log-error');
        deployBtn.disabled = false;
        deployBtn.textContent = '🚀 Deploy Application';
      }} else {{
        setTimeout(poll, 2000);
      }}
    }};
    
    setTimeout(poll, 2000);
    
  }} catch(e) {{
    addLog('Error: ' + e.message, 'log-error');
    deployBtn.disabled = false;
    deployBtn.textContent = '🚀 Deploy Application';
  }}
}}
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
      <div style="display:flex;gap:8px;">
        <button onclick="downloadLogs()" class="btn-secondary" style="padding:9px 16px;border-radius:7px;font-size:13px;">↓ Download</button>
        <button onclick="toggleAutoRefresh()" id="refreshBtn" class="btn-secondary" style="padding:9px 16px;border-radius:7px;font-size:13px;">⟳ Auto-refresh: OFF</button>
      </div>
    </div>
    
    <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px;" id="filterBtns">
      <button class="btn-secondary filter-btn active-filter" data-filter="all" onclick="filterLogs('all')" style="padding:5px 14px;border-radius:5px;font-size:12px;">All</button>
      <button class="btn-secondary filter-btn" data-filter="error" onclick="filterLogs('error')" style="padding:5px 14px;border-radius:5px;font-size:12px;color:#ff6b6b;">Errors</button>
      <button class="btn-secondary filter-btn" data-filter="build" onclick="filterLogs('build')" style="padding:5px 14px;border-radius:5px;font-size:12px;color:var(--brand2);">Build</button>
      <button class="btn-secondary filter-btn" data-filter="stdout" onclick="filterLogs('stdout')" style="padding:5px 14px;border-radius:5px;font-size:12px;">Stdout</button>
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

function renderLogs(logs) {{
  const container = document.getElementById('logContainer');
  const filtered = currentFilter === 'all' ? logs : logs.filter(l => l.source === currentFilter || l.level === currentFilter);
  let html = '';
  filtered.forEach(l => {{
    const cls = l.level === 'error' ? 'log-error' : l.level === 'warning' ? 'log-warning' : l.source === 'build' || l.source === 'pip' ? 'log-build' : 'log-info';
    const ts = (l.created_at||'').slice(0,19).replace('T',' ');
    const msg = (l.message||'').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    html += `<div class="log-line ${{cls}}"><span style="color:var(--muted);user-select:none">${{ts}}  </span>${{msg}}</div>`;
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

// Auto-scroll on load
document.getElementById('logContainer').scrollTop = document.getElementById('logContainer').scrollHeight;
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
async def api_deploy(
    request: Request,
    background_tasks: BackgroundTasks,
    name: str = Form(...),
    description: str = Form(""),
    file: UploadFile = File(...),
    env_vars: str = Form("{}"),
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
