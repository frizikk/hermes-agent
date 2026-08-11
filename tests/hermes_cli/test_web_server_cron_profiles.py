"""Regression tests for dashboard cron job profile routing."""

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
from queue import Empty, SimpleQueue
import threading

import pytest
from fastapi import HTTPException


@pytest.fixture()
def isolated_profiles(tmp_path, monkeypatch):
    """Give profile discovery an isolated default home with one named profile."""
    from hermes_cli import profiles

    default_home = tmp_path / ".hermes"
    profiles_root = default_home / "profiles"
    worker_home = profiles_root / "worker_alpha"

    for home in (default_home, worker_home):
        (home / "cron").mkdir(parents=True, exist_ok=True)
        (home / "config.yaml").write_text("model: test-model\n", encoding="utf-8")

    monkeypatch.setattr(profiles, "_get_default_hermes_home", lambda: default_home)
    monkeypatch.setattr(profiles, "_get_profiles_root", lambda: profiles_root)
    return {"default": default_home, "worker_alpha": worker_home}


def _drain_queue(q):
    values = []
    while True:
        try:
            values.append(q.get_nowait())
        except Empty:
            return values




def test_fire_cron_job_scopes_store_and_runtime_home_together(
    isolated_profiles,
    monkeypatch,
):
    """A profile fire must execute and persist under the same profile home."""
    from cron import jobs as cron_jobs
    from cron import scheduler
    from hermes_cli import web_server

    from hermes_constants import (
        reset_hermes_home_override,
        set_hermes_home_override,
    )

    default_home = isolated_profiles["default"]
    worker_home = isolated_profiles["worker_alpha"]
    monkeypatch.setattr(scheduler, "_hermes_home", None)
    captured = {}

    class RecordingProvider:
        def fire_due(self, job_id, *, adapters=None, loop=None):
            captured["job_id"] = job_id
            captured["runtime_home"] = scheduler._get_hermes_home()
            captured["jobs_file"] = cron_jobs._current_cron_store().jobs_file
            return True

    monkeypatch.setattr(
        "cron.scheduler_provider.resolve_cron_scheduler",
        lambda: RecordingProvider(),
    )

    outer_token = set_hermes_home_override(default_home)
    try:
        assert web_server._fire_cron_job_for_profile("worker_alpha", "worker-job") is True
        assert captured == {
            "job_id": "worker-job",
            "runtime_home": worker_home,
            "jobs_file": worker_home / "cron" / "jobs.json",
        }
        assert scheduler._get_hermes_home() == default_home
    finally:
        reset_hermes_home_override(outer_token)


def test_create_registers_scheduler_inside_target_profile(
    isolated_profiles,
    monkeypatch,
):
    """Dashboard create must resolve and register under the selected profile."""
    from cron import jobs as cron_jobs
    from cron.scheduler_provider import CronScheduler
    from hermes_cli import web_server
    from hermes_constants import get_hermes_home

    worker_home = isolated_profiles["worker_alpha"]
    captured = {}

    class RecordingProvider(CronScheduler):
        @property
        def name(self):
            return "recording"

        def start(self, stop_event, **kw):
            pass

        def register_job(self, job):
            captured["job"] = job
            captured["runtime_home"] = get_hermes_home()
            captured["jobs_file"] = cron_jobs._current_cron_store().jobs_file

    monkeypatch.setattr(
        "cron.scheduler_provider.resolve_cron_scheduler",
        lambda: RecordingProvider(),
    )

    job = web_server._call_cron_for_profile(
        "worker_alpha",
        "create_job",
        prompt="managed by named profile",
        schedule="every 1h",
        name="named-profile-job",
    )

    assert captured["job"]["id"] == job["id"]
    assert captured["runtime_home"] == worker_home
    assert captured["jobs_file"] == worker_home / "cron" / "jobs.json"
    assert job["profile"] == "worker_alpha"


def test_cron_run_outputs_are_read_from_the_owning_profile(
    isolated_profiles,
):
    """Desktop run detail must use the durable markdown output, not a chat row."""
    from hermes_cli import web_server

    worker_home = isolated_profiles["worker_alpha"]
    job = web_server._call_cron_for_profile(
        "worker_alpha",
        "create_job",
        prompt="produce a markdown report",
        schedule="every 1h",
        name="markdown-report",
    )
    assert isinstance(job, dict)
    output_dir = worker_home / "cron" / "output" / job["id"]
    output_dir.mkdir(parents=True)
    older = output_dir / "2026-08-10_09-00-00.md"
    newer = output_dir / "2026-08-11_09-00-00.md"
    older.write_text("# Older\n", encoding="utf-8")
    newer.write_text("# Report\n\n| Name | Value |\n| --- | --- |\n| ok | 1 |\n", encoding="utf-8")

    listed = web_server._list_cron_job_outputs_sync(job["id"], limit=1)

    assert listed["profile"] == "worker_alpha"
    assert listed["outputs"] == [
        {
            "id": "2026-08-11_09-00-00",
            "filename": newer.name,
            "byte_size": newer.stat().st_size,
            "created_at": newer.stat().st_mtime,
        }
    ]
    detail = web_server._get_cron_job_output_sync(
        job["id"], "2026-08-11_09-00-00"
    )
    assert detail["profile"] == "worker_alpha"
    assert detail["content"].startswith("# Report")
    assert "| ok | 1 |" in detail["content"]


def test_cron_run_output_rejects_path_escape(isolated_profiles):
    from hermes_cli import web_server

    with pytest.raises(HTTPException) as exc_info:
        web_server._get_cron_job_output_sync("missing", "../jobs")

    assert exc_info.value.status_code == 400


def test_cron_run_output_does_not_follow_symlinks(isolated_profiles, tmp_path):
    """The dashboard must not turn a planted output symlink into a file reader."""
    from hermes_cli import web_server

    worker_home = isolated_profiles["worker_alpha"]
    job = web_server._call_cron_for_profile(
        "worker_alpha",
        "create_job",
        prompt="produce a markdown report",
        schedule="every 1h",
        name="symlink-report",
    )
    assert isinstance(job, dict)
    output_dir = worker_home / "cron" / "output" / job["id"]
    output_dir.mkdir(parents=True)
    secret = tmp_path / "secret.md"
    secret.write_text("must not leak", encoding="utf-8")
    planted = output_dir / "2026-08-11_09-00-00.md"
    try:
        planted.symlink_to(secret)
    except OSError:
        pytest.skip("symlink creation is unavailable on this platform")

    assert web_server._list_cron_job_outputs_sync(job["id"])["outputs"] == []
    with pytest.raises(HTTPException) as exc_info:
        web_server._get_cron_job_output_sync(job["id"], planted.stem)

    assert exc_info.value.status_code == 404


def test_cron_run_output_rejects_symlink_swap_during_open(
    isolated_profiles,
    monkeypatch,
    tmp_path,
):
    """The descriptor read must stay pinned when the path changes after lstat."""
    from cron import jobs as cron_jobs
    from hermes_cli import web_server

    worker_home = isolated_profiles["worker_alpha"]
    job = web_server._call_cron_for_profile(
        "worker_alpha",
        "create_job",
        prompt="produce a markdown report",
        schedule="every 1h",
        name="race-report",
    )
    assert isinstance(job, dict)
    output_dir = worker_home / "cron" / "output" / job["id"]
    output_dir.mkdir(parents=True)
    planted = output_dir / "2026-08-11_09-00-00.md"
    planted.write_text("safe output", encoding="utf-8")
    secret = tmp_path / "secret.md"
    secret.write_text("must not leak", encoding="utf-8")

    real_open = cron_jobs.os.open
    swapped = False

    def swap_then_open(path, flags, *args, **kwargs):
        nonlocal swapped
        if not swapped and Path(path).name == planted.name:
            planted.unlink()
            try:
                planted.symlink_to(secret)
            except OSError:
                pytest.skip("symlink creation is unavailable on this platform")
            swapped = True
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(cron_jobs.os, "open", swap_then_open)

    with pytest.raises(HTTPException) as exc_info:
        web_server._get_cron_job_output_sync(
            job["id"], planted.stem, profile="worker_alpha"
        )

    assert exc_info.value.status_code == 404


def test_cron_run_output_rejects_symlinked_output_root(
    isolated_profiles,
    tmp_path,
):
    """A planted parent symlink must not move reads outside the profile store."""
    from hermes_cli import web_server

    worker_home = isolated_profiles["worker_alpha"]
    job = web_server._call_cron_for_profile(
        "worker_alpha",
        "create_job",
        prompt="produce a markdown report",
        schedule="every 1h",
        name="parent-symlink-report",
    )
    assert isinstance(job, dict)
    external_root = tmp_path / "outside"
    external_job_dir = external_root / job["id"]
    external_job_dir.mkdir(parents=True)
    planted = external_job_dir / "2026-08-11_09-00-00.md"
    planted.write_text("must not leak", encoding="utf-8")
    output_root = worker_home / "cron" / "output"
    if output_root.exists():
        output_root.rmdir()
    try:
        output_root.symlink_to(external_root, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation is unavailable on this platform")

    assert web_server._list_cron_job_outputs_sync(
        job["id"], profile="worker_alpha"
    )["outputs"] == []
    with pytest.raises(HTTPException) as exc_info:
        web_server._get_cron_job_output_sync(
            job["id"], planted.stem, profile="worker_alpha"
        )

    assert exc_info.value.status_code == 404


@pytest.mark.parametrize("force_path_fallback", [False, True])
def test_cron_run_output_rejects_symlinked_job_directory(
    isolated_profiles,
    monkeypatch,
    tmp_path,
    force_path_fallback,
):
    """Descriptor and cross-platform path fallback both reject a job-dir link."""
    from cron import jobs as cron_jobs
    from hermes_cli import web_server

    if force_path_fallback:
        monkeypatch.setattr(
            cron_jobs.os,
            "supports_dir_fd",
            frozenset(cron_jobs.os.supports_dir_fd - {cron_jobs.os.open}),
        )

    worker_home = isolated_profiles["worker_alpha"]
    job = web_server._call_cron_for_profile(
        "worker_alpha",
        "create_job",
        prompt="produce a markdown report",
        schedule="every 1h",
        name="job-dir-symlink-report",
    )
    assert isinstance(job, dict)
    external_job_dir = tmp_path / "outside-job"
    external_job_dir.mkdir()
    planted = external_job_dir / "2026-08-11_09-00-00.md"
    planted.write_text("must not leak", encoding="utf-8")
    output_root = worker_home / "cron" / "output"
    output_root.mkdir(exist_ok=True)
    job_dir = output_root / job["id"]
    try:
        job_dir.symlink_to(external_job_dir, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation is unavailable on this platform")

    assert web_server._list_cron_job_outputs_sync(
        job["id"], profile="worker_alpha"
    )["outputs"] == []
    with pytest.raises(HTTPException) as exc_info:
        web_server._get_cron_job_output_sync(
            job["id"], planted.stem, profile="worker_alpha"
        )

    assert exc_info.value.status_code == 404


def test_cron_run_output_listing_surfaces_storage_errors(
    isolated_profiles,
    monkeypatch,
):
    from cron import jobs as cron_jobs
    from hermes_cli import web_server

    worker_home = isolated_profiles["worker_alpha"]
    job = web_server._call_cron_for_profile(
        "worker_alpha",
        "create_job",
        prompt="produce a markdown report",
        schedule="every 1h",
        name="unreadable-report",
    )
    assert isinstance(job, dict)
    output_dir = worker_home / "cron" / "output" / job["id"]
    output_dir.mkdir(parents=True)
    real_scandir = cron_jobs.os.scandir

    def fail_output_scan(path):
        if isinstance(path, int) or Path(path) == output_dir:
            raise PermissionError("output directory unavailable")
        return real_scandir(path)

    monkeypatch.setattr(cron_jobs.os, "scandir", fail_output_scan)

    with pytest.raises(PermissionError, match="output directory unavailable"):
        web_server._list_cron_job_outputs_sync(
            job["id"], profile="worker_alpha"
        )


def test_cron_run_output_listing_surfaces_metadata_errors(
    isolated_profiles,
    monkeypatch,
):
    """A durable output metadata failure is not an empty-history response."""
    from cron import jobs as cron_jobs
    from hermes_cli import web_server

    worker_home = isolated_profiles["worker_alpha"]
    job = web_server._call_cron_for_profile(
        "worker_alpha",
        "create_job",
        prompt="produce a markdown report",
        schedule="every 1h",
        name="unreadable-output-metadata",
    )
    assert isinstance(job, dict)
    output_dir = worker_home / "cron" / "output" / job["id"]
    output_dir.mkdir(parents=True)

    class UnreadableOutputEntry:
        name = "2026-08-11_09-00-00.md"

        def is_file(self, *, follow_symlinks=True):
            return True

        def stat(self, *, follow_symlinks=True):
            raise PermissionError("output metadata unavailable")

    class OutputEntries:
        def __enter__(self):
            return iter([UnreadableOutputEntry()])

        def __exit__(self, exc_type, exc, traceback):
            return False

    monkeypatch.setattr(cron_jobs.os, "scandir", lambda _path: OutputEntries())

    with pytest.raises(PermissionError, match="output metadata unavailable"):
        web_server._list_cron_job_outputs_sync(
            job["id"], profile="worker_alpha"
        )


def test_dashboard_create_reports_saved_but_unregistered(
    isolated_profiles,
    monkeypatch,
):
    """Dashboard callers can distinguish persistence from remote registration."""
    from cron.scheduler import CronSchedulerRegistrationError
    from hermes_cli import web_server

    job = {"id": "saved-job", "name": "saved job"}
    failure = CronSchedulerRegistrationError(
        job,
        RuntimeError("private callback URL and token"),
    )

    def fail_create(*args, **kwargs):
        raise failure

    monkeypatch.setattr(web_server, "_call_cron_for_profile", fail_create)

    with pytest.raises(HTTPException) as exc_info:
        web_server._create_cron_job_sync(
            web_server.CronJobCreate(
                prompt="managed by named profile",
                schedule="every 1h",
                name="named-profile-job",
            ),
            profile="worker_alpha",
        )

    assert exc_info.value.status_code == 424
    assert exc_info.value.detail == {
        "error": str(failure),
        "job_id": "saved-job",
        "job_saved": True,
        "scheduler_registered": False,
        "retry_create": False,
    }
    assert "private callback URL and token" not in str(exc_info.value.detail)


def test_profile_call_cannot_retarget_ticker_store_mid_write(
    isolated_profiles,
    monkeypatch,
):
    """A dashboard profile call must not redirect a concurrent ticker save."""
    from cron import jobs as cron_jobs
    from hermes_cli import web_server

    default_cron = isolated_profiles["default"] / "cron"
    worker_cron = isolated_profiles["worker_alpha"] / "cron"
    default_file = default_cron / "jobs.json"
    worker_file = worker_cron / "jobs.json"
    default_job = {
        "id": "default-job",
        "name": "default job",
        "schedule": {"kind": "interval", "minutes": 60},
        "next_run_at": "2026-07-09T00:00:00+00:00",
    }
    worker_job = {
        "id": "worker-job",
        "name": "worker job",
        "schedule": {"kind": "interval", "minutes": 60},
        "next_run_at": "2026-07-09T00:00:00+00:00",
    }
    default_file.write_text(json.dumps({"jobs": [default_job]}), encoding="utf-8")
    worker_file.write_text(json.dumps({"jobs": [worker_job]}), encoding="utf-8")

    monkeypatch.setattr(cron_jobs, "CRON_DIR", default_cron)
    monkeypatch.setattr(cron_jobs, "JOBS_FILE", default_file)
    monkeypatch.setattr(cron_jobs, "OUTPUT_DIR", default_cron / "output")
    monkeypatch.setattr(
        cron_jobs,
        "compute_next_run",
        lambda _schedule, _last_run_at=None: "2026-07-10T00:00:00+00:00",
    )

    ticker_loaded = threading.Event()
    release_ticker = threading.Event()
    profile_entered = threading.Event()
    ticker_done = threading.Event()
    ticker_thread = threading.local()
    original_load_jobs = cron_jobs.load_jobs

    def blocking_load_jobs():
        loaded = original_load_jobs()
        if getattr(ticker_thread, "active", False):
            ticker_loaded.set()
            assert release_ticker.wait(5), "profile call did not enter in time"
        return loaded

    def hold_profile_call():
        profile_entered.set()
        assert ticker_done.wait(5), "ticker did not finish in time"
        return True

    def run_ticker_write():
        ticker_thread.active = True
        try:
            return cron_jobs.advance_next_run("default-job")
        finally:
            ticker_done.set()

    monkeypatch.setattr(cron_jobs, "load_jobs", blocking_load_jobs)
    monkeypatch.setattr(cron_jobs, "_hold_profile_call", hold_profile_call, raising=False)

    with ThreadPoolExecutor(max_workers=2) as pool:
        ticker_future = pool.submit(run_ticker_write)
        assert ticker_loaded.wait(5), "ticker did not load the default store"
        profile_future = pool.submit(
            web_server._call_cron_for_profile,
            "worker_alpha",
            "_hold_profile_call",
        )
        assert profile_entered.wait(5), "profile call did not retarget its store"
        release_ticker.set()
        assert ticker_future.result(timeout=5) is True
        assert profile_future.result(timeout=5) is True

    default_saved = json.loads(default_file.read_text(encoding="utf-8"))["jobs"]
    worker_saved = json.loads(worker_file.read_text(encoding="utf-8"))["jobs"]
    assert [job["id"] for job in worker_saved] == ["worker-job"]
    assert [job["id"] for job in default_saved] == ["default-job"]
    assert default_saved[0]["next_run_at"] == "2026-07-10T00:00:00+00:00"






@pytest.mark.asyncio
async def test_cron_mutation_without_profile_finds_named_profile_job(isolated_profiles):
    from hermes_cli import web_server

    worker_job = web_server._call_cron_for_profile(
        "worker_alpha",
        "create_job",
        prompt="managed by named profile",
        schedule="every 1h",
        name="named-profile-job",
    )

    paused = await web_server.pause_cron_job(worker_job["id"])
    assert paused["profile"] == "worker_alpha"
    assert paused["enabled"] is False

    default_jobs = await web_server.list_cron_jobs(profile="default")
    worker_jobs = await web_server.list_cron_jobs(profile="worker_alpha")

    assert default_jobs == []
    assert len(worker_jobs) == 1
    assert worker_jobs[0]["id"] == worker_job["id"]
    assert worker_jobs[0]["enabled"] is False




@pytest.mark.asyncio
async def test_dashboard_cron_rejects_missing_context_from(isolated_profiles):
    from hermes_cli import web_server

    with pytest.raises(HTTPException) as create_exc:
        await web_server.create_cron_job(
            web_server.CronJobCreate(
                prompt="process missing upstream",
                schedule="every 1h",
                context_from=["missing-job-id"],
            ),
            profile="worker_alpha",
        )

    assert create_exc.value.status_code == 400
    assert "missing-job-id" in create_exc.value.detail

    job = web_server._call_cron_for_profile(
        "worker_alpha",
        "create_job",
        prompt="managed by named profile",
        schedule="every 1h",
        name="context-update-target",
    )

    with pytest.raises(HTTPException) as update_exc:
        await web_server.update_cron_job(
            job["id"],
            web_server.CronJobUpdate(
                updates={
                    "context_from": ["missing-job-id"],
                }
            ),
            profile="worker_alpha",
        )

    assert update_exc.value.status_code == 400
    assert "missing-job-id" in update_exc.value.detail






