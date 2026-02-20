#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = [
#   "typer>=0.12",
#   "servicex",
#   "func_adl_servicex_xaodr25",
#   "servicex_analysis_utils",
#   "jinja2",
# ]
# ///

from __future__ import annotations

import asyncio
import inspect
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import typer
from func_adl_servicex_xaodr25 import FuncADLQueryPHYSLITE
from servicex import Sample, ServiceXSpec, dataset, deliver
from servicex.servicex_client import ProgressBarFormat
from servicex_analysis_utils import to_awk

try:
    from servicex import deliver_async as sx_deliver_async
except ImportError:
    sx_deliver_async = None

DATASET_DID = "user.zmarshal:user.zmarshal.364702_OpenData_v1_p6026_2024-04-23"
SAMPLE_NAME = "jet_pt_fetch"
QUERY_TIMEOUT_SECONDS = 600

app = typer.Typer(add_completion=False, help="ServiceX fetch and swarm stress tool.")


def build_query() -> Any:
    return (
        FuncADLQueryPHYSLITE()
        .Select(lambda evt: {"jets": evt.Jets()})
        .Select(
            lambda collections: {
                "jet_pt": collections.jets.Select(lambda jet: jet.pt() / 1000.0)
            }
        )
    )


def build_spec(query: Any) -> ServiceXSpec:
    return ServiceXSpec(
        Sample=[
            Sample(
                Name=SAMPLE_NAME,
                Dataset=dataset.Rucio(DATASET_DID),
                NFiles=1,
                Query=query,
            )
        ]
    )


async def run_deliver_async(spec: ServiceXSpec) -> Any:
    result = sx_deliver_async(
        spec,
        ignore_local_cache=True,
        progress_bar=ProgressBarFormat.none,
    )
    if inspect.isawaitable(result):
        return await result

    wait_method = getattr(result, "wait", None)
    if callable(wait_method):
        waited = wait_method()
        if inspect.isawaitable(waited):
            return await waited
        return waited

    return result


def run_deliver_sync_with_timeout(spec: ServiceXSpec, timeout_seconds: int) -> Any:
    def on_timeout(_signum: int, _frame: Any) -> None:
        raise TimeoutError

    previous = signal.signal(signal.SIGALRM, on_timeout)
    signal.setitimer(signal.ITIMER_REAL, float(timeout_seconds))
    try:
        return deliver(
            spec,
            ignore_local_cache=True,
            progress_bar=ProgressBarFormat.none,
        )
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, previous)


def count_jets(delivered: Any) -> int:
    awkward_payload = to_awk(delivered)
    sample_data = awkward_payload[SAMPLE_NAME]
    jet_pts = sample_data["jet_pt"]
    total_jets = 0
    for event_jets in jet_pts:
        total_jets += len(event_jets)
    return int(total_jets)


def wait_for_release_file(release_file: Path, timeout_seconds: float) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if release_file.exists():
            return True
        time.sleep(0.01)
    return False


@app.command()
def fetch(
    release_file: Path | None = typer.Option(
        None,
        "--release-file",
        help="Wait until this file exists before starting the query.",
    ),
    release_wait_timeout: float = typer.Option(
        30.0,
        "--release-wait-timeout",
        min=0.1,
        help="Maximum seconds to wait for --release-file before failing.",
    ),
) -> None:
    if release_file is not None:
        if not wait_for_release_file(release_file, release_wait_timeout):
            print("Timed out waiting for release signal", file=sys.stderr)
            raise typer.Exit(code=1)

    start = time.perf_counter()
    query = build_query()
    spec = build_spec(query)

    try:
        if sx_deliver_async is None:
            delivered = run_deliver_sync_with_timeout(spec, QUERY_TIMEOUT_SECONDS)
        else:
            delivered = asyncio.run(
                asyncio.wait_for(run_deliver_async(spec), timeout=QUERY_TIMEOUT_SECONDS)
            )
    except TimeoutError:
        print("Query timed out")
        raise typer.Exit(code=1)
    except Exception as exc:
        print(f"Query failed: {exc}", file=sys.stderr)
        raise typer.Exit(code=1)

    elapsed = time.perf_counter() - start
    jets = count_jets(delivered)
    print(f"Query took {elapsed:.2f} seconds. Found {jets} jets")


def classify_result(return_code: int, stdout: str) -> str:
    if return_code == 0:
        return "ok"
    if stdout.strip() == "Query timed out":
        return "timeout"
    return "failed"


def render_progress(
    *,
    done: int,
    total: int,
    ok: int,
    timeout: int,
    failed: int,
    elapsed: float,
) -> str:
    running = total - done
    return (
        f"\rElapsed {elapsed:.1f}s | running={running} done={done}/{total} "
        f"ok={ok} timeout={timeout} failed={failed}"
    )


@app.command()
def swarm(
    count: int = typer.Argument(..., min=1, help="Number of fetch jobs to start."),
    release_delay: float = typer.Option(
        10.0,
        "--release-delay",
        min=0.0,
        help="Seconds to wait after launching workers before releasing them.",
    ),
    release_wait_timeout: float = typer.Option(
        30.0,
        "--release-wait-timeout",
        min=0.1,
        help="Timeout passed to child fetch jobs while waiting for release signal.",
    ),
) -> None:
    script_path = Path(__file__).resolve()
    start = time.perf_counter()
    release_fd, release_name = tempfile.mkstemp(prefix="servicex_swarm_release_")
    release_path = Path(release_name)
    # Workers need to wait for file creation, so ensure it does not exist yet.
    Path(release_name).unlink(missing_ok=True)
    os.close(release_fd)

    processes: list[subprocess.Popen[str]] = []
    try:
        for _ in range(count):
            proc = subprocess.Popen(
                [
                    sys.executable,
                    str(script_path),
                    "fetch",
                    "--release-file",
                    str(release_path),
                    "--release-wait-timeout",
                    str(release_wait_timeout),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            processes.append(proc)

        print(
            f"Started {count} workers. Waiting {release_delay:.1f}s before release signal..."
        )
        time.sleep(release_delay)
        release_path.touch()
        print("Release signal sent. Workers are now starting queries.")

        statuses = ["running"] * count
        done = 0

        while done < count:
            for idx, proc in enumerate(processes):
                if statuses[idx] != "running":
                    continue
                return_code = proc.poll()
                if return_code is None:
                    continue

                stdout, _stderr = proc.communicate()
                statuses[idx] = classify_result(return_code, stdout)
                done += 1

            ok = statuses.count("ok")
            timeout = statuses.count("timeout")
            failed = statuses.count("failed")
            elapsed = time.perf_counter() - start
            sys.stdout.write(
                render_progress(
                    done=done,
                    total=count,
                    ok=ok,
                    timeout=timeout,
                    failed=failed,
                    elapsed=elapsed,
                )
            )
            sys.stdout.flush()
            time.sleep(0.2)

        sys.stdout.write("\n")
        total_elapsed = time.perf_counter() - start
        ok = statuses.count("ok")
        timeout = statuses.count("timeout")
        failed = statuses.count("failed")
        print(
            f"Swarm finished in {total_elapsed:.2f}s. "
            f"ok={ok} timeout={timeout} failed={failed}"
        )

        if timeout > 0 or failed > 0:
            raise typer.Exit(code=1)
    finally:
        release_path.unlink(missing_ok=True)


if __name__ == "__main__":
    app()
