# Reproduce the data-engineering environment

Source: https://github.com/gorakhargosh/watchdog/commit/90d2fdfe6afe55b6f97fa043de5912d49771b667

This repository republishes the tracked files of that upstream commit under their original licenses.
Upstream CI workflows are omitted from the evaluation snapshot. This commit adds environment instructions
and the dependency lock only. The source license is Apache-2.0; original license and attribution files remain.

Python 3.13.15 and uv were used on Linux. From a fresh checkout:

```sh
uv venv --python 3.13.15 .venv
export PYTHONUTF8=1 PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1
uv pip install --python .venv/bin/python -r requirements-domain.lock
uv pip install --python .venv/bin/python --no-build-isolation --no-deps -e .
.venv/bin/python -B -m pytest -q -o addopts= -p no:cacheprovider tests/test_events.py tests/test_snapshot_diff.py tests/test_event_debouncer.py tests/test_skip_repeats_queue.py tests/test_observers_polling.py::test_hard_link_rename_emits_only_move
```

The selected tests use small fixtures or in-process substitutes. They need no remote service after dependency setup.
Start a fresh checkout and rebuild its virtual environment to restore initial state.
