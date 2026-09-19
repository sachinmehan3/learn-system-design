"""
Run every lesson's demo in order (or only those whose path contains a filter).

    python run_all.py              # everything
    python run_all.py caching      # only lessons with "caching" in their path
    python run_all.py --check      # run silently; just report pass/fail per lesson
"""

import os
import pathlib
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).parent


def lessons(filter_text=""):
    for path in sorted(ROOT.glob("[0-9][0-9]_*/*.py")):
        if filter_text.lower() in str(path.relative_to(ROOT)).lower():
            yield path


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    check_only = "--check" in sys.argv
    filter_text = args[0] if args else ""
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    failures = []
    for path in lessons(filter_text):
        rel = path.relative_to(ROOT)
        start = time.perf_counter()
        if not check_only:
            print("\n" + "#" * 80 + f"\n# {rel}\n" + "#" * 80)
        result = subprocess.run([sys.executable, str(path)], env=env,
                                capture_output=check_only, text=True)
        elapsed = time.perf_counter() - start
        ok = result.returncode == 0
        if not ok:
            failures.append(str(rel))
        if check_only:
            print(f"  {'PASS' if ok else 'FAIL'}  {elapsed:5.1f}s  {rel}")
            if not ok:
                print(result.stderr[-2000:])
    print(f"\n{'All lessons ran successfully.' if not failures else 'FAILED: ' + ', '.join(failures)}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
