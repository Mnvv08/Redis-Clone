"""The whole tour in one command: python demo.py   (about 5 seconds)"""
import runpy

TOUR = [
    ("Pulling the plug on a cluster: with and without fsync", "power_demo.py"),
    ("A key-value store where retries never run twice", "kv_demo.py"),
    ("Checking the history is linearizable, and catching a buggy server", "lin_demo.py"),
    ("Snapshots: the log stays small and a sleeping node catches up", "snapshot_demo.py"),
    ("The real thing: 3 processes, TCP and files; kill the leader", "real_demo.py"),
]

for number, (title, script) in enumerate(TOUR, 1):
    print(f"\n{'=' * 78}\n{number}/{len(TOUR)}  {title}   ({script})\n{'=' * 78}")
    runpy.run_path(script, run_name="__main__")
print("\nRun the test suite with: python -m pytest")
