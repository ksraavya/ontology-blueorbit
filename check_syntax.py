import py_compile
import sys

sys.path.insert(0, '.')

files = [
    'modules/climate/constants.py',
    'modules/climate/ingest.py',
    'modules/climate/transform.py',
    'modules/climate/compute.py',
    'modules/climate/load.py',
    'modules/climate/bridge.py',
    'modules/climate/runner.py',
    'analytics/climate/scores.py',
    'analytics/climate/runner.py',
]

all_ok = True
for f in files:
    try:
        py_compile.compile(f, doraise=True)
        print(f'OK   {f}')
    except py_compile.PyCompileError as e:
        print(f'ERR  {f}')
        print(f'     {e}')
        all_ok = False

print()
if all_ok:
    print('All files passed syntax check.')
else:
    print('Some files have errors — fix them before running the pipeline.')

sys.exit(0 if all_ok else 1)