import sys
sys.path.insert(0, '.')

modules = [
    'modules.climate.constants',
    'modules.climate.transform',
    'modules.climate.compute',
    'modules.climate.load',
    'modules.climate.bridge',
    'analytics.climate.scores',
    'analytics.climate.runner',
]

all_ok = True
for mod in modules:
    try:
        __import__(mod)
        print(f'OK   {mod}')
    except ImportError as e:
        print(f'ERR  {mod}')
        print(f'     Missing package: {e}')
        all_ok = False
    except Exception as e:
        print(f'ERR  {mod}')
        print(f'     {e}')
        all_ok = False

print()
if all_ok:
    print('All imports passed.')
else:
    print('Fix the errors above before running the pipeline.')

sys.exit(0 if all_ok else 1)