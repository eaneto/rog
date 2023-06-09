import glob
import sys
import os
import subprocess
import shutil

from rog_client import initialize_rog_server, kill_rog_server

test_files = glob.glob("tests/*_test.py")

os.environ["ROG_HOME"] = "/tmp/rog"

try:
    profile = sys.argv[1]
except:
    profile = "local"

any_failed = False
for test in test_files:
    # Reset the rog directory every test so that a log created in one
    # test doesn't affect the other test.
    try:
        shutil.rmtree("/tmp/rog")
    except FileNotFoundError:
        pass

    os.mkdir("/tmp/rog")
    pid = initialize_rog_server(profile)
    try:
        print(f"Executing {test}")
        command = ["python", test]
        p = subprocess.run(command)
        if p.returncode != 0:
            any_failed = True
    finally:
        kill_rog_server(pid)

if any_failed:
    raise Exception("At least one of the tests failed")
