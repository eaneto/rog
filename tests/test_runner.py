import glob
import os
import subprocess
import shutil

from rog_client import initialize_rog_server, kill_rog_server

test_files = glob.glob("tests/*_test.py")

os.environ["ROG_HOME"] = "/tmp/rog"

for test in test_files:
    # Reset the rog directory every test so that a log created in one
    # test doesn't affect the other test.
    try:
        shutil.rmtree("/tmp/rog")
    except FileNotFoundError:
        pass

    os.mkdir("/tmp/rog")
    pid = initialize_rog_server()
    try:
        print(f"Executing {test}")
        command = ["python", test]
        p = subprocess.run(command)
        if p.returncode != 0:
            raise Exception("Tests failed")
    finally:
        kill_rog_server(pid)
