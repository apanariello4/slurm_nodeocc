import sys
import os
from pathlib import Path


import numpy as np

conf_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(conf_path)
sys.path.append(conf_path)
from curses import wrapper
from view.curses_multiwindow import main, Singleton, try_open_socket_as_slave

from readers.compute_wait_time import get_wait_time
from importlib import reload
import readers.slurmreader
import view.slurm_list
import view.slurm_viz
import view.styles

import time
import json
import argparse
import traceback
from model.infrastructure import Infrastructure
from model.job import Job
import asyncio
import importlib.metadata

program_name = 'nodeocc'
version_number = importlib.metadata.version(program_name)
updated = False

BBLUE = '\033[1;34m'
END = '\033[0m'
try:
    url = "https://pypi.python.org/pypi/nodeocc/json"
    import requests
    newest_version = json.loads(requests.get(url).text)['info']['version']
    print(BBLUE, end='')
    if newest_version.split('.')[0] > version_number.split('.')[0]:
        print(f"New major version available: {newest_version} (current: {version_number})")
        print(f"Please update with 'pip install {program_name} --upgrade'")
        time.sleep(7)
    elif newest_version.split('.')[1] > version_number.split('.')[1]:
        print(f"New minor version available: {newest_version} (current: {version_number})")
        print(f"Please update with 'pip install {program_name} --upgrade'")
        time.sleep(7)
    elif newest_version.split('.')[2] > version_number.split('.')[2]:
        print(f"New patch version available: {newest_version} (current: {version_number})")
        print(f"Please update with 'pip install {program_name} --upgrade'")
        time.sleep(7)
    else:
        print(f"Up to date: {version_number}")
        updated = True
        time.sleep(3)
    print(END, end='')
except Exception as e:
    print(f"{BBLUE}Could not get newest version from pip, check if {program_name} is up to date{END}")
    newest_version = '?.?.?'
    time.sleep(5)


BEGIN_DELIM = "!{$"
END_DELIM_ENCODED = "!}$".encode('utf-8')
MAX_BUF = 65535
MAX_MSG_LEN = 1024 * 1024 * 1024

parser = argparse.ArgumentParser(description='Visualize slurm jobs')
parser.add_argument('--debug', action='store_true', help='Enable logging')
parser.add_argument('--master', action='store_true', help='Start master process')
parser.add_argument('--daemon_only', action='store_true', help='Disable all prints - only run in background')
parser.add_argument('--force_override', action='store_true', help='Force override of port file, kill previous instance')
parser.add_argument('--basepath', type=str, default='/nas/softechict-nas-2/mboschini/cool_scripts/new_nodeocc/', help='Base path for nodeocc. Must be readable by all users')
args = parser.parse_args()

# check basepath is readable
assert os.access(args.basepath, os.R_OK), f"Base path {args.basepath} not readable"

# export args
Singleton.getInstance(args)

last_update = os.path.getmtime(conf_path + "/view/slurm_viz.py")
if os.path.getmtime(conf_path + "/view/slurm_list.py") > last_update:
    last_update = os.path.getmtime(conf_path + "/view/slurm_list.py")
if os.path.getmtime(conf_path + "/view/styles.py") > last_update:
    last_update = os.path.getmtime(conf_path + "/view/styles.py")
if os.path.getmtime(conf_path + "/readers/slurmreader.py") > last_update:
    last_update = os.path.getmtime(conf_path + "/readers/slurmreader.py")


def get_avg_wait_time(instance: Singleton):
    try:
        raise Exception("Not implemented")
        avg_wait_time = get_wait_time('prod' if instance.cur_partition == 'prod' else 'students-prod')
        h = avg_wait_time // 3600
        m = (avg_wait_time % 3600) // 60
        time_str = f"{int(h)}h{int(m)}m"
        return time_str
    except Exception as e:
        instance.err(f"Could not get wait times")
        instance.err(traceback.format_exc())
        return 'err', 'err'


def update_data_master(instance):
    if not instance.check_port_file_master():
        instance.log(f"Port file dead, killing current instance")
        instance.sock.close()
        del instance
        exit(0)

    inf = readers.slurmreader.read_infrastructure()
    instance.timeme(f"- infrastructure load")

    jobs, _ = readers.slurmreader.read_jobs()
    instance.timeme(f"- jobs list read")

    # send data to clients (if any)
    running_jobs = [j.to_nested_dict() for j in jobs if j.state == 'CG' or j.state == 'R']
    queue_jobs = [j.to_nested_dict() for j in jobs if j.state != 'R' and j.state != 'CG']
    queue_jobs = sorted(queue_jobs, key=lambda x: x['priority'], reverse=True)
    maxlen = min(100, len(running_jobs) + len(queue_jobs))

    msg = json.dumps({'inf': inf.to_nested_dict(), 'jobs': running_jobs + queue_jobs[:maxlen - len(running_jobs)]})

    # msg to bytes
    msg = msg.encode('utf-8')
    msg_len = len(msg)
    msg = (str(msg_len) + BEGIN_DELIM).encode('utf-8') + msg + END_DELIM_ENCODED
    instance.timeme(f"- msg encoded with len {msg_len}")

    instance.sock.sendto(msg, ('<broadcast>', instance.port))
    instance.timeme(f"- broadcast")

    return inf, jobs


async def get_data_slave(instance):
    inf, jobs = None, None
    try:
        # listen for data from master
        # instance.sock.settimeout(6.5)
        data, addr = instance.sock.recvfrom(MAX_BUF)

        if BEGIN_DELIM in data.decode('utf-8'):
            msg_len = int(data.decode('utf-8').split(BEGIN_DELIM)[0])
            msg_len += len(str(msg_len)) + 2
            instance.timeme(f"about to receive {msg_len} bytes")
            while END_DELIM_ENCODED not in data:
                buf_len = min(MAX_BUF, msg_len - len(data))
                instance.timeme(f"bytes remaining {msg_len - len(data)} - receiving")
                data += instance.sock.recvfrom(buf_len)[0]

                if len(data) > MAX_MSG_LEN:
                    instance.err(f"Message too long, aborting")
                    return

            data = data.split(END_DELIM_ENCODED)[0]
            # data = data[:msg_len]
            data = data.decode('utf-8')
            data = data.split(BEGIN_DELIM)[1]

            msg = json.loads(data)
            inf = Infrastructure.from_dict(msg['inf'])
            jobs = [Job.from_dict(j) for j in msg['jobs']]
            avg_wait_time = "N/A"  # get_avg_wait_time(instance)
            instance.timeme(f"- receive")
        else:
            instance.timeme(f"- no data")
            return None, None, None, None

    except BlockingIOError as e:
        pass

    except TimeoutError as e:
        instance.log(f"TIMEOUT")
        try_open_socket_as_slave(instance)

    return inf, jobs, avg_wait_time


async def get_all():
    global last_update
    # watch for reload
    updt = os.path.getmtime(conf_path + "/view/slurm_viz.py")
    updt = max(updt, os.path.getmtime(conf_path + "/view/slurm_list.py"))
    updt = max(updt, os.path.getmtime(conf_path + "/view/styles.py"))
    updt = max(updt, os.path.getmtime(conf_path + "/readers/slurmreader.py"))

    instance = Singleton.getInstance()
    instance.timeme(f"Starting update")

    if updt > last_update:
        reload(readers.slurmreader)
        reload(view.slurm_list)
        reload(view.slurm_viz)
        reload(view.styles)
        last_update = updt

    instance.timeme(f"- reload")

    inf, jobs, avg_wait_time = None, None, None
    try:
        if args.master:
            inf, jobs = update_data_master(instance)
        else:
            # loop = asyncio.get_event_loop()
            instance.timeme(f"- listening for data")

            # wait for data from master but async update the view
            inf, jobs, avg_wait_time = await get_data_slave(instance)
    except Exception as e:
        instance.err(f"Exception: {e}")
        instance.err(traceback.format_exc())
        # instance.rens = 'Something went wrong'
        # instance.nocc = ':('

    return inf, jobs, avg_wait_time


def display_main(stdscr):
    return asyncio.run(main(stdscr))


def _main():
    if args.daemon_only:
        assert args.master, "Daemon mode only available for master"
        instance = Singleton.getInstance()

        instance.log(f"Starting master daemon")
        # register atexit
        import atexit

        def exit_handler():
            instance.log(f"Exiting...")
            instance.sock.close()
            # remove .port file
            if instance.port_file_exists():
                Path(instance.get_port_file_name()[1]).unlink()
        atexit.register(exit_handler)

        while True:
            instance.timeme(f"Updating...")
            try:
                update_data_master(instance)
            except Exception as e:
                instance.err(f"Exception: {e}")
                instance.err(traceback.format_exc())

            time.sleep(5)

    else:
        # configure singleton

        Singleton.getInstance().signature = f"{program_name} v{version_number}"
        Singleton.getInstance().version = version_number
        Singleton.getInstance().updated = updated
        Singleton.getInstance().newest_version = newest_version
        Singleton.getInstance().fetch_fn = get_all

        wrapper(display_main)


if __name__ == '__main__':
    _main()
