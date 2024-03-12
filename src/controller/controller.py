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
EXTRA_MSG_BEGIN_DELIM = "!{#"
MAX_BUF = 65535
MAX_MSG_LEN = 1024 * 1024 * 1024
JOB_LIMIT_PER_MSG = 100

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

def send_msg(instance, msg, n_extra_msgs=0, is_extra=False):
    # msg to bytes
    msg = msg.encode('utf-8')
    begin_delim = EXTRA_MSG_BEGIN_DELIM if is_extra else BEGIN_DELIM
    # for backward compatibility, msg_len = n_extra_msgs if extra data is expected, 0 otherwise
    msg_len = n_extra_msgs
    # msg_len = len(msg)
    msg = (str(msg_len) + begin_delim).encode('utf-8') + msg + END_DELIM_ENCODED
    instance.timeme(f"- msg encoded with len {msg_len}")

    instance.sock.sendto(msg, ('<broadcast>', instance.port))

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
    maxlen = min(JOB_LIMIT_PER_MSG, len(running_jobs) + len(queue_jobs))
    
    queue_jobs, extra_queue_jobs = queue_jobs[:maxlen - len(running_jobs)], queue_jobs[maxlen - len(running_jobs):]
    cur_timestamp = time.time()
    msg = json.dumps({'inf': inf.to_nested_dict(), 'jobs': running_jobs + queue_jobs, 'ts': str(cur_timestamp)})


    N_EXTRA_MSGS = (len(extra_queue_jobs) // JOB_LIMIT_PER_MSG) + 1 if len(extra_queue_jobs) > 0 else 0
    send_msg(instance, msg, n_extra_msgs=N_EXTRA_MSGS)
    instance.timeme("- main broadcast")

    if extra_queue_jobs:
        # get batches of JOB_LIMIT_PER_MSG jobs
        for job_idx in range(N_EXTRA_MSGS):
            i = job_idx * JOB_LIMIT_PER_MSG
            msg = json.dumps({'inf': inf.to_nested_dict(), 'jobs': extra_queue_jobs[i:i + JOB_LIMIT_PER_MSG], 'ts': str(cur_timestamp)})
            send_msg(instance, msg, is_extra=True, n_extra_msgs=N_EXTRA_MSGS - job_idx - 1)
            instance.timeme(f"- extra broadcast #{job_idx+1}")

    return inf, jobs

def decode_msg_slave(data, delim=BEGIN_DELIM):
    data = data.split(END_DELIM_ENCODED)[0]
    data = data.decode('utf-8')
    data = data.split(delim)[1]

    msg = json.loads(data)
    inf = Infrastructure.from_dict(msg['inf'])
    jobs = [Job.from_dict(j) for j in msg['jobs']]
    avg_wait_time = "N/A"  # get_avg_wait_time(instance)
    return inf, jobs, avg_wait_time, msg

async def get_data_slave(instance):
    inf, jobs = None, None
    try:
        # listen for data from master
        # instance.sock.settimeout(6.5)
        data, _ = instance.sock.recvfrom(MAX_BUF)
        decoded_data = data.decode('utf-8')
        if BEGIN_DELIM in decoded_data:
            n_msgs_remaining = int(decoded_data.split(BEGIN_DELIM)[0])
            # msg_len += len(str(msg_len)) message len is deprecated and used for n_msgs_remaining
            instance.timeme(f"received first message of {n_msgs_remaining} (total len {len(data)} bytes)")

            inf, jobs, avg_wait_time, decoded_msg = decode_msg_slave(data)
            instance.timeme(f"- receive")
            orig_timestamp = float(decoded_msg['ts'])

            total_msg_n = n_msgs_remaining
            msg_store = {i:[] for i in range(total_msg_n)}
            while n_msgs_remaining>0:
                data, _ = instance.sock.recvfrom(MAX_BUF)
                back_msg_idx = int(data.decode('utf-8').split(EXTRA_MSG_BEGIN_DELIM)[0])
                msg_idx = total_msg_n - back_msg_idx - 1 
                _, new_jobs, _, new_ts = decode_msg_slave(data, delim=EXTRA_MSG_BEGIN_DELIM)

                # check if timestamp is the same
                if float(new_ts['ts']) != orig_timestamp:
                    instance.timeme(f"Timestamp mismatch, skipping")
                    return inf, jobs, avg_wait_time # return what we have
                
                msg_store[msg_idx] += new_jobs

                n_msgs_remaining -= 1
                instance.timeme(f"- extra receive, {n_msgs_remaining} remaining")

            # sort and merge 
            for msg_idx in range(total_msg_n):
                if len(msg_store[msg_idx]) == 0:
                    instance.log(f"Empty message {msg_idx}")
                jobs += msg_store[msg_idx]
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
