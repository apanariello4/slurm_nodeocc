import asyncio
import curses
import grp
import logging
import os
import signal
import socket
import threading
import time
import traceback
from logging.handlers import RotatingFileHandler
from pathlib import Path

from view import update_views

a_filter_values = [None, 'me', 'prod', 'stud', 'cvcs']

BUTTON_ACTIONS = {
    'up_1': [ord('w'), curses.KEY_UP, ord('k')],
    'down_1': [ord('s'), curses.KEY_DOWN, ord('j')],
    'left_1': [ord('a'), curses.KEY_LEFT, ord('h')],
    'right_1': [ord('d'), curses.KEY_RIGHT, ord('l')],
    'up_tab': [curses.KEY_NPAGE],
    'down_tab': [curses.KEY_PPAGE],
    'quit': [ord('q'), 'q'],
    'sort_prio': [ord('S')],
    'res_view_mode': [ord('g')],
    'job_id_type': [ord('b')],
    'show_starttime': [ord('z')],
    'show_account': [ord('t')],
    'show_prio': [ord('p')],
    'refresh': [ord('y')],
    'info': [ord('i')],
}


def try_open_socket_as_slave(instance):
    if not instance.port_file_exists():
        raise Exception("No master running")

    cur_port = int(instance.get_port_file_name()[0].split('.')[0])
    if hasattr(instance, 'port'):
        # check if port file has same name
        if instance.port != cur_port:
            instance.log(f"- MASTER HAS CHANGED PORT: {instance.port} vs {cur_port}")

    instance.port = cur_port
    instance._open_socket_as_slave(instance.port)

    if instance.try_open_counter > 5:
        instance.err(f"Could not open socket as slave on port {instance.port}")
        raise Exception(f"Could not open socket on port {instance.port}")


class Singleton:
    __instance = None

    @staticmethod
    def getInstance(args=None):
        """ Static access method. """
        if Singleton.__instance is None:
            Singleton(args)
        if args is not None:
            Singleton.__instance.args = args
        return Singleton.__instance

    # destructor
    def __del__(self):
        if self.args.master:
            if self.port_file_exists():
                Path(self.get_port_file_name()[1]).unlink()

    def __init__(self, args=None):
        """ Virtually private constructor. """
        if Singleton.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            Singleton.__instance = self
        self.try_open_counter = 0
        self.voff = 0
        self.mouse_state = {}
        self.max_columns = 0

        self.nocc = ""
        self.rens = ""

        self.fetch_fn = None
        self.view_mode = 'gpu'
        self.job_id_type = 'agg'
        self.args = args

        self.show_account = False
        self.show_prio = False
        self.sort_by_prio = False
        self.show_starttime = False

        self.inf = None
        self.jobs = []
        self.avg_wait_time = 'err'
        self.a_filter = 0
        self.k = -1

        # check if user group is 'student' or 'tesisti'
        user_group = grp.getgrgid(os.getgid()).gr_name
        self.cur_partition = 'stud' if user_group in ['studenti', 'tesisti'] else 'prod'

        self.basepath = self.args.basepath

        if self.args.daemon_only:
            handler = RotatingFileHandler(os.path.join(self.basepath, '.master_log.txt'), maxBytes=5 * 1024 * 1024, backupCount=2, mode='w')
            logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S',
                                handlers=[handler])
        # set logging file
        elif self.args.debug:
            # create rotating file handler
            handler = RotatingFileHandler(os.path.expanduser('~') + '/.nodeocc_log.txt', maxBytes=5 * 1024 * 1024, backupCount=2, mode='w')
            logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S',
                                handlers=[handler])

        if self.args.master:
            # check if any .port file exists
            if self.port_file_exists():
                if self.args.force_override:
                    _, port_filepath = self.get_port_file_name()

                    # read pid from file
                    pid = Path(port_filepath).read_text()
                    # kill process
                    try:
                        os.kill(int(pid), signal.SIGTERM)
                    except Exception as e:
                        self.log("Could not kill process, deleting port file")
                        self.log(e)

                    # delete port file
                    Path(port_filepath).unlink()
                else:
                    raise Exception("Master already running")
            self.port = self.create_socket_as_master()
        else:
            try_open_socket_as_slave(self)

    def get_port_file_name(self):
        if self.port_file_exists():
            fname = [f for f in os.listdir(self.basepath) if f.endswith('.port')][0]
            return fname, os.path.join(self.basepath, fname)
        return None

    def check_port_file_master(self):
        if self.port_file_exists():
            prev_port = int(self.get_port_file_name()[0].split('.')[0])
            if hasattr(self, 'port'):
                return self.port == prev_port
        return False

    def port_file_exists(self):
        return len([f for f in os.listdir(self.basepath) if f.endswith('.port')]) > 0

    def create_socket_as_master(self):
        # create udp socket for broadcasting
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Enable broadcasting mode
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        self.sock.bind(('', 0))

        self.port = self.sock.getsockname()[1]
        self.sock.settimeout(2)
        self.log(f"Socket created as master on port {self.port}")

        # get pid of current process
        self.pid = os.getpid()

        # create file to store port
        Path(self.basepath, f'{self.port}.port').write_text(str(self.pid))

        return self.port

    def _open_socket_as_slave(self, port):
        try:
            # create udp socket for broadcasting
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # sock listen on port
            self.sock.bind(('', port))

            self.sock.setblocking(True)

            self.sock.settimeout(6.5)
            self.log(f"Socket opened on port {self.port}")

            self.try_open_counter = 0
            return self.port
        except Exception as e:
            self.err(f"Exception: {e}")
            self.err(traceback.format_exc())
            self.try_open_counter += 1
            return None

    def timeme(self, msg=None):
        if not hasattr(self, '_ctime'):
            self._ctime = time.time()
            if msg is not None:
                self.log(msg)
        else:
            _ntime = time.time()
            self.log(f"{msg} took {_ntime - self._ctime:.2f} seconds")
            self._ctime = _ntime

    def err(self, msg):
        if self.args.debug or self.args.daemon_only:
            logging.error(msg)

    def log(self, msg):
        if self.args.debug or self.args.daemon_only:
            logging.info(msg)

    async def fetch(self):
        _ctime = time.time()

        if self.fetch_fn is not None:
            inf, jobs, avg_wait_time = await self.fetch_fn()  # a_filter_values[self.a_filter])
            self.inf = inf if inf is not None else self.inf
            self.jobs = jobs if jobs is not None else self.jobs
            self.avg_wait_time = avg_wait_time if avg_wait_time is not None else self.avg_wait_time

        _delta_t = time.time() - _ctime
        self.log(f"Fetch took {_delta_t:.2f} seconds")

    def add_button(self, y, x, width, action):
        if y not in self.mouse_state:
            self.mouse_state[y] = {}
        if isinstance(width, str):
            width = len(width)
        for j in range(x, x + width):
            self.mouse_state[y][j] = action


class Buffer(object):

    def __init__(self, window, lines, screen):
        self.window = window
        self.lines = lines
        self.buffer = [""]
        self.screen = screen

    def write(self, text):
        lines = text.split("\n")
        self.buffer[-1] += lines[0]
        self.buffer.extend(lines[1:])
        self.refresh()

    def writeln(self, text):
        self.write(text + "\n")

    def input(self, text=""):
        return self._input(text, lambda: self.window.getstr().decode('utf-8'))

    def input_chr(self, text=""):
        return self._input(text, lambda: chr(self.window.getch()))

    def _input(self, text, get_input):
        self.write(text)
        input = get_input()
        self.writeln(input)
        return input

    def refresh(self, voff=0, usevoff=False):
        instance = Singleton.getInstance()
        self.window.erase()
        off = max(0, ((self.lines - 2) - len(self.buffer)) // 2)
        if usevoff and voff < 0:
            instance.voff = 0
        elif usevoff and voff > len(self.buffer) - self.lines + 2:
            instance.voff = len(self.buffer) - self.lines + 2

        voff = max(min(voff, len(self.buffer) - self.lines + 2), 0)
        for nr, line in enumerate(self.buffer[voff:voff + self.lines - 2]):  # self.buffer[-self.lines+2:]):
            lastcol = 2
            xacc = 1

            for chunk in line.split('<*'):
                if ':*>' in chunk:

                    color = int(chunk[0:chunk.index('~')])
                    chunk_segments = chunk[chunk.index('~') + 1:].split(':*>')
                    try:
                        self.window.addstr(nr + off + 1, xacc, chunk_segments[0], curses.color_pair(color) | (curses.A_REVERSE if color > 9 else 0))
                    except curses.error:
                        pass
                    xacc += len(chunk_segments[0])

                    if len(chunk_segments[1]) > 0:
                        self.window.addstr(nr + off + 1, xacc, chunk_segments[1])
                        xacc += len(chunk_segments[1])
                else:
                    try:
                        self.window.addstr(nr + off + 1, xacc, chunk)
                    except Exception as e:
                        instance.err(e)
                        pass
                    xacc += len(chunk)
        if len(self.buffer) > self.lines - 2:
            self.screen.addstr(self.lines - 2, instance.xoffset + instance.left_width // 2 - 6, ' ▼ SCROLL ▲ ', curses.color_pair(2) | curses.A_REVERSE)
            instance.add_button(self.lines - 2, instance.xoffset + 31, 'D', BUTTON_ACTIONS['down_1'][0])
            instance.add_button(self.lines - 2, instance.xoffset + 40, 'U', BUTTON_ACTIONS['up_1'][0])

        self.window.border()
        self.window.noutrefresh()


def process_mouse():
    try:
        _, x, y, _, bstate = curses.getmouse()

        if bstate & curses.BUTTON1_RELEASED != 0:
            ms = Singleton.getInstance().mouse_state
            if y in ms and x in ms[y]:
                return True, ms[y][x]
        return False, -1
    except BaseException:
        return False, -1


def handle_keys(stdscr, instance):
    k = stdscr.getch()
    instance.log("GOT CHAR: " + str(k))
    if k == curses.KEY_RESIZE:
        instance.log("RESIZED WINDOW")

    if k == curses.ERR and k == -1:
        return

    instance.k = k

    valid_mouse = False
    if k == curses.KEY_MOUSE:
        valid_mouse, ck = process_mouse()
        if valid_mouse:
            k = ck

    instance.mouse_state = {}

    # process input
    # RIGHT
    if k in BUTTON_ACTIONS['right_1']:
        instance.a_filter = (instance.a_filter + 1) % len(a_filter_values)
        instance.voff = 0
    # LEFT
    elif k in BUTTON_ACTIONS['left_1']:
        instance.a_filter = (instance.a_filter + (len(a_filter_values) - 1)) % len(a_filter_values)
        instance.voff = 0
    elif valid_mouse and isinstance(k, str) and k.startswith('AF_'):
        instance.a_filter = int(k.split('AF_')[1])
    # DOWN
    elif k in BUTTON_ACTIONS['down_1']:
        instance.voff += 1
    # UP
    elif k in BUTTON_ACTIONS['up_1']:
        instance.voff -= 1

    elif k in BUTTON_ACTIONS['sort_prio']:
        instance.sort_by_prio = not instance.sort_by_prio
    elif k in BUTTON_ACTIONS['res_view_mode']:
        instance.view_mode = {"gpu": "ram", "ram": "cpu", "cpu": "gpu"}[instance.view_mode]
        instance.right_width = 33 if instance.view_mode != 'info' else 39
        stdscr.clear()
    elif k in BUTTON_ACTIONS['job_id_type']:
        instance.job_id_type = "true" if instance.job_id_type == "agg" else "agg"
    elif k in BUTTON_ACTIONS['show_starttime']:
        # instance.show_starttime = not instance.show_starttime
        pass
    elif k in BUTTON_ACTIONS['show_account']:
        instance.show_account = not instance.show_account
    elif k in BUTTON_ACTIONS['show_prio']:
        instance.show_prio = not instance.show_prio
    elif k in BUTTON_ACTIONS['info']:
        instance.view_mode = 'info' if instance.view_mode != 'info' else 'gpu'
        instance.right_width = 33 if instance.view_mode != 'info' else 39
        stdscr.clear()
    elif k in BUTTON_ACTIONS['up_tab']:
        # get screen size
        height, width = stdscr.getmaxyx()

        instance.voff += ((height - 4) // 2) + 1
    elif k in BUTTON_ACTIONS['down_tab']:
        height, width = stdscr.getmaxyx()

        instance.voff -= ((height - 4) // 2) + 1


def update_screen(stdscr, instance):
    update_views(stdscr, instance, a_filter_values[instance.a_filter])
    xoffset = 0
    instance.xoffset = xoffset

    lines, columns = os.get_terminal_size().lines, os.get_terminal_size().columns
    s_lines, s_columns = stdscr.getmaxyx()
    instance.max_columns = s_columns
    if instance.k in BUTTON_ACTIONS['refresh']:
        stdscr.clear()

    totsize = 106
    if instance.show_account:
        totsize += 10
    if instance.show_prio:
        totsize += 8
    totsize += 10  # from /etc/update-motd.d/02-wait-times

    if columns < totsize:
        stdscr.addstr(1, 1, "MINIMUM TERM. WIDTH")
        stdscr.addstr(2, 1, f"REQUIRED: {totsize}")
        stdscr.addstr(3, 1, "CURRENT: " + str(columns))
        stdscr.refresh()
        return

    # update state (recompute lines for safety)
    if lines != s_lines or columns != s_columns:
        instance.log(f"Resized window: {s_lines}x{s_columns} -> {lines}x{columns}")
        stdscr.clear()
        s_columns = columns
        s_lines = lines

    stdscr.refresh()

    left_width = columns - instance.right_width  # 72
    right_width = instance.right_width  # 33
    instance.left_width = left_width
    left_window = curses.newwin(lines - 1, left_width, 0, xoffset)
    left_buffer = Buffer(left_window, lines, stdscr)
    right_window = curses.newwin(lines - 1, right_width - 2, 0, xoffset + left_width + 1)
    right_buffer = Buffer(right_window, lines, stdscr)

    left_buffer.write(instance.rens)
    right_buffer.write(instance.nocc)
    right_buffer.refresh()
    left_buffer.refresh(instance.voff, True)

    # render menu
    stdscr.addstr(lines - 1, xoffset + 1 + 0, '◀')
    instance.add_button(lines - 1, xoffset + 1 + 0, '◀', BUTTON_ACTIONS['left_1'][0])
    stdscr.addstr(lines - 1, xoffset + 1 + 2, 'ALL', curses.color_pair(2) | (curses.A_REVERSE if a_filter_values[instance.a_filter] is None else 0))

    instance.add_button(lines - 1, xoffset + 1 + 2, 'ALL', 'AF_0')

    stdscr.addstr(lines - 1, xoffset + 1 + 6, 'ME', curses.color_pair(2) | (curses.A_REVERSE if a_filter_values[instance.a_filter] == 'me' else 0))
    instance.add_button(lines - 1, xoffset + 1 + 6, 'ME', 'AF_1')
    stdscr.addstr(lines - 1, xoffset + 1 + 9, 'PROD', curses.color_pair(2) | (curses.A_REVERSE if a_filter_values[instance.a_filter] == 'prod' else 0))
    instance.add_button(lines - 1, xoffset + 1 + 9, 'PROD', 'AF_2')
    stdscr.addstr(lines - 1, xoffset + 1 + 14, 'STUD', curses.color_pair(2) | (curses.A_REVERSE if a_filter_values[instance.a_filter] == 'stud' else 0))
    instance.add_button(lines - 1, xoffset + 1 + 14, 'STUD', 'AF_3')
    stdscr.addstr(lines - 1, xoffset + 1 + 19, 'CVCS', curses.color_pair(2) | (curses.A_REVERSE if a_filter_values[instance.a_filter] == 'cvcs' else 0))
    instance.add_button(lines - 1, xoffset + 1 + 19, 'CVCS', 'AF_4')

    stdscr.addstr(lines - 1, xoffset + 1 + 24, '▶')
    instance.add_button(lines - 1, xoffset + 1 + 24, '▶', BUTTON_ACTIONS['right_1'][0])
    stdscr.addstr(lines - 1, xoffset + 1 + 25, ' ' * (columns - 27 - xoffset))

    stdscr.addstr(lines - 1, left_width - 8, '[Q:QUIT]', curses.color_pair(2))
    instance.add_button(lines - 1, left_width - 8, '[Q:QUIT]', BUTTON_ACTIONS['quit'][0])  # 53

    stdscr.addstr(lines - 1, left_width - 18 + 19, '[Y:REDRAW]', curses.color_pair(2))
    instance.add_button(lines - 1, left_width - 18 + 19, '[Y:REDRAW]', BUTTON_ACTIONS['refresh'][0])

    stdscr.addstr(0, left_width + 2, '[I:', curses.color_pair(2))
    stdscr.addstr(0, left_width + 5, 'INFO', curses.color_pair(2) | (curses.A_REVERSE if instance.view_mode == 'info' else 0))
    stdscr.addstr(0, left_width + 9, ']', curses.color_pair(2))
    instance.add_button(0, left_width + 2, '[I:INFO]', BUTTON_ACTIONS['info'][0])
    stdscr.addstr(0, left_width + 10, '─' * (columns - 15 - left_width - 3), curses.color_pair(2))
    stdscr.addstr(0, columns - 15, '[G:', curses.color_pair(2))
    stdscr.addstr(0, columns - 12, 'GPU', curses.color_pair(2) | (curses.A_REVERSE if instance.view_mode == 'gpu' else 0))
    stdscr.addstr(0, columns - 12 + 3, 'RAM', curses.color_pair(2) | (curses.A_REVERSE if instance.view_mode == 'ram' else 0))
    stdscr.addstr(0, columns - 12 + 6, 'CPU', curses.color_pair(2) | (curses.A_REVERSE if instance.view_mode == 'cpu' else 0))
    stdscr.addstr(0, columns - 12 + 9, ']', curses.color_pair(2))
    instance.add_button(0, columns - 15, '[G:GPURAMCPU]', BUTTON_ACTIONS['res_view_mode'][0])

    stdscr.addstr(lines - 1, xoffset + 25 + 2, '[B:', curses.color_pair(2))
    stdscr.addstr(lines - 1, xoffset + 25 + 2 + 3, 'AGG', curses.color_pair(2) | (curses.A_REVERSE if instance.job_id_type == 'agg' else 0))
    stdscr.addstr(lines - 1, xoffset + 25 + 2 + 3 + 3, 'TRUE', curses.color_pair(2) | (curses.A_REVERSE if instance.job_id_type == 'true' else 0))
    stdscr.addstr(lines - 1, xoffset + 25 + 2 + 3 + 3 + 4, ']', curses.color_pair(2))
    instance.add_button(lines - 1, xoffset + 25 + 2, '[B:AGGTRUE]', BUTTON_ACTIONS['job_id_type'][0])

    stdscr.addstr(lines - 1, xoffset + 37 + 2, '[P:', curses.color_pair(2))
    stdscr.addstr(lines - 1, xoffset + 37 + 2 + 3, 'PRIORITY', curses.color_pair(2) | (curses.A_REVERSE if instance.show_prio else 0))
    stdscr.addstr(lines - 1, xoffset + 37 + 2 + 3 + 8, ']', curses.color_pair(2))
    instance.add_button(lines - 1, xoffset + 37 + 2, '[P:PRIORITY]', BUTTON_ACTIONS['show_prio'][0])

    stdscr.addstr(lines - 1, xoffset + 50 + 2, '[T:', curses.color_pair(2))
    stdscr.addstr(lines - 1, xoffset + 50 + 2 + 3, 'ACCOUNT', curses.color_pair(2) | (curses.A_REVERSE if instance.show_account else 0))
    stdscr.addstr(lines - 1, xoffset + 50 + 2 + 3 + 7, ']', curses.color_pair(2))
    instance.add_button(lines - 1, xoffset + 50 + 2, '[T:ACCOUNT]', BUTTON_ACTIONS['show_account'][0])

    # get slurm user partition
    stdscr.addstr(lines - 1, xoffset + 62 + 2, f'(Avg time {instance.avg_wait_time})', curses.color_pair(2))

    signature = instance.signature if instance.updated else f'{instance.version} -> {instance.newest_version}'
    stdscr.addstr(lines - 1, columns - 2 - len(signature), signature, curses.color_pair(2) if instance.updated else curses.color_pair(3))

    stdscr.refresh()
    curses.doupdate()


def get_char_async(stdscr, instance):
    while instance.k not in BUTTON_ACTIONS['quit']:  # != 'q'
        handle_keys(stdscr, instance)
        update_screen(stdscr, instance)

    # raise SIGINT to cancel update task
    os.kill(os.getpid(), signal.SIGINT)


async def update_screen_info(stdscr, instance):
    while instance.k not in BUTTON_ACTIONS['quit']:
        await instance.fetch()
        instance.log("GOT DATA FROM MASTER")
        update_screen(stdscr, instance)


async def wait_first(futures, instance):
    ''' Return the result of the first future to finish. Cancel the remaining
    futures. '''
    try:
        done, pending = await asyncio.wait(futures,
                                           return_when=asyncio.FIRST_COMPLETED)

        # cancel the other tasks, we have a result. We need to wait for the cancellations
        # to propagate.
        for task in pending:
            task.cancel()
        await asyncio.wait(pending)
    except Exception as e:
        instance.log(e)
        # get trace
        instance.log(traceback.format_exc())


async def main(stdscr):
    # Clear screen
    instance = Singleton.getInstance()
    stdscr.clear()
    curses.noecho()
    curses.curs_set(0)
    # stdscr.timeout(2000)

    stdscr.nodelay(False)

    # stdscr.timeout(int(timedelta_refresh*1000))
    _ = curses.mousemask(1)

    curses.use_default_colors()
    # colors
    curses.init_pair(1, 3, 0)
    # curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_WHITE)
    curses.init_pair(2, -1, -1)
    # curses.init_pair(2, curses.COLOR_WHITE, curses.COLOR_BLACK)

    curses.init_pair(3, 1, -1)
    # curses.init_pair(3, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.init_pair(4, 3, -1)
    # curses.init_pair(4, curses.COLOR_YELLOW, curses.COLOR_BLACK)
    curses.init_pair(5, 2, -1)
    # curses.init_pair(5, curses.COLOR_GREEN, curses.COLOR_BLACK)
    curses.init_pair(6, 5, -1)
    # curses.init_pair(6, curses.COLOR_MAGENTA, curses.COLOR_BLACK)
    curses.init_pair(7, 4, -1)
    # curses.init_pair(7, curses.COLOR_BLUE, curses.COLOR_BLACK)
    curses.init_pair(8, 6, -1)
    # curses.init_pair(8, curses.COLOR_CYAN, curses.COLOR_BLACK)

    curses.init_pair(10, 3, -1)
    # curses.init_pair(10, curses.COLOR_WHITE, curses.COLOR_RED)
    curses.init_pair(11, 3, -1)
    # curses.init_pair(11, curses.COLOR_WHITE, curses.COLOR_GREEN)
    curses.init_pair(12, 3, -1)
    # curses.init_pair(12, curses.COLOR_WHITE, curses.COLOR_YELLOW)
    curses.init_pair(13, 5, -1)
    # curses.init_pair(13, curses.COLOR_WHITE, curses.COLOR_MAGENTA)

    curses.init_pair(15, -1, 6)
    # curses.init_pair(15, curses.COLOR_WHITE, curses.COLOR_CYAN) (reversed because >9)
    # status

    instance.a_filter = 0
    instance.right_width = 33

    stdscr.clear()

    def exit_handler(sig, frame):
        instance.log(f"FORCED EXIT...")
        instance.sock.close()
        exit(0)
    signal.signal(signal.SIGINT, exit_handler)

    # print waiting message un stdscr
    stdscr.addstr(0, 0, "Waiting for data from master...")
    await instance.fetch()

    update_screen(stdscr, instance)
    update_screen(stdscr, instance)  # need 2 for some reason...

    update_task = None

    curses_thread = threading.Thread(target=get_char_async, args=(stdscr, instance))
    curses_thread.daemon = True
    curses_thread.start()

    update_task = asyncio.create_task(update_screen_info(stdscr, instance))
    try:
        await update_task
    except asyncio.exceptions.CancelledError:
        pass
    except Exception as e:
        instance.log(e)
        # get trace
        instance.log(traceback.format_exc())
