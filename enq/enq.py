import os, sys, time
import subprocess
from pathlib import Path
from filelock import FileLock
import json
import torch
import shutil
import copy

queue_file_path = Path(os.path.expanduser("~/.enq.json"))
lock_file_path = Path(os.path.expanduser("~/.enq.json.lock"))
flag = "--bg1234#$"
interval = 2


def pop_queue():
    lock = FileLock(lock_file_path)
    lock.acquire()
    with open(queue_file_path, "r") as f:
        data: dict = json.load(f)
    queue = data["queue"]
    if len(queue) == 0:
        return None, 0

    n = queue.pop(0)
    with open(queue_file_path, "w") as f:
        f.write(json.dumps(data, indent=4))
    lock.release()
    return n, len(queue)


def try_to_enqueue():
    if len(sys.argv) < 2:
        return

    lock = FileLock(lock_file_path)
    lock.acquire()
    with open(queue_file_path, "r") as f:
        data: dict = json.load(f)
    queue = data["queue"]
    id = data["last_id"]
    element = {
        "args": sys.argv[1:],
        "id": id,
    }
    data["last_id"] += 1
    queue.append(element)
    with open(queue_file_path, "w") as f:
        f.write(json.dumps(data, indent=4))
    lock.release()

    print(f"enqueued id {id}")
    return


def reset():
    try:
        os.unlink(lock_file_path)
        os.unlink(queue_file_path)
    except:
        pass


if flag not in sys.argv:
    sys.argv.append(flag)
    sys.argv = [sys.executable] + sys.argv
    p = subprocess.Popen(
        sys.argv,
        cwd=os.getcwd(),
        start_new_session=True,
    )
    exit(0)

assert flag in sys.argv
sys.argv.remove(flag)
if not lock_file_path.exists() or not queue_file_path.exists():
    reset()

    with open(lock_file_path, "w") as f:
        f.write("lock")
    with open(queue_file_path, "w") as f:
        f.write(json.dumps({"last_id": 0, "queue": []}))
else:
    try_to_enqueue()
    exit(0)

try_to_enqueue()


class Manager:
    def __init__(self) -> None:
        self._num_devices = torch.cuda.device_count()
        # self._num_devices = 2
        if self._num_devices == 0:
            raise Exception("No gpus found")

        self._available = [True for _ in range(self._num_devices)]
        self._running = [None for _ in range(self._num_devices)]

        print(f"Initializing queue for {self._num_devices} devices pid: {os.getpid()}")

    def available_index(self):
        if True in self._available:
            return self._available.index(True)
        return None

    def launch_proc(self, idx, element):
        self._available[idx] = False
        args = element["args"]
        id = element["id"]
        assert self._running[idx] == None
        args[0] = shutil.which(args[0])
        print(f"running id {id}:{args} on {idx}")
        env = copy.deepcopy(os.environ)
        env["CUDA_VISIBLE_DEVICES"] = f"{idx}"
        p = subprocess.Popen(args, cwd=os.getcwd(), start_new_session=True, env=env)
        self._running[idx] = {"proc": p, "args": args, "id": id}

    def poll_processes(self):
        for i, proc_dict in enumerate(self._running):
            if proc_dict is None:
                continue
            ret_code = proc_dict["proc"].poll()
            is_running = ret_code is None
            if is_running:
                continue
            print(f'id {proc_dict["id"]} finished running, returned {ret_code}')
            self._available[i] = True
            self._running[i] = None

    def run(self) -> None:
        while True:
            self.poll_processes()
            available_idx = self.available_index()
            if available_idx is None:
                time.sleep(interval)
                continue

            nxt, n_left_in_queue = pop_queue()
            if nxt is None:
                print("queue is empty")
                reset()
                return
            self.launch_proc(available_idx, nxt)
            if self.available_index() is not None and n_left_in_queue > 0:
                continue
            time.sleep(interval)


try:
    m = Manager()

    m.run()
finally:
    reset()
