import multiprocessing
import subprocess
from prism.config import get_settings

settings = get_settings()
worker_count = settings['workers']


def work(a):
    cmd, arg = a
    return subprocess.Popen([cmd, arg, '-b'])


def main():
    pool = multiprocessing.Pool(processes=worker_count)
    pool.map(work, [('rqworker', 'worker-%s' % (i + 1))
                    for i in range(worker_count)])


if __name__ == '__main__':
    main()
