from multiprocessing import Pool

import time

work = (["A", 5], ["B", 2], ["C", 1], ["D", 3],["A", 5], ["B", 2], ["C", 1], ["D", 3],["A", 5], ["B", 2], ["C", 1], ["D", 3],["A", 5], ["B", 2], ["C", 1], ["D", 3],["A", 5], ["B", 2], ["C", 1], ["D", 3])


def work_log(work_data):
    print(" Process %s waiting 10 seconds\n" % (work_data))
    time.sleep(int(work_data))
    print(" Process %s Finished.\n" % work_data)


def pool_handler():
    work = []
    for i in range(1,100):
        work.append(i)
    p = Pool(4)
    p.map(work_log, work)


if __name__ == '__main__':
    pool_handler()