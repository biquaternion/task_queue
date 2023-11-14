#!/usr/bin/env python
# -*- coding: utf-8 -*-
import random

import pytest

from task_queue import Resources, Task, TaskQueue


def test_task_queue():
    tasks = [Task(0, 25, Resources(1024, 2, 0), '0', ''),
             Task(1, 100, Resources(512, 4, 0), '1', ''),
             Task(2, 50, Resources(1024, 2, 0), '2', ''),
             Task(3, 15, Resources(9192, 2, 0), '3', ''),
             Task(4, 25, Resources(1024, 16, 2), '4', ''),
             Task(5, 85, Resources(1024, 4, 8), '5', '')]

    available_resources = Resources(ram=4096, cpu_cores=8, gpu_count=2)
    tq = TaskQueue()
    tq.add_task(tasks[0])
    assert len(tq.tasks) == 1
    tq.add_task(tasks[1])
    assert len(tq.tasks) == 2
    task = tq.get_task(available_resources=available_resources)
    assert len(tq.tasks) == 1
    assert task.priority >= max(tq.tasks, key=lambda x: x.priority).priority
    tq.clean()
    assert len(tq.tasks) == 0
    tq.add_task(tasks[3])
    tq.add_task(tasks[4])
    tq.add_task(tasks[5])
    task = tq.get_task(available_resources=available_resources)
    assert task is None


def test_task_queue_2():
    MAX_TASK_NUM = int(1e6)
    rams = [32, 64, 128, 256, 512, 1024, 2048, 4096]
    cpus = list(range(1, 11))
    gpus = list(range(1, 11))
    tasks1 = [Task(id=i,
                   priority=random.randint(20, 30),
                   resources=Resources(512, 2, 1),
                   content=f'{i}',
                   result='') for i in range(1000)]
    tasks2 = [Task(id=i,
                   priority=random.randint(20, 30),
                   resources=Resources(1024, 4, 2),
                   content=f'{i}',
                   result='') for i in range(1000, 2000)]
    tasks3 = [Task(id=i,
                   priority=random.randint(20, 30),
                   resources=Resources(9192, 16, 4),
                   content=f'{i}',
                   result='') for i in range(2000, 3000)]
    tasks4 = [Task(id=i,
                   priority=random.randint(1, 100),
                   resources=Resources(rams[random.randint(0, len(rams) - 1)],
                                       cpu_cores=cpus[random.randint(0, len(cpus) - 1)],
                                       gpu_count=gpus[random.randint(0, len(gpus) - 1)]),
                   content=f'{i}',
                   result='') for i in range(3000, MAX_TASK_NUM)]
    tasks = tasks1 + tasks2 + tasks3
    available_resources = Resources(4096, 8, 2)
    tq = TaskQueue()
    for i in range(1000):
        tq.add_task(tasks[i])
    task = tq.get_task(available_resources=available_resources)
    assert len(tq.tasks) == 999
    assert task.priority >= max(tq.tasks, key=lambda x: x.priority).priority
    for i in range(1000, 2000):
        tq.add_task(tasks[i])
    for i in range(2000, 3000):
        tq.add_task(tasks[i])
    task = tq.get_task(available_resources=available_resources)
    assert len(tq.tasks) == 2998
    assert task.priority >= max(tq.tasks, key=lambda x: x.priority).priority

    none_count = 0
    for i in range(2998):
        task = tq.get_task(available_resources=available_resources)
        if task is None:
            none_count += 1
    assert none_count == 1000

    tasks += tasks4
    for i in range(3000, MAX_TASK_NUM):
        tq.add_task(tasks[i])
    task = tq.get_task(available_resources=available_resources)
    assert len(tq.tasks) == MAX_TASK_NUM - 2001
    assert task.priority >= max(tq.tasks, key=lambda x: x.priority).priority


if __name__ == '__main__':
    pass
