#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dataclasses import dataclass
from typing import Union


@dataclass
class Resources:
    ram: int
    cpu_cores: int
    gpu_count: int


@dataclass
class Task:
    id: int
    priority: int
    resources: Resources
    content: str
    result: str


class TaskQueue:
    def __init__(self):
        self.tasks = None
        self.sorted = False
        self.clean()

    def clean(self):
        self.tasks = []

    def add_task(self, task: Task):
        self.tasks.append(task)
        self.sorted = False

    def get_task(self, available_resources: Resources) -> Union[Task, None]:
        if not self.sorted:
            self.tasks.sort(key=lambda x: x.priority, reverse=True)
            self.sorted = True
        t = filter(lambda x: self.check_fit_resources(resources=x.resources,
                                                      available=available_resources),
                   self.tasks)
        try:
            result = next(t)
            self.tasks.remove(result)
        except StopIteration:
            result = None
        return result

    @staticmethod
    def check_fit_resources(resources: Resources, available: Resources) -> bool:
        return (resources.ram <= available.ram) and (resources.cpu_cores <= available.cpu_cores) and (
                resources.gpu_count <= available.gpu_count)


if __name__ == '__main__':
    pass
