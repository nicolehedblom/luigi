# -*- coding: utf-8 -*-
#
# Copyright 2015-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function
import os
import shutil
import time
import datetime

import luigi


class MyExternal(luigi.ExternalTask):

    def complete(self):
        return False


class Boom(luigi.Task):
    task_namespace = 'examples'
    num = luigi.IntParameter()

    def run(self):
        print("Running Boom")

    def requires(self):
        for i in range(5, 200):
            yield Bar(i)


class Foo(luigi.Task):
    task_namespace = 'examples'
    num = luigi.IntParameter()
    num2 = luigi.IntParameter()

    def run(self):
        print("Running Foo")

    def requires(self):
        yield MyExternal()
        for i in range(1):
            yield Boom(i)


class Bar(luigi.Task):
    task_namespace = 'examples'
    num = luigi.IntParameter()

    def run(self):
        self.output().open('w').close()

    def output(self):
        """
        Returns the target output for this task.

        :return: the target output for this task.
        :rtype: object (:py:class:`~luigi.target.Target`)
        """
        return luigi.LocalTarget('/tmp/bar/%d' % self.num)


class DateTask(luigi.Task):
    task_namespace = 'examples'
    date = luigi.DateParameter()
    num = luigi.IntParameter()

    def run(self):
        print("Running Foo")

    def requires(self):
        yield MyExternal()
        for i in range(1):
            yield Boom(i)


class EntryPoint(luigi.Task):
    task_namespace = 'examples'

    def run(self):
        print("Running EntryPoint")

    def requires(self):
        for i in range(10):
            yield Foo(100, i)
        for i in range(2):
            yield DateTask(datetime.datetime(1998, 3, 23), i)
