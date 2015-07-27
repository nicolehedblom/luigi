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
import mock
from helpers import unittest, LuigiTestCase

from luigi import six

import luigi
import luigi.worker
import luigi.execution_summary
import threading
import datetime


class ExecutionSummaryTest(LuigiTestCase):

    def setUp(self):
        super(ExecutionSummaryTest, self).setUp()
        self.scheduler = luigi.scheduler.CentralPlannerScheduler(prune_on_get_work=False)
        self.worker = luigi.worker.Worker(scheduler=self.scheduler)

    def run_task(self, task):
        self.worker.add(task)  # schedule
        self.worker.run()  # run

    def summary_dict(self):
        return luigi.execution_summary._summary_dict(self.worker)

    def summary(self):
        return luigi.execution_summary.summary(self.worker)

    def test_all_statuses(self):
        class Bar(luigi.Task):
            num = luigi.IntParameter()

            def run(self):
                if self.num == 0:
                    raise ValueError()

            def complete(self):
                if self.num == 1:
                    return True
                return False

        class Foo(luigi.Task):
            def run(self):
                pass

            def requires(self):
                for i in range(5):
                    yield Bar(i)

        self.run_task(Foo())
        d = self.summary_dict()
        self.assertEqual({Bar(num=1)}, d['already_done'])
        self.assertEqual({Bar(num=2), Bar(num=3), Bar(num=4)}, d['completed'])
        self.assertEqual({Bar(num=0)}, d['failed'])
        self.assertEqual({Foo()}, d['upstream_failure'])
        self.assertFalse(d['upstream_missing_dependency'])
        self.assertFalse(d['run_by_other_worker'])
        self.assertFalse(d['still_pending_ext'])

    def test_upstream_not_running(self):
        class ExternalBar(luigi.ExternalTask):
            num = luigi.IntParameter()

            def complete(self):
                if self.num == 1:
                    return True
                return False

        class Bar(luigi.Task):
            num = luigi.IntParameter()

            def run(self):
                if self.num == 0:
                    raise ValueError()

        class Foo(luigi.Task):
            def run(self):
                pass

            def requires(self):
                for i in range(5):
                    yield ExternalBar(i)
                    yield Bar(i)

        self.run_task(Foo())
        d = self.summary_dict()
        self.assertEqual({ExternalBar(num=1)}, d['already_done'])
        self.assertEqual({Bar(num=1), Bar(num=2), Bar(num=3), Bar(num=4)}, d['completed'])
        self.assertEqual({Bar(num=0)}, d['failed'])
        self.assertEqual({Foo()}, d['upstream_failure'])
        self.assertEqual({Foo()}, d['upstream_missing_dependency'])
        self.assertFalse(d['run_by_other_worker'])
        self.assertEqual({ExternalBar(num=0), ExternalBar(num=2), ExternalBar(num=3), ExternalBar(num=4)}, d['still_pending_ext'])

    def test_already_running(self):
        lock1 = threading.Lock()
        lock2 = threading.Lock()

        class ParentTask(luigi.Task):
            def __init__(self, *args, **kwargs):
                super(ParentTask, self).__init__(*args, **kwargs)
                self.comp = False

            def complete(self):
                return self.comp

            def run(self):
                self.comp = True

            def requires(self):
                yield LockTask()

        class LockTask(luigi.Task):
            def __init__(self, *args, **kwargs):
                super(LockTask, self).__init__(*args, **kwargs)
                self.comp = False

            def complete(self):
                return self.comp

            def run(self):
                lock2.release()
                lock1.acquire()
                self.comp = True

        lock1.acquire()
        lock2.acquire()
        other_worker = luigi.worker.Worker(scheduler=self.scheduler, worker_id="my_other_worker")
        other_worker.add(ParentTask())
        t1 = threading.Thread(target=other_worker.run)
        t1.start()
        lock2.acquire()
        self.run_task(ParentTask())
        lock1.release()
        t1.join()
        d = self.summary_dict()
        self.assertEqual({LockTask()}, d['run_by_other_worker'])
        self.assertEqual({ParentTask()}, d['upstream_run_by_other_worker'])
        self.assertIn("my_other_worker", self.summary())

    def test_larger_tree(self):

        class Dog(luigi.Task):
            def __init__(self, *args, **kwargs):
                super(Dog, self).__init__(*args, **kwargs)
                self.comp = False

            def complete(self):
                return self.comp

            def run(self):
                self.comp = True

            def requires(self):
                yield Cat(2)

        class Cat(luigi.Task):
            num = luigi.IntParameter()

            def __init__(self, *args, **kwargs):
                super(Cat, self).__init__(*args, **kwargs)
                self.comp = False

            def run(self):
                if self.num == 2:
                    raise ValueError()
                self.comp = True

            def complete(self):
                if self.num == 1:
                    return True
                else:
                    return self.comp

        class Bar(luigi.Task):
            num = luigi.IntParameter()

            def __init__(self, *args, **kwargs):
                super(Bar, self).__init__(*args, **kwargs)
                self.comp = False

            def complete(self):
                return self.comp

            def run(self):
                self.comp = True

            def requires(self):
                if self.num == 0:
                    yield ExternalBar()
                    yield Cat(0)
                if self.num == 1:
                    yield Cat(0)
                    yield Cat(1)
                if self.num == 2:
                    yield Dog()

        class Foo(luigi.Task):
            def run(self):
                pass

            def requires(self):
                for i in range(3):
                    yield Bar(i)

        class ExternalBar(luigi.ExternalTask):

            def complete(self):
                return False

        self.run_task(Foo())
        d = self.summary_dict()

        self.assertEqual({Cat(num=1)}, d['already_done'])
        self.assertEqual({Cat(num=0), Bar(num=1)}, d['completed'])
        self.assertEqual({Cat(num=2)}, d['failed'])
        self.assertEqual({Dog(), Bar(num=2), Foo()}, d['upstream_failure'])
        self.assertEqual({Bar(num=0), Foo()}, d['upstream_missing_dependency'])
        self.assertFalse(d['run_by_other_worker'])
        self.assertEqual({ExternalBar()}, d['still_pending_ext'])

    def test_with_dates(self):
        """ Just test that it doesn't crash with date params """

        start = datetime.datetime(1998, 3, 23)

        class Bar(luigi.Task):
            date = luigi.DateParameter()

            def __init__(self, *args, **kwargs):
                super(Bar, self).__init__(*args, **kwargs)
                self.comp = False

            def run(self):
                self.comp = True

            def complete(self):
                return self.comp

        class Foo(luigi.Task):

            def run(self):
                pass

            def requires(self):
                for i in range(10):
                    new_date = start + datetime.timedelta(days=i)
                    yield Bar(date=new_date)

        self.run_task(Foo())
        d = self.summary_dict()
        exp_set = {Bar(start + datetime.timedelta(days=i)) for i in range(10)}
        exp_set.add(Foo())
        self.assertEqual(exp_set, d['completed'])
        s = self.summary()
        self.assertIn('date=1998-0', s)
        self.assertIn('Scheduled 11 tasks', s)
        self.assertIn('Luigi Execution Summary', s)
        self.assertNotIn('00:00:00', s)
