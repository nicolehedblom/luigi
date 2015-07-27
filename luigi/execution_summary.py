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
"""
This module provide the function :py:func:`summary` that is used for printing
an execution summary at the end of luigi invocations. For example:

    .. code:: console

            $ luigi --module examples.execution_summary_example examples.EntryPoint --local-scheduler
            ...
            ... lots of spammy output
            ...
            INFO: There are 11 pending tasks unique to this worker
            INFO: Worker Worker(salt=843361665, workers=1, host=arash-spotify-T440s, username=arash, pid=18534) was stopped. Shutting down Keep-Alive thread
            INFO:
            ===== Luigi Execution Summary =====

            Scheduled 210 tasks of which:
            * 195 were already done:
                - 195 examples.Bar(num=119,42,144,121,160,135,151,89,...)
            * 1 ran successfully:
                - 1 examples.Boom(num=0)
            * 14 were left pending:
                * 1 were external dependencies:
                    - 1 MyExternal()
                * 13 had missing dependencies:
                    - 1 examples.EntryPoint()
                    - examples.Foo(num=100, num2=4) and 9 other examples.Foo
                    - examples.DateTask(date=1998-03-23, num=1) and 1 other examples.DateTask

            ===== Luigi Execution Summary =====
"""

import textwrap
import datetime


def _partition_tasks(worker):
    """
    Takes a worker and sorts out which tasks were completed, already done, failed and still pending.
    Still_pending_not_ext is only used to get upstream_failure, upstream_missing_dependency and run_by_other_worker
    """
    task_history = worker._add_task_history
    pending_tasks = {task for(task, status, ext) in task_history if status == 'PENDING'}
    set_tasks = {}
    set_tasks["completed"] = {task for (task, status, ext) in task_history if status == 'DONE' and task in pending_tasks}
    set_tasks["already_done"] = {task for (task, status, ext) in task_history if status == 'DONE' and task not in pending_tasks and task not in set_tasks["completed"]}
    set_tasks["failed"] = {task for (task, status, ext) in task_history if status == 'FAILED'}
    set_tasks["still_pending_ext"] = {task for (task, status, ext) in task_history if status == 'PENDING' and task not in set_tasks["failed"] and task not in set_tasks["completed"] and not ext}
    set_tasks["still_pending_not_ext"] = {task for (task, status, ext) in task_history if status == 'PENDING' and task not in set_tasks["failed"] and task not in set_tasks["completed"] and ext}
    set_tasks["upstream_failure"] = set()
    set_tasks["upstream_missing_dependency"] = set()
    set_tasks["upstream_run_by_other_worker"] = set()
    set_tasks["run_by_other_worker"] = set()
    return set_tasks


def _dfs(set_tasks, current_task):
    if current_task in set_tasks["still_pending_not_ext"]:
        upstream_failure = False
        upstream_missing_dependency = False
        upstream_run_by_other_worker = False
        for task in current_task.requires():
            _dfs(set_tasks, task)
            if task in set_tasks["failed"] or task in set_tasks["upstream_failure"]:
                set_tasks["upstream_failure"].add(current_task)
                upstream_failure = True
            if task in set_tasks["still_pending_ext"] or task in set_tasks["upstream_missing_dependency"]:
                set_tasks["upstream_missing_dependency"].add(current_task)
                upstream_missing_dependency = True
            if task in set_tasks["run_by_other_worker"] or task in set_tasks["upstream_run_by_other_worker"]:
                set_tasks["upstream_run_by_other_worker"].add(current_task)
                upstream_run_by_other_worker = True
        if not upstream_failure and not upstream_missing_dependency and not upstream_run_by_other_worker:
            set_tasks["run_by_other_worker"].add(current_task)


def _group_tasks_by_name_and_status(set_tasks):
    """
    Takes a dictionary with sets of tasks grouped by their status and returns a dictionary with dictionaries with an array of tasks grouped by their status and task name
    """
    group_tasks = {}
    for status, task_dict in set_tasks.items():
        group_tasks[status] = {}
        for task in task_dict:
            if task.task_family not in group_tasks[status]:
                group_tasks[status][task.task_family] = []
            group_tasks[status][task.task_family].append(task)
    return group_tasks


def _get_str(task_dict, extra_indent):
    lines = []
    for task_family, tasks in task_dict.items():
        row = '    '
        if extra_indent:
            row = '{0}    '.format(row)
        if len(lines) >= 5:
            row = '{0}...'.format(row)
            lines.append(row)
            break
        if len((tasks[0].get_params())) == 1:
            attributes = sorted({getattr(task, tasks[0].get_params()[0][0]) for task in tasks})
            row = '{0}- {1} {2}({3}='.format(row, len(tasks), task_family, tasks[0].get_params()[0][0])
            if _ranging_attributes(attributes) and len(attributes) > 3:
                row = '{0}{1}...{2}'.format(row, tasks[0].get_params()[0][1].serialize(attributes[0]), tasks[0].get_params()[0][1].serialize(attributes[len(attributes) - 1]))
            else:
                row = '{0}{1}'.format(row, _get_str_one_parameter(tasks))
            row += ")"
        elif len(tasks[0].get_params()) == 0:
            row = '{0}- {1} {2}() '.format(row, len(tasks), str(task_family))
        else:
            ranging = False
            params = _get_set_of_params(tasks)
            if _only_one_unique_param(params):
                unique_param = _get_unique_param(params)
                attributes = sorted(params[unique_param])
                if _ranging_attributes(attributes) and len(attributes) > 2:
                    ranging = True
                    row = '{0}- {1}({2}'.format(row, task_family, _get_str_ranging_multiple_parameters(attributes, tasks, unique_param))
            if not ranging:
                if len(tasks) == 1:
                    row = '{0}- {1} {2}'.format(row, len(tasks), tasks[0])
                if len(tasks) == 2:
                    row = '{0}- {1} and {2}'.format(row, tasks[0], tasks[1])
                if len(tasks) > 2:
                    row = '{0}- {1} and {2} other {3}'.format(row, tasks[0], len(tasks) - 1, task_family)
        lines.append(row)
    return '\n'.join(lines)


def _get_str_ranging_multiple_parameters(attributes, tasks, unique_param):
    row = ''
    str_unique_param = '{0}...{1}'.format(unique_param[1].serialize(attributes[0]), unique_param[1].serialize(attributes[len(attributes) - 1]))
    for param in tasks[0].get_params():
        row = '{0}{1}='.format(row, param[0])
        if param[0] == unique_param[0]:
            row = '{0}{1}'.format(row, str_unique_param)
        else:
            row = '{0}{1}'.format(row, getattr(tasks[0], param[0]))
        if param != tasks[0].get_params()[len(tasks[0].get_params()) - 1]:
            row = "{0}, ".format(row)
    row = '{0})'.format(row)
    return row


def _get_set_of_params(tasks):
    params = {}
    for param in tasks[0].get_params():
        params[param] = {getattr(task, param[0]) for task in tasks}
    return params


def _only_one_unique_param(params):
    len_params = len(params)
    different = [1 for param in params if len(params[param]) == 1]
    len_different = len(different)
    if len_params - len_different == 1:
        return True
    else:
        return False


def _get_unique_param(params):
    for param in params:
        if len(params[param]) > 1:
            return param


def _ranging_attributes(attributes):
    ranging = False
    if len(attributes) > 2 and _is_of_enumerable_type(attributes[0]):
        ranging = True
        difference = attributes[1] - attributes[0]
        for i in range(1, len(attributes)):
            if attributes[i] - attributes[i - 1] != difference:
                ranging = False
                break
    return ranging


def _is_of_enumerable_type(value):
    if type(value) == int or type(value) == datetime.datetime or type(value) == datetime.date:
        return True
    else:
        return False


def _get_str_one_parameter(tasks):
    row = ''
    count = 0
    for task in tasks:
        if len(row) >= 30 and count > 1:
            row = '{0}...'.format(row)
            break
        row = '{0}{1}'.format(row, getattr(task, task.get_params()[0][0]))
        if count < len(tasks) - 1:
            row = '{0},'.format(row)
        count += 1
    return row


def _serialize_first_param(task):
    return task.get_params()[0][1].serialize(getattr(task, task.get_params()[0][0]))


def _get_number_of_tasks(task_dict):
    num = 0
    for task_family, tasks in task_dict.items():
        num += len(tasks)
    return num


def _get_comments(group_tasks):
    comments = {}
    for status, task_dict in group_tasks.items():
        comments[status] = "* " + str(_get_number_of_tasks(task_dict))
        if _get_number_of_tasks(task_dict) == 0:
            comments.pop(status)
    if "already_done" in comments:
        comments["already_done"] = '{0} were already done:\n'.format(comments['already_done'])
    if "completed" in comments:
        comments["completed"] = '{0} ran successfully:\n'.format(comments['completed'])
    if "failed" in comments:
        comments["failed"] = '{0} failed:\n'.format(comments['failed'])
    still_pending = False
    if "still_pending_ext" in comments:
        comments["still_pending_ext"] = '    {0} were external dependencies:\n'.format(comments['still_pending_ext'])
        still_pending = True
    if "upstream_run_by_other_worker" in comments:
        comments["upstream_run_by_other_worker"] = '    {0} had dependencies that were being run by other worker:\n'.format(comments['upstream_run_by_other_worker'])
    if "upstream_failure" in comments:
        comments["upstream_failure"] = '    {0} had failed dependencies:\n'.format(comments['upstream_failure'])
        still_pending = True
    if "upstream_missing_dependency" in comments:
        comments["upstream_missing_dependency"] = '    {0} had missing dependencies:\n'.format(comments['upstream_missing_dependency'])
        still_pending = True
    if "run_by_other_worker" in comments:
        comments["run_by_other_worker"] = '    {0} were being run by another worker:\n'.format(comments['run_by_other_worker'])
        still_pending = True
    if still_pending:
        comments["still_pending"] = '* {0} were left pending:\n'.format(_get_number_of_tasks(group_tasks["still_pending_ext"]) + _get_number_of_tasks(group_tasks["still_pending_not_ext"]))
    return comments


def _get_statuses():
    statuses = ["already_done", "completed", "failed", "still_pending", "still_pending_ext", "upstream_failure", "upstream_missing_dependency", "run_by_other_worker"]
    return statuses


def _summary_dict(worker):
    set_tasks = _partition_tasks(worker)
    for task in set_tasks["still_pending_not_ext"]:
        _dfs(set_tasks, task)
    return set_tasks


def _summary_format(set_tasks):
    group_tasks = _group_tasks_by_name_and_status(set_tasks)
    str_tasks = {}
    comments = _get_comments(group_tasks)
    statuses = _get_statuses()
    num_all_tasks = len(set_tasks["already_done"]) + len(set_tasks["completed"]) + len(set_tasks["failed"]) + len(set_tasks["still_pending_ext"]) + len(set_tasks["still_pending_not_ext"])
    str_output = 'Scheduled {0} tasks of which:\n'.format(num_all_tasks)
    for i in range(len(statuses)):
        if statuses[i] not in comments:
            continue
        str_output = '{0}{1}'.format(str_output, comments[statuses[i]])
        if statuses[i] != 'still_pending':
            str_output = '{0}{1}\n'.format(str_output, _get_str(group_tasks[statuses[i]], i>3))
    if num_all_tasks == len(set_tasks["already_done"]) + len(set_tasks["still_pending_ext"]) + len(set_tasks["still_pending_not_ext"]):
        str_output = '{0}Did not run any tasks\n'.format(str_output)
    if num_all_tasks == 0:
        str_output = 'Did not schedule any tasks\n'
    return str_output


def _summary_wrap(str_output):
    return textwrap.dedent("""
    ===== Luigi Execution Summary =====

    {str_output}
    ===== Luigi Execution Summary =====
    """).format(str_output=str_output)


def summary(worker):
    """
    Given a worker, return a human readable string describing roughly what the
    workers have done.
    """
    return _summary_wrap(_summary_format(_summary_dict(worker)))
