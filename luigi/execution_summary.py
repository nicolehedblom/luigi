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
            if task.task_family in group_tasks[status]:
                group_tasks[status][task.task_family].append(task)
            else:
                group_tasks[status][task.task_family] = []
                group_tasks[status][task.task_family].append(task)
    return group_tasks


def _get_str(task_dict, count):
    lines = []
    for task_family, tasks in task_dict.items():
        row = '    '
        if count > 3:
            row += '    '
        if len(lines) >= 5:
            row += '...'
            lines.append(row)
            break
        if len(tasks[0].get_params()) == 1:
            row += "- " + str(len(tasks)) + " " + str(task_family) + "(" + tasks[0].get_params()[0][0] + "="
            row += _get_str_one_parameter(tasks)
            row += ")"
        elif len(tasks[0].get_params()) == 0:
            row += "- " + str(len(tasks)) + " " + str(task_family) + "()"
        else:
            row += "- " + str(tasks[0]) + " and " + str(len(tasks) - 1) + " other " + str(task_family)
        lines.append(row)
    return '\n'.join(lines)


def _get_str_one_parameter(tasks):
    row = ''
    count = 0
    for task in tasks:
        if len(row) >= 30 and count > 1:
            row += "..."
            break
        row += _serialize_first_param(task)
        if count < len(tasks) - 1:
            row += ","
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
        comments["already_done"] += " were already done:\n"
    if "completed" in comments:
        comments["completed"] += " ran successfully:\n"
    if "failed" in comments:
        comments["failed"] += " failed:\n"
    still_pending = False
    if "still_pending_ext" in comments:
        comments["still_pending_ext"] = "    " + comments["still_pending_ext"] + " were external dependencies:\n"
        still_pending = True
    if "upstream_run_by_other_worker" in comments:
        comments["upstream_run_by_other_worker"] = "    " + comments["upstream_run_by_other_worker"] + " had dependencies that were being run by other worker:\n"
    if "upstream_failure" in comments:
        comments["upstream_failure"] = "    " + comments["upstream_failure"] + " had failed dependencies:\n"
        still_pending = True
    if "upstream_missing_dependency" in comments:
        comments["upstream_missing_dependency"] = "    " + comments["upstream_missing_dependency"] + " had missing dependencies:\n"
        still_pending = True
    if "run_by_other_worker" in comments:
        comments["run_by_other_worker"] = "    " + comments["run_by_other_worker"] + " were being run by another worker:\n"
        still_pending = True
    if still_pending:
        comments["still_pending"] = "* " + str(_get_number_of_tasks(group_tasks["still_pending_ext"]) + _get_number_of_tasks(group_tasks["still_pending_not_ext"])) + " were left pending:\n"
    return comments


def _get_statuses():
    statuses = ["already_done", "completed", "failed", "still_pending", "still_pending_ext", "upstream_failure", "upstream_missing_dependency", "run_by_other_worker"]
    return statuses


def _summary_dict(worker):
    set_tasks = _partition_tasks(worker)
    for task in set_tasks["still_pending_not_ext"]:
        _dfs(set_tasks, task)
    return set_tasks


def _summary_format(set_tasks, worker):
    group_tasks = _group_tasks_by_name_and_status(set_tasks)
    str_tasks = {}
    comments = _get_comments(group_tasks)
    statuses = _get_statuses()
    num_all_tasks = len(set_tasks["already_done"]) + len(set_tasks["completed"]) + len(set_tasks["failed"]) + len(set_tasks["still_pending_ext"]) + len(set_tasks["still_pending_not_ext"])
    str_output = 'Scheduled ' + str(num_all_tasks) + " tasks of which:\n"
    for i in range(len(statuses)):
        if statuses[i] not in comments:
            continue
        str_output += comments[statuses[i]]
        if i != 3:
            str_output += _get_str(group_tasks[statuses[i]], i) + '\n'
    if num_all_tasks == len(set_tasks["already_done"]) + len(set_tasks["still_pending_ext"]) + len(set_tasks["still_pending_not_ext"]):
        str_output = "Did not run any tasks\n"
    if num_all_tasks == 0:
        str_output = 'Did not schedule any tasks\n'

    # TODO(nicolehedblom): Clean up this mess Arash created :)
    worker_that_blocked_task = dict()
    get_work_response_history = worker._get_work_response_history
    for get_work_response in get_work_response_history:
        if get_work_response['task_id'] is None:
            for running_task in get_work_response['running_tasks']:
                other_worker_id = running_task['worker']
                other_task_id = running_task['task_id']
                other_task = worker._scheduled_tasks.get(other_task_id)
                if other_task:
                    worker_that_blocked_task[other_task] = other_worker_id

    already_running_tasks_with_reasons = set_tasks['run_by_other_worker'].intersection(worker_that_blocked_task.keys())
    for task in already_running_tasks_with_reasons:
        str_output += 'Task {} was blocked by {}\n'.format(str(task), worker_that_blocked_task[task])

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
    return _summary_wrap(_summary_format(_summary_dict(worker), worker))
