# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Cloud Tasks backend integration for Taskqueue SDK."""

import datetime
import os
import urllib.request
from google.api_core import exceptions as google_exceptions
from google.appengine.api import app_identity
from google.cloud import tasks_v2beta3
from google.protobuf.timestamp_pb2 import Timestamp


class _DummyRPC(object):
  """A dummy RPC object to wrap synchronous calls for async compatibility."""

  def __init__(self, result_provider):
    self._result_provider = result_provider

  def get_result(self):
    return self._result_provider()

  def wait(self):
    pass

  def check_success(self):
    pass


def _get_region():
  """Determines the App Engine region."""
  region = os.environ.get('GAE_REGION')
  if region:
    return region

  try:
    req = urllib.request.Request(
        'http://metadata.google.internal/computeMetadata/v1/instance/zone',
        headers={'Metadata-Flavor': 'Google'}
    )
    with urllib.request.urlopen(req, timeout=1) as response:
      zone = response.read().decode('utf-8')
      if '/' in zone:
        zone = zone.split('/')[-1]
      region = zone.rsplit('-', 1)[0]
      return region
  except Exception:
    pass

  # Fallback to us-central1 if we can't detect it
  return 'us-central1'


def create_task_in_cloud_tasks(queue_name, task, multiple):
  """Calls Cloud Tasks API to create a task."""
  client = tasks_v2beta3.CloudTasksClient()
  project = os.environ.get('GOOGLE_CLOUD_PROJECT')
  if project and (project.startswith('s~') or project.startswith('e~')):
    project = project[2:]
  region = _get_region()

  parent = client.queue_path(project, region, queue_name)

  # Construct the target URL using HTTP request to bypass GAE routing bug
  default_hostname = app_identity.get_default_version_hostname()
  target_service = task.target or os.environ.get('GAE_SERVICE')

  # Workaround for SDK bug that extracts service name with trailing '-dot'
  if target_service and target_service.endswith('-dot'):
    target_service = target_service[:-4]

  if target_service and target_service != 'default':
    url_host = f"{target_service}-dot-{default_hostname}"
  else:
    url_host = default_hostname

  # Ensure task.url starts with /
  relative_uri = task.url or '/'
  if not relative_uri.startswith('/'):
    relative_uri = '/' + relative_uri

  url = f"https://{url_host}{relative_uri}"
  print(f"Jetski: Constructed Cloud Tasks URL (refactored): {url}", flush=True)

  http_method = tasks_v2beta3.HttpMethod.POST
  if task.method:
    method_map = {
        'POST': tasks_v2beta3.HttpMethod.POST,
        'GET': tasks_v2beta3.HttpMethod.GET,
        'PUT': tasks_v2beta3.HttpMethod.PUT,
        'DELETE': tasks_v2beta3.HttpMethod.DELETE,
        'HEAD': tasks_v2beta3.HttpMethod.HEAD,
    }
    http_method = method_map.get(task.method, tasks_v2beta3.HttpMethod.POST)

  headers = {}
  if task.headers:
    headers = dict(task.headers)

  # Manually inject GAE headers for compatibility with the test app
  headers['X-AppEngine-QueueName'] = queue_name
  if task.name:
    headers['X-AppEngine-TaskName'] = task.name

  body = b''
  if task.payload:
    if isinstance(task.payload, str):
      body = task.payload.encode('utf-8')
    else:
      body = task.payload

  # Construct AppEngineHttpRequest
  app_engine_http_request = {
      'http_method': http_method,
      'relative_uri': task.url or '/',
      'body': body,
      'headers': headers,
  }

  routing = {}
  if target_service:
    routing['service'] = target_service
    # Also set version to see if it bypasses the regional routing bug
    version = os.environ.get('GAE_VERSION')
    if version:
      routing['version'] = version
    print(
        f"Jetski: Using AppEngineHttpRequest with routing (refactored):"
        f" service={target_service}, version={version}",
        flush=True,
    )
  else:
    print(
        'Jetski: Using AppEngineHttpRequest with default routing'
        ' (refactored)',
        flush=True,
    )

  if routing:
    app_engine_http_request['app_engine_routing'] = routing

  ct_task = {'app_engine_http_request': app_engine_http_request}

  if task.name:
    ct_task['name'] = client.task_path(project, region, queue_name, task.name)

  if task.retry_options:
    import logging
    logging.warning(
        "Jetski: Per-task retry_options are ignored by the CLOUD_TASK backend. "
        "Please configure retry settings at the queue level instead."
    )

  if task.eta:
    epoch = datetime.datetime.utcfromtimestamp(0)
    eta = task.eta
    if eta.tzinfo is not None:
      eta = eta.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    delta = eta - epoch
    seconds = int(delta.total_seconds())
    nanos = int(delta.microseconds * 1000)
    timestamp = Timestamp(seconds=seconds, nanos=nanos)
    ct_task['schedule_time'] = timestamp

  try:
    response = client.create_task(request={'parent': parent, 'task': ct_task})
    task_id = response.name.split('/')[-1]
    task._Task__name = task_id
    task._Task__queue_name = queue_name
    task._Task__enqueued = True
    if multiple:
      return [task]
    else:
      return task
  except google_exceptions.AlreadyExists as e:
    from google.appengine.api.taskqueue.taskqueue import TaskAlreadyExistsError
    raise TaskAlreadyExistsError(str(e))
  except Exception as e:
    raise e


def purge_queue_in_cloud_tasks(queue_name):
  """Purges all tasks in a queue using Cloud Tasks API."""
  client = tasks_v2beta3.CloudTasksClient()
  project = os.environ.get('GOOGLE_CLOUD_PROJECT')
  if project and (project.startswith('s~') or project.startswith('e~')):
    project = project[2:]
  region = _get_region()

  name = client.queue_path(project, region, queue_name)
  try:
    client.purge_queue(request={'name': name})
    print(
        f"Jetski: Successfully purged queue {queue_name} using Cloud Tasks",
        flush=True,
    )
  except Exception as e:
    raise e


def delete_tasks_in_cloud_tasks(queue_name, tasks, multiple):
  """Deletes tasks from a queue using Cloud Tasks API."""
  client = tasks_v2beta3.CloudTasksClient()
  project = os.environ.get('GOOGLE_CLOUD_PROJECT')
  if project and (project.startswith('s~') or project.startswith('e~')):
    project = project[2:]
  region = _get_region()

  # Check pre-conditions (duplicate names or already deleted)
  task_names = set()
  for task in tasks:
    if not task.name:
      from google.appengine.api.taskqueue.taskqueue import BadTaskStateError
      raise BadTaskStateError('A task name must be specified for a task')
    if task.was_deleted:
      from google.appengine.api.taskqueue.taskqueue import BadTaskStateError
      raise BadTaskStateError(
          'The task %s has already been deleted' % task.name
      )
    if task.name in task_names:
      from google.appengine.api.taskqueue.taskqueue import DuplicateTaskNameError
      raise DuplicateTaskNameError(
          'The task name %s is duplicated' % task.name
      )
    task_names.add(task.name)

  exception = None
  for task in tasks:
    name = client.task_path(project, region, queue_name, task.name)
    try:
      client.delete_task(request={'name': name})
      task._Task__deleted = True
      print(
          f"Jetski: Successfully deleted task {task.name} using Cloud Tasks",
          flush=True,
      )
    except google_exceptions.NotFound:
      # Already deleted or completed, corresponding to UNKNOWN_TASK/TOMBSTONED_TASK
      task._Task__deleted = False
      print(
          f"Jetski: Task {task.name} not found (already processed/deleted)"
          " during deletion",
          flush=True,
      )
    except Exception as e:
      if exception is None:
        exception = e

  if exception is not None:
    raise exception

  if multiple:
    return tasks
  else:
    return tasks[0]

