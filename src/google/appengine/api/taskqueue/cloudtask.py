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

import base64
import datetime
import json
import os
import urllib.error
import urllib.request
from google.api_core import exceptions as google_exceptions
from google.appengine.api import app_identity
from google.cloud import tasks_v2beta3
from google.protobuf import json_format
from google.protobuf.timestamp_pb2 import Timestamp
import google.auth
from google.auth.transport.requests import Request


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
  region = os.environ.get('LOCATION_ID') or os.environ.get('GAE_LOCATION') or os.environ.get('GAE_REGION')
  if region:
    return region

  try:
    req = urllib.request.Request(
        'http://metadata.google.internal/computeMetadata/v1/instance/region',
        headers={'Metadata-Flavor': 'Google'}
    )
    with urllib.request.urlopen(req, timeout=2) as response:
      region_path = response.read().decode('utf-8')
      return region_path.split('/')[-1]
  except Exception:
    pass

  # Fallback to us-central1 if we can't detect it
  return os.environ.get('LOCAL_GCP_REGION', 'us-central1')


def _to_duration(seconds):
  if seconds is None:
    return None
  from google.protobuf.duration_pb2 import Duration
  duration = Duration()
  duration.seconds = int(seconds)
  duration.nanos = int((seconds - duration.seconds) * 1e9)
  return duration


def _build_retry_config(retry_options):
  if not retry_options:
    return None

  config = {}

  if retry_options.task_retry_limit is not None:
    config['max_attempts'] = retry_options.task_retry_limit + 1
  if retry_options.task_age_limit is not None:
    config['max_retry_duration'] = _to_duration(retry_options.task_age_limit)
  if retry_options.min_backoff_seconds is not None:
    config['min_backoff'] = _to_duration(retry_options.min_backoff_seconds)
  if retry_options.max_backoff_seconds is not None:
    config['max_backoff'] = _to_duration(retry_options.max_backoff_seconds)
  if retry_options.max_doublings is not None:
    config['max_doublings'] = retry_options.max_doublings

  if config:
    return config
  return None


def _build_ct_task_payload(queue_name, task, client, project, region):
  """Builds the Cloud Tasks Task proto payload from GAE Task."""
  default_hostname = app_identity.get_default_version_hostname()
  target_val = task.target if isinstance(task.target, str) else None
  target_service = target_val or os.environ.get('GAE_SERVICE')

  # Workaround for SDK bug that extracts service name with trailing '-dot'
  if target_service and target_service.endswith('-dot'):
    target_service = target_service[:-4]

  headers = {}
  if task.headers:
    headers = dict(task.headers)

  headers['X-AppEngine-QueueName'] = queue_name
  if task.name:
    headers['X-AppEngine-TaskName'] = task.name

  body = b''
  if task.payload:
    if isinstance(task.payload, str):
      body = task.payload.encode('utf-8')
    else:
      body = task.payload

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

  app_engine_http_request = {
      'http_method': http_method,
      'relative_uri': task.url or '/',
      'body': body,
      'headers': headers,
  }

  routing = {}
  if target_service:
    routing['service'] = target_service
    version = os.environ.get('GAE_VERSION')
    if version:
      routing['version'] = version

  if routing:
    app_engine_http_request['app_engine_routing'] = routing

  ct_task = {'app_engine_http_request': app_engine_http_request}

  if task.name:
    ct_task['name'] = client.task_path(project, region, queue_name, task.name)

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

  if task.retry_options:
    retry_config = _build_retry_config(task.retry_options)
    if retry_config:
      ct_task['retry_config'] = retry_config

  return ct_task


def _get_auth_headers():
  """Generates pure HTTP authentication headers via ADC."""
  credentials, _ = google.auth.default()
  if not credentials.valid:
    credentials.refresh(Request())
  return {
      'Authorization': f'Bearer {credentials.token}',
      'Content-Type': 'application/json'
  }


def _convert_to_rest_payload(ct_task):
  """Converts a hybrid proto-dict task to a pure JSON dict for REST API."""
  rest_task = {}

  if 'name' in ct_task:
    rest_task['name'] = ct_task['name']

  if 'app_engine_http_request' in ct_task:
    ae_req = ct_task['app_engine_http_request']
    rest_ae_req = {}

    method_val = ae_req.get('http_method', tasks_v2beta3.HttpMethod.POST)
    if hasattr(method_val, 'name'):
      rest_ae_req['http_method'] = method_val.name
    elif isinstance(method_val, int):
      rest_ae_req['http_method'] = tasks_v2beta3.HttpMethod(method_val).name
    else:
      rest_ae_req['http_method'] = str(method_val)

    rest_ae_req['relative_uri'] = ae_req.get('relative_uri', '/')

    body_bytes = ae_req.get('body', b'')
    if body_bytes:
      rest_ae_req['body'] = base64.b64encode(body_bytes).decode('utf-8')

    if 'headers' in ae_req:
      rest_ae_req['headers'] = ae_req['headers']

    if 'app_engine_routing' in ae_req:
      rest_ae_req['app_engine_routing'] = ae_req['app_engine_routing']

    rest_task['app_engine_http_request'] = rest_ae_req

  if 'schedule_time' in ct_task:
    rest_task['schedule_time'] = json_format.MessageToDict(
        ct_task['schedule_time']
    )

  if 'retry_config' in ct_task:
    rc = ct_task['retry_config']
    rest_rc = {}
    if 'max_attempts' in rc:
      rest_rc['max_attempts'] = rc['max_attempts']
    if 'max_doublings' in rc:
      rest_rc['max_doublings'] = rc['max_doublings']

    for duration_field in ['max_retry_duration', 'min_backoff', 'max_backoff']:
      if duration_field in rc and rc[duration_field]:
        rest_rc[duration_field] = json_format.MessageToDict(rc[duration_field])

    rest_task['retry_config'] = rest_rc

  return rest_task


def _create_single_task_in_cloud_tasks(queue_name, task, multiple):
  """Helper to create a single task using CloudTasksClient CreateTask API."""
  client = tasks_v2beta3.CloudTasksClient()
  project = os.environ.get('GOOGLE_CLOUD_PROJECT')
  if project and (project.startswith('s~') or project.startswith('e~')):
    project = project[2:]
  region = _get_region()

  parent = client.queue_path(project, region, queue_name)
  ct_task = _build_ct_task_payload(queue_name, task, client, project, region)

  try:
    response_task = client.create_task(request={'parent': parent, 'task': ct_task})
    task_id = response_task.name.split('/')[-1]
    task._Task__name = task_id
    task._Task__queue_name = queue_name
    task._Task__enqueued = True
    if multiple:
      return [task]
    else:
      return task
  except (google_exceptions.AlreadyExists, google_exceptions.Conflict) as e:
    from google.appengine.api.taskqueue.taskqueue import TaskAlreadyExistsError
    raise TaskAlreadyExistsError(str(e))
  except google_exceptions.NotFound as e:
    from google.appengine.api.taskqueue.taskqueue import UnknownQueueError
    raise UnknownQueueError(str(e))
  except google_exceptions.BadRequest as e:
    if 'Queue does not exist' in str(e):
      from google.appengine.api.taskqueue.taskqueue import UnknownQueueError
      raise UnknownQueueError(str(e))
    raise e
  except Exception as e:
    raise e


def _create_batch_tasks_in_cloud_tasks(queue_name, tasks, multiple):
  """Helper to create tasks in batches of up to 100 using CloudTasksClient BatchCreateTasks API."""
  client = tasks_v2beta3.CloudTasksClient()
  project = os.environ.get('GOOGLE_CLOUD_PROJECT')
  if project and (project.startswith('s~') or project.startswith('e~')):
    project = project[2:]
  region = _get_region()

  parent = client.queue_path(project, region, queue_name)

  # Check pre-conditions
  task_names = set()
  for task in tasks:
    if task.name:
      if task.name in task_names:
        from google.appengine.api.taskqueue.taskqueue import DuplicateTaskNameError
        raise DuplicateTaskNameError(
            'The task name %s is duplicated' % task.name
        )
      task_names.add(task.name)

  created_tasks = []
  chunk_size = 100
  for i in range(0, len(tasks), chunk_size):
    batch = tasks[i : i + chunk_size]
    requests_payload = []
    for t in batch:
      ct_task_payload = _build_ct_task_payload(
          queue_name, t, client, project, region
      )
      requests_payload.append({'parent': parent, 'task': ct_task_payload})

    try:
      op = client.batch_create_tasks(
          request={'parent': parent, 'requests': requests_payload}
      )
      response_tasks = op.response.tasks if hasattr(op, 'response') and hasattr(op.response, 'tasks') else getattr(op, 'tasks', [])
      for t, res_task in zip(batch, response_tasks):
        task_id = res_task.name.split('/')[-1] if hasattr(res_task, 'name') else res_task['name'].split('/')[-1]
        t._Task__name = task_id
        t._Task__queue_name = queue_name
        t._Task__enqueued = True
        created_tasks.append(t)
    except (google_exceptions.AlreadyExists, google_exceptions.Conflict) as e:
      from google.appengine.api.taskqueue.taskqueue import TaskAlreadyExistsError
      raise TaskAlreadyExistsError(str(e))
    except google_exceptions.NotFound as e:
      from google.appengine.api.taskqueue.taskqueue import UnknownQueueError
      raise UnknownQueueError(str(e))
    except google_exceptions.BadRequest as e:
      if 'Queue does not exist' in str(e):
        from google.appengine.api.taskqueue.taskqueue import UnknownQueueError
        raise UnknownQueueError(str(e))
      raise e
    except Exception as e:
      raise e

  if multiple:
    return created_tasks
  else:
    return created_tasks[0]


def create_tasks_in_cloud_tasks(queue_name, tasks, multiple):
  """Creates one or more tasks using Cloud Tasks API (supporting BatchCreateTasks)."""
  if len(tasks) == 1:
    return _create_single_task_in_cloud_tasks(queue_name, tasks[0], multiple)
  else:
    return _create_batch_tasks_in_cloud_tasks(queue_name, tasks, multiple)


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


def _map_rest_code_to_tq_code(code):
  if code in [5, 404]:
    return 14  # UNKNOWN_TASK
  if code in [3, 400]:
    return 5   # INVALID_TASK_NAME
  if code in [6, 409]:
    return 10  # TASK_ALREADY_EXISTS
  if code in [7, 403]:
    return 9   # PERMISSION_DENIED
  return 3     # INTERNAL_ERROR


def delete_tasks_in_cloud_tasks(queue_name, tasks, multiple):
  """Deletes tasks from a queue using Cloud Tasks Client SDK (supporting BatchDeleteTasks)."""
  client = tasks_v2beta3.CloudTasksClient()
  project = os.environ.get('GOOGLE_CLOUD_PROJECT')
  if project and (project.startswith('s~') or project.startswith('e~')):
    project = project[2:]
  region = _get_region()

  parent = client.queue_path(project, region, queue_name)

  # Check pre-conditions (duplicate names or already deleted)
  task_names_set = set()
  for task in tasks:
    if not task.name:
      from google.appengine.api.taskqueue.taskqueue import BadTaskStateError
      raise BadTaskStateError('A task name must be specified for a task')
    if task.was_deleted:
      from google.appengine.api.taskqueue.taskqueue import BadTaskStateError
      raise BadTaskStateError(
          'The task %s has already been deleted' % task.name
      )
    if task.name in task_names_set:
      from google.appengine.api.taskqueue.taskqueue import DuplicateTaskNameError
      raise DuplicateTaskNameError(
          'The task name %s is duplicated' % task.name
      )
    task_names_set.add(task.name)

  chunk_size = 1000
  for i in range(0, len(tasks), chunk_size):
    batch = tasks[i : i + chunk_size]
    task_names = [
        client.task_path(project, region, queue_name, t.name) for t in batch
    ]

    try:
      op = client.batch_delete_tasks(
          request={'parent': parent, 'names': task_names}
      )
      metadata = getattr(op, 'metadata', {})
      failed_requests = getattr(metadata, 'failed_requests', getattr(metadata, 'failedRequests', {}))

      from google.appengine.api.taskqueue.taskqueue import _TranslateError

      exception = None
      for idx, t in enumerate(batch):
        error_status = failed_requests.get(idx) or failed_requests.get(str(idx))
        if error_status:
          code = getattr(error_status, 'code', None)
          tq_code = _map_rest_code_to_tq_code(code)
          if tq_code in [14, 11]:
            t._Task__deleted = False
          elif exception is None:
            exception = _TranslateError(tq_code)
        else:
          t._Task__deleted = True

      if exception is not None:
        raise exception
    except Exception as e:
      raise e

  if multiple:
    return tasks
  else:
    return tasks[0]


def build_rest_payload_for_transactional_task(queue_name, task):
  """Builds the REST task payload for a transactional task."""
  client = tasks_v2beta3.CloudTasksClient()
  project = os.environ.get('GOOGLE_CLOUD_PROJECT')
  if project and (project.startswith('s~') or project.startswith('e~')):
    project = project[2:]
  region = _get_region()

  ct_task = _build_ct_task_payload(queue_name, task, client, project, region)
  return _convert_to_rest_payload(ct_task)


def dispatch_rest_task(queue_name, task_payload):
  """Dispatches a pre-built REST task payload immediately."""
  project = os.environ.get('GOOGLE_CLOUD_PROJECT')
  if project and (project.startswith('s~') or project.startswith('e~')):
    project = project[2:]
  region = _get_region()
  _execute_rest_create_task(project, region, queue_name, task_payload)


import threading
import logging
from google.appengine.api import datastore

_transaction_pending_keys = threading.local()


def _get_tx_pending():
  return getattr(_transaction_pending_keys, 'keys', None)


def _start_tx_context():
  prev = getattr(_transaction_pending_keys, 'keys', None)
  _transaction_pending_keys.keys = []
  return prev


def _restore_tx_context(prev):
  _transaction_pending_keys.keys = prev


import contextlib


@contextlib.contextmanager
def _use_default_datastore_adapter():
  conn = datastore._GetConnection()
  orig_adapter = getattr(conn, '_BaseConnection__adapter', None)
  if orig_adapter is not None:
    conn._BaseConnection__adapter = datastore._adapter
    try:
      yield
    finally:
      conn._BaseConnection__adapter = orig_adapter
  else:
    yield


def _dispatch_pending_keys_now(pending_keys, handled_by_sweeper=False):
  try:
    with _use_default_datastore_adapter():
      entities = datastore.Get(pending_keys)
  except Exception as e:
    logging.error("Failed to fetch pending transactional tasks: %s", e)
    return

  now = datetime.datetime.utcnow()
  for entity in entities:
    if not entity:
      continue
    queue_name = entity.get('queue_name')
    payload_str = entity.get('cloud_task_payload')
    task_name = entity.get('cloud_task_name')

    if not payload_str or not queue_name:
      continue

    # Unconditionally acquire transactional lease before dispatching
    try:
      entity['status'] = 'PROCESSING'
      entity['lock_expires'] = now + datetime.timedelta(seconds=60)
      entity['handled_by_sweeper'] = handled_by_sweeper
      with _use_default_datastore_adapter():
        datastore.Put(entity)
    except Exception as e:
      logging.warning("Failed to acquire lock for task %s: %s", task_name, e)
      continue

    try:
      payload = json.loads(payload_str)
      dispatch_rest_task(queue_name, payload)
      with _use_default_datastore_adapter():
        datastore.Delete(entity.key())
      logging.info("Successfully dispatched transactional task %s", task_name)
    except (google_exceptions.AlreadyExists, google_exceptions.Conflict):
      with _use_default_datastore_adapter():
        datastore.Delete(entity.key())
      logging.info("Transactional task %s already exists in Cloud Tasks; cleaned up entity", task_name)
    except Exception as e:
      logging.error(
          "Failed to dispatch transactional task %s: %s", task_name, e
      )
      retry_count = entity.get('retry_count', 0) + 1
      entity['retry_count'] = retry_count
      entity['last_error'] = str(e)[:500]
      if retry_count >= 5:
        entity['status'] = 'FAILED'
        entity['lock_expires'] = None
      else:
        entity['status'] = 'PENDING'
        entity['lock_expires'] = None
      try:
        with _use_default_datastore_adapter():
          datastore.Put(entity)
      except Exception as put_err:
        logging.error("Failed to record error state for task %s: %s", task_name, put_err)


def _register_post_commit_dispatch(queue_name, pending_keys):
  try:
    from google.appengine.ext import ndb

    if ndb.in_transaction():
      ndb.get_context().call_on_commit(
          lambda: _dispatch_pending_keys_now(pending_keys)
      )
      return
  except ImportError:
    pass

  tx_pending = _get_tx_pending()
  if tx_pending is not None:
    tx_pending.extend(pending_keys)
  else:
    from google.appengine.api.taskqueue.taskqueue import BadTransactionStateError

    raise BadTransactionStateError(
        'Transactional tasks must be added inside a transaction.'
    )


def _patched_RunInTransaction(function, *args, **kwargs):
  prev_context = _start_tx_context()
  try:
    result = _original_RunInTransaction(function, *args, **kwargs)
    pending_keys = _get_tx_pending()
    if pending_keys:
      _dispatch_pending_keys_now(pending_keys)
    return result
  finally:
    _restore_tx_context(prev_context)


_original_RunInTransaction = datastore.RunInTransaction
datastore.RunInTransaction = _patched_RunInTransaction


def add_transactional_tasks(queue_name, tasks, multiple):
  """Enqueues transactional tasks into Datastore outbox."""
  # Generate names for unnamed tasks so they are immediately available
  for t in tasks:
    if not t.name:
      import uuid

      t._Task__name = "task-" + str(uuid.uuid4())

  rest_tasks = []
  for t in tasks:
    ct_payload = build_rest_payload_for_transactional_task(queue_name, t)
    rest_tasks.append((t.name, ct_payload))

  pending_keys = []
  for t_name, payload in rest_tasks:
    entity = datastore.Entity('_AE_PendingCloudTask')
    entity['queue_name'] = queue_name
    entity['cloud_task_name'] = t_name
    entity['cloud_task_payload'] = json.dumps(payload)
    entity['created'] = datetime.datetime.utcnow()
    entity['status'] = 'PENDING'
    entity['lock_expires'] = None
    entity['retry_count'] = 0
    entity['last_error'] = ''
    entity['handled_by_sweeper'] = False
    entity['sdk_lang'] = 'PYTHON'
    with _use_default_datastore_adapter():
      datastore.Put(entity)
    pending_keys.append(entity.key())

  _register_post_commit_dispatch(queue_name, pending_keys)


def sweep():
  """Queries Datastore for pending Cloud Tasks and dispatches them."""
  try:
    with _use_default_datastore_adapter():
      query = datastore.Query('_AE_PendingCloudTask')
      entities = query.Run()
  except Exception as e:
    logging.error("Failed to query _AE_PendingCloudTask in sweeper: %s", e)
    return

  now = datetime.datetime.utcnow()
  keys_to_dispatch = []
  for entity in entities:
    if not entity:
      continue
    status = entity.get('status', 'PENDING')
    if status == 'DONE':
      continue
    if status == 'PROCESSING':
      lock_expires = entity.get('lock_expires')
      if lock_expires and isinstance(lock_expires, datetime.datetime):
        if now < lock_expires:
          continue  # still actively processing and lock valid
      elif not lock_expires:
        continue  # assume lock valid if just started

    created = entity.get('created')
    if status == 'PENDING' and created and isinstance(created, datetime.datetime):
      if (now - created).total_seconds() < 60:
        continue  # give fast-path 60s to dispatch post-commit

    retry_count = entity.get('retry_count', 0)
    if status == 'FAILED' and retry_count >= 5:
      continue  # exceeded max sweeper retries

    keys_to_dispatch.append(entity.key())

  if keys_to_dispatch:
    logging.info("Cloud Tasks sweeper found %d tasks to process.", len(keys_to_dispatch))
    _dispatch_pending_keys_now(keys_to_dispatch, handled_by_sweeper=True)


def sweep_wsgi_app(environ, start_response):
  """WSGI app handler for /_ah/cloudtask/sweep."""
  is_cron = str(environ.get('HTTP_X_APPENGINE_CRON', '')).lower() == 'true' or str(environ.get('X-AppEngine-Cron', '')).lower() == 'true'
  if not is_cron and not str(environ.get('SERVER_SOFTWARE', '')).lower().startswith('dev'):
    status = '403 Forbidden'
    response_headers = [('Content-Type', 'text/plain')]
    start_response(status, response_headers)
    return [b'Access denied: endpoint only accessible via App Engine Cron.\n']

  try:
    sweep()
    status = '200 OK'
    response_headers = [('Content-Type', 'text/plain')]
    start_response(status, response_headers)
    return [b'Sweeper completed successfully.\n']
  except Exception as e:
    logging.error("Cloud Tasks sweeper failed: %s", e)
    status = '500 Internal Server Error'
    response_headers = [('Content-Type', 'text/plain')]
    start_response(status, response_headers)
    return [f'Sweeper failed: {e}\n'.encode('utf-8')]


