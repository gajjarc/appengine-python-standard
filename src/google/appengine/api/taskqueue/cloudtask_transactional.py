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

"""Cloud Tasks transactional task support for Taskqueue SDK."""

import base64
import contextlib
import datetime
import json
import logging
import os
import threading

from google.api_core import exceptions as google_exceptions
from google.appengine.api import datastore
from google.appengine.api.taskqueue import cloudtask
from google.cloud import tasks_v2beta3


def build_rest_payload_for_transactional_task(queue_name, task):
  """Builds the REST task payload for a transactional task."""
  client = tasks_v2beta3.CloudTasksClient()
  project = os.environ.get('GOOGLE_CLOUD_PROJECT')
  if project and (project.startswith('s~') or project.startswith('e~')):
    project = project[2:]
  region = cloudtask._get_region()

  ct_task = cloudtask._build_ct_task_payload(queue_name, task, client, project, region)
  return cloudtask._convert_to_rest_payload(ct_task)


def dispatch_rest_task(queue_name, task_payload):
  """Dispatches a pre-built task payload immediately using CloudTasksClient."""
  client = tasks_v2beta3.CloudTasksClient()
  project = os.environ.get('GOOGLE_CLOUD_PROJECT')
  if project and (project.startswith('s~') or project.startswith('e~')):
    project = project[2:]
  region = cloudtask._get_region()

  parent = client.queue_path(project, region, queue_name)

  # Unpack base64 body if encoded by _convert_to_rest_payload
  if 'app_engine_http_request' in task_payload:
    ae_req = task_payload['app_engine_http_request']
    if 'body' in ae_req and isinstance(ae_req['body'], str):
      ae_req['body'] = base64.b64decode(ae_req['body'].encode('utf-8'))

  client.create_task(request={'parent': parent, 'task': task_payload})


_transaction_pending_keys = threading.local()


def _get_tx_pending():
  return getattr(_transaction_pending_keys, 'keys', None)


def _start_tx_context():
  prev = getattr(_transaction_pending_keys, 'keys', None)
  _transaction_pending_keys.keys = []
  return prev


def _restore_tx_context(prev):
  _transaction_pending_keys.keys = prev


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
