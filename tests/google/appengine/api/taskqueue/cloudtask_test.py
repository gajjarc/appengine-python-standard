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

import os
import unittest
from unittest import mock
from google.appengine.api.taskqueue import cloudtask
from google.appengine.api.taskqueue import taskqueue


class CloudtaskTest(unittest.TestCase):

  def setUp(self):
    super(CloudtaskTest, self).setUp()
    self.mock_client = mock.Mock()
    self.mock_client.task_path.return_value = 'projects/p/locations/l/queues/q/tasks/t'

  @mock.patch.dict(os.environ, {'GAE_SERVICE': 'default-service'}, clear=False)
  @mock.patch('google.appengine.api.app_identity.get_default_version_hostname', return_value='app.appspot.com')
  def test_build_ct_task_payload_with_default_app_version_target(self, _):
    task = taskqueue.Task(url='/test', target=taskqueue.DEFAULT_APP_VERSION)
    payload = cloudtask._build_ct_task_payload(
        queue_name='default',
        task=task,
        client=self.mock_client,
        project='p',
        region='us-central1'
    )
    self.assertIn('app_engine_http_request', payload)
    self.assertEqual(
        payload['app_engine_http_request'].get('app_engine_routing', {}).get('service'),
        'default-service'
    )

  @mock.patch.dict(os.environ, {}, clear=True)
  @mock.patch('google.appengine.api.app_identity.get_default_version_hostname', return_value='app.appspot.com')
  def test_build_ct_task_payload_with_string_target(self, _):
    task = taskqueue.Task(url='/test', target='worker-dot')
    payload = cloudtask._build_ct_task_payload(
        queue_name='default',
        task=task,
        client=self.mock_client,
        project='p',
        region='us-central1'
    )
    self.assertEqual(
        payload['app_engine_http_request'].get('app_engine_routing', {}).get('service'),
        'worker'
    )


if __name__ == '__main__':
  unittest.main()
