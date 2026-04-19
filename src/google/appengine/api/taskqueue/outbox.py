import datetime
import json
import logging
import os
from google.appengine.api import datastore
from google.appengine.api import app_identity
from google.appengine.api import urlfetch
from google.cloud import tasks_v2beta2

PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT')
LOCATION_ID = os.environ.get('GAE_LOCATION', 'us-east1')

def trigger_dispatch(datastore_key):
    """Triggers immediate dispatch for a pending task."""
    try:
        process_pending_task(datastore_key)
    except Exception as e:
        logging.error(f"Error in trigger_dispatch for {datastore_key}: {e}")

def process_pending_task(datastore_key):
    """Processes a _AE_PendingCloudTask: leases it, creates Cloud Task, deletes or marks as done."""
    
    # 1. Transactional Lease
    def _lease_task():
        try:
            def _tx():
                try:
                    entity = datastore.Get(datastore_key)
                except datastore.EntityNotFoundError:
                    return None
                    
                if entity['status'] == 'DONE':
                    return None
                    
                now = datetime.datetime.utcnow()
                if entity['status'] == 'PROCESSING' and entity.get('lease_expires') and entity['lease_expires'] > now:
                    return None
                    
                entity['status'] = 'PROCESSING'
                entity['lease_expires'] = now + datetime.timedelta(minutes=2)
                datastore.Put(entity)
                return entity
                
            return datastore.RunInTransaction(_tx)
        except Exception as e:
            logging.error(f"Failed to lease task in transaction: {e}")
            return None

    entity = _lease_task()
    if not entity:
        return "Task not acquired"

    # 2. Create Cloud Task
    try:
        task_data = json.loads(entity['cloud_task_payload'])
        
        client = tasks_v2beta2.CloudTasksClient()
        parent = client.queue_path(PROJECT_ID, LOCATION_ID, entity['queue_name'])
        
        app_engine_request = tasks_v2beta2.AppEngineHttpRequest(
            relative_url=task_data['url'],
        )
        
        if task_data.get('method'):
            method_enum = getattr(tasks_v2beta2.HttpMethod, task_data['method'], tasks_v2beta2.HttpMethod.POST)
            app_engine_request.http_method = method_enum
            
        if task_data.get('payload') is not None:
            payload_bytes = task_data['payload']
            if isinstance(payload_bytes, str):
                payload_bytes = payload_bytes.encode('utf8')
            app_engine_request.payload = payload_bytes
            
        if task_data.get('headers'):
            for k, v in task_data['headers'].items():
                app_engine_request.headers[k] = v
                
        if task_data.get('target'):
            app_engine_request.app_engine_routing = tasks_v2beta2.AppEngineRouting(service=task_data['target'])
            
        ct_task = tasks_v2beta2.Task(app_engine_http_request=app_engine_request)
        
        if task_data.get('eta'):
            from google.protobuf import timestamp_pb2
            ts = timestamp_pb2.Timestamp()
            ts.FromDatetime(datetime.datetime.fromisoformat(task_data['eta']))
            ct_task.schedule_time = ts
            
        if task_data.get('retry_options'):
            ro = task_data['retry_options']
            try:
                if ro.get('task_retry_limit') is not None:
                    ct_task._pb.retry_config.max_attempts = ro['task_retry_limit']
                if ro.get('min_backoff_seconds') is not None:
                    ct_task._pb.retry_config.min_backoff.seconds = int(ro['min_backoff_seconds'])
                if ro.get('max_backoff_seconds') is not None:
                    ct_task._pb.retry_config.max_backoff.seconds = int(ro['max_backoff_seconds'])
            except AttributeError as e:
                 logging.warning(f"Failed to set retry_config via _pb in outbox: {e}")

        response = client.create_task(parent=parent, task=ct_task)
        logging.info(f"Created Cloud Task: {response.name}")

        # 3. Cleanup
        def _cleanup():
            try:
                def _tx_cleanup():
                    ent = datastore.Get(datastore_key)
                    ent['status'] = 'DONE'
                    ent['lease_expires'] = None
                    ent['cloud_task_created'] = datetime.datetime.utcnow()
                    datastore.Put(ent)
                datastore.RunInTransaction(_tx_cleanup)
            except Exception as e:
                logging.error(f"Failed to cleanup task in outbox: {e}")
                
        _cleanup()
        return "Success"

    except Exception as e:
        logging.error(f"Failed to create Cloud Task from outbox: {e}")
        def _handle_failure():
            try:
                def _tx_fail():
                    ent = datastore.Get(datastore_key)
                    ent['status'] = 'PENDING'
                    ent['lease_expires'] = None
                    ent['retry_count'] = ent.get('retry_count', 0) + 1
                    ent['last_error'] = str(e)
                    datastore.Put(ent)
                datastore.RunInTransaction(_tx_fail)
            except Exception as e2:
                logging.error(f"Failed to handle failure in outbox: {e2}")
        _handle_failure()
        return f"Failed: {e}"

def sweeper_job():
    """Scans for stuck tasks."""
    now = datetime.datetime.utcnow()
    cutoff = now - datetime.timedelta(minutes=5)

    query = datastore.Query('_AE_PendingCloudTask')
    query['status ='] = 'PENDING'
    
    stuck_pending = query.Get(50)
    
    count = 0
    for entity in stuck_pending:
        if entity['created'] < cutoff:
            process_pending_task(entity.key())
            count += 1
            
    query2 = datastore.Query('_AE_PendingCloudTask')
    query2['status ='] = 'PROCESSING'
    expired_leases = query2.Get(50)
    
    for entity in expired_leases:
        if entity.get('lease_expires') and entity['lease_expires'] < now:
            process_pending_task(entity.key())
            count += 1
            
    return f"Sweeper processed {count} tasks"
