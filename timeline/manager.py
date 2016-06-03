from collections import OrderedDict
import json
import logging
import multiprocessing
import os
import sys
import signal
import time
import uuid
import zipfile

"""
A Manager is an object that creates and manages a daemonised process.

In this case there is a ParseManager which is a daemon which monitors a
directory and parses all cbcollects which enter the directory, once parsed it
will give magic bob the key to the parsed result.

The CheckerManager waits for snapshots to be placed into its directory and runs
the checkers on the parsed blobs referenced in the snapshots.

Both Managers place their results straight into Couchbase as well as passing
on any relevant information to other services.
"""

# Number of parsing worker threads
POOL_SIZE = os.environ.get('TIMELINE_POOL_SIZE', multiprocessing.cpu_count())

# Time to sleep between directory trawls
SLEEP_TIME = 15

logging.basicConfig(format='%(levelname)s|%(name)s: %(message)s',
                    level=logging.INFO)

# see https://github.com/couchbaselabs/supportal/blob/master/supportal/lib/s3checker/s3checker.py#L343
class Manager(object):
    def __init__(self, directory, suffix, work_function, git_rev):
        self.logger = logging.getLogger('timeline.manager')
        try:
            import couchbase_ffi
            from couchbase.bucket import Bucket
        except ImportError:
            self.logger.critical('Failed to import Couchbase dependencies',
                                 exc_info=True)
            sys.exit(1)
        else:
            try:
                conn_str = 'couchbase://{}/{}'.format(
                    os.environ.get('DB_HOST', 'localhost'),
                    os.environ.get('DB_BUCKET_TIMELINE', 'timeline'))
                self.bucket = Bucket(conn_str)
                self.bucket.timeout = 30
                self.logger.info('Successfully connected to {}'
                                 .format(conn_str))
            except Exception:
                self.logger.critical('Failed to open bucket',
                                     exc_info=True)
                sys.exit(1)
            self.directory = directory
            self.suffix = suffix
            self.work_function = work_function
            self.git_rev = git_rev

    def trawl_directory(self):
        for file_name in os.listdir(self.directory):
            full_path = os.path.join(self.directory, file_name)
            if file_name.endswith(self.suffix):
                self.file_action(full_path)
        time.sleep(SLEEP_TIME)

    def file_action(self, file_name):
        raise NotImplementedError

    def delete_file(self, file_name):
        try:
            os.remove(file_name)
            self.logger.info('Removed {}'.format(file_name))
        except IOError:
            self.logger.error('Failed to remove {}'.format(file_name),
                              exc_info=True)
            return False
        else:
            return True

    def store_in_cb(self, key, doc):
        from couchbase.exceptions import TimeoutError, TemporaryFailError, \
            ValueFormatError
        try:
            self.bucket.upsert(key, doc)
            self.logger.info('Stored {} in CB'.format(key))
        except (TimeoutError, TemporaryFailError):
            self.logger('Experienced timeout storing key `{}` in Couchbase, '
                        'retrying')
            try:
                self.bucket.upsert(key, doc)
            except TimeoutError:
                self.logger.error('Experienced 2nd timeout storing key `{}` in'
                                  ' Couchbase, giving up for now')
                return False
            else:
                return True
        except ValueFormatError as e:
            self.logger.error('Failed to store key `{}` in Couchbase'
                              .format(key), exc_info=True)
            raise e
        else:
            return True


class ParserManager(Manager):
    def __init__(self, directory, parse_func, git_rev):
        super(ParserManager, self).__init__(directory, '.zip', parse_func,
                                            git_rev)
        self.logger = logging.getLogger('timeline.manager.parser')
        self.processing_queue = multiprocessing.Queue()
        self.file_list = []
        self.processed_queue = multiprocessing.Queue()
        self.register_kill()
        for i in range(0, int(POOL_SIZE)):
            p = multiprocessing.Process(target=self.parse_to_json,
                                        args=())
            p.start()
        while True:
            while not self.processed_queue.empty():
                parse_result = self.processed_queue.get()
                # If a result is None then there has been an error
                # Don't bother trying to store this, but delete
                # the file still
                if (parse_result is not None and
                        parse_result['result'] is not None):
                    if not self.store_parsed_log(
                            parse_result['result'],
                            parse_result['uuid'],
                            parse_result['collected_date'],
                            parse_result['node_name']):
                        self.processed_queue.put(parse_result)
                        self.logger.info('Added {} back to the processed '
                                         'queue'.format(
                                             parse_result['file_name']))
                        continue
                else:
                    self.logger.warning('Unparsable zip found {}'
                                        .format(parse_result['file_name']))

                if self.delete_file(parse_result['file_name']):
                    self.file_list.remove(parse_result['file_name'])
                else:
                    self.processed_queue.put(parse_result)
                    self.logger.info('Added {} back to the processed queue'
                                     .format(parse_result['file_name']))

            self.trawl_directory()

    def parse_to_json(self):
        self.register_kill()
        logger_process = logging.getLogger('timeline.manager.parser.{}'.format(
            multiprocessing.current_process().name))
        while True:
            file_name = self.processing_queue.get()
            logger_process.info('Taken {} from the processing queue'
                                .format(file_name))
            try:
                timeline = self.work_function(file_name)
            except Exception as e:
                logger_process.warning('Error parsing zip-file: {}'
                                       .format(e.message))
                timeline = None
                cluster_uuid = None
                collected_date = None
                my_node = None
            else:
                if timeline is None:
                    continue

                try:
                    cluster_uuid = timeline.cluster_uuid
                except AttributeError:
                    cluster_uuid = '{}_no_cluster'.format(os.path
                                                          .split(file_name)[1])
                try:
                    my_node = timeline.default_node_name
                except AttributeError:
                    logger_process.error('Unable to find node name of {}'
                                         .format(file_name))
                    self.processed_queue.put({'file_name': file_name,
                                              'result': None})
                    continue
                else:
                    if my_node[:5] == 'ns_1@':
                        my_node = my_node[5:]

                try:
                    collected_date = timeline.collection_time
                except AttributeError:
                    logger_process.error('Unable to find collection time of {}'
                                         .format(file_name))
                    self.processed_queue.put({'file_name': file_name,
                                              'result': None})
                    continue

                timeline = timeline.to_dict()
                timeline['git_rev'] = self.git_rev

            processed_object = {'file_name': file_name,
                                'result': timeline,
                                'uuid': cluster_uuid,
                                'collected_date': collected_date,
                                'node_name': my_node}
            self.processed_queue.put(processed_object)
            logger_process.info('Added {} to the processed queue'
                                .format(file_name))

    def store_parsed_log(self, doc, cluster_uuid, collected_date, my_node):
        # Do storage here
        key = 'Timeline::{}::{}::{}'.format(cluster_uuid, collected_date,
                                            my_node)
        if self.store_in_cb(key, doc):
            result_path = os.path.join(
                os.environ.get('DIR_MAGICBOB', '/magicbob'),
                '{}.timeline'.format(str(uuid.uuid4()))
            )

            try:
                with open(result_path, 'w') as f:
                    f.write(key)
            except IOError:
                self.logger.error('Failed to write key {} to Magic Bob'
                                  .format(key), exc_info=True)
                return False
            else:
                self.logger.info('Wrote key {} to Magic Bob'.format(key))
                return True

    def file_action(self, file_name):
        if file_name not in self.file_list:
            try:
                # This prevent partial zip files from
                # being parsed, e.g files being copied
                # into the directory
                zipfile.ZipFile(file_name, 'r')
            except zipfile.BadZipfile:
                self.logger.warning('Bad zip found - {}'
                                    .format(file_name))
            except IOError as e:
                self.logger.info('IOError on {} - {}'.format(file_name,
                                                             e.message))
            else:
                self.processing_queue.put(file_name)
                self.logger.info('Added {} to the processing queue'
                                 .format(file_name))
                self.file_list.append(file_name)

    def kill(self, x, y):
        # Signal handlers for ctrl+c
        self.logger.info('Program killed by CTRL+C')
        os._exit(130)

    def register_kill(self):
        # Register signal handler for clean exit
        signal.signal(signal.SIGINT, self.kill)


class CombinerManager(Manager):
    def __init__(self, directory, result_func, git_rev):
        super(CombinerManager, self).__init__(directory, '.snapshot',
                                              result_func, git_rev)
        self.logger = logging.getLogger('timeline.manager.combiner')
        while True:
            self.trawl_directory()

    def file_action(self, file_name):
        with open(file_name, 'r') as f:
            keys = f.read().split('\n')

        keys = [key for key in keys if key]

        snapshot_key = keys[0]
        result = self.work_function(
            self.get_parsed_from_cb(keys[1:]))
        self.logger.debug('Result - {}'.format(result))
        self.store_results(result, snapshot_key)
        os.remove(file_name)
        self.logger.info('Removed file {}'.format(file_name))

    def get_parsed_from_cb(self, keys):
        from couchbase.exceptions import TimeoutError
        keys = list(set(keys))
        docs = None
        try:
            docs = self.bucket.get_multi(keys, quiet=True)
        except TimeoutError:
            self.logger.warning('Timeout fetching keys {}, retrying'
                                .format(keys))
            try:
                docs = self.bucket.get_multi(keys, quiet=True)
            except TimeoutError:
                self.logger.error('Second timeout fetching keys {}, giving up'
                                  .format(keys))
                sys.exit(1)

            except Exception:
                self.logger.error('Error fetching keys {}'
                                  .format(keys), exc_info=True)
        except Exception:
            self.logger.error('Error fetching keys {}'
                              .format(keys), exc_info=True)

        if docs:
            self.logger.info('Loaded keys {} from CB'.format(keys))
            try:
                parse_results = [docs[key].value for key in keys
                                 if docs[key].value]
            except Exception:
                self.logger.error('Failed to retrieve values for key {}'
                                  .format(key), exc_info=True)
            else:
                return parse_results

    def store_results(self, results, snapshot_name):
        key = 'Timeline::{}'.format(snapshot_name)
        results = json.loads(results)
        doc = {'git_rev': self.git_rev, 'results': results}
        self.store_in_cb(key, doc)
