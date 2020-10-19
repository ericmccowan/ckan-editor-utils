from urllib.parse import urlparse, quote

import boto3
import functools
import json

from botocore.exceptions import ClientError
from io import BytesIO

import requests
import os
import logging

logger = logging.getLogger(__name__)


class UserException(Exception):
    pass


class CKANResponse(object):

    def __init__(self, response: requests.models.Response):

        self.response = response
        self._ok = False
        self.status = None
        self.status_code = None

        if self.response is not None:
            self.status_code = self.response.status_code

            try:
                self.result = response.json()

            except Exception as e:
                self.result = dict(message='{}: {}'.format(e, response.text.replace('\n', ' ')))

            if self.response.ok and self.result.get('success'):
                self._ok = True
                self.status = 'OK'
                self.result = self.result.get('result')

            else:
                self.status = 'not OK'

                if self.status_code == 503:
                    self.result = 'Service unavailable'

                if self.status_code == 403:
                    self.result = 'Forbidden'

                if isinstance(self.result, dict):
                    err = self.result.get('error', {})

                    if err.get('name'):
                        self.result = err.get('name', ['']).pop()

                    elif err.get('id'):
                        self.result = err.get('id', '')

                    elif err.get('message'):
                        self.result = err.get('message', '')

                    elif err.get('__type') == 'Validation Error':
                        self.result = err

            if isinstance(self.result, str):
                self.result = dict(message=self.result)

        else:
            self.result = dict(result=None)
            self.status_code = None

        if self.ok:
            logger.info(str(self))
        else:
            logger.warning(str(self))

    @property
    def ok(self):

        return self._ok

    @ok.setter
    def ok(self, value: bool):

        if isinstance(value, bool):
            self._ok = value

    def __str__(self):
        if self.ok:
            return 'Response {} {}'.format(self.status_code, self.status)
        else:
            return 'Response {} {}: {}'.format(self.status_code, self.status, json.dumps(self.result))


def _urlencode_json(data: dict) -> str:
    data_str = json.dumps(data, default=str)
    data_enc = quote(data_str)
    return data_enc


def site_read(url, key):
    response = requests.get(url + 'site_read', headers=dict(Authorization=key))
    return response


def package_show(url, key, dataset_id):
    logger.info('Showing dataset ' + dataset_id)
    response = requests.get(url + 'package_show', params={'id': dataset_id}, headers=dict(Authorization=key))
    return response


def package_query(url, key, query):
    # query eg 'type:report'
    logger.info('Searching datasets for filtered query' + str(query))
    response = requests.get(url + 'package_search', params={'fq': [query]}, headers=dict(Authorization=key))
    return response


def resource_show(url, key, resource_id):
    logger.info('Showing resource ' + resource_id)
    response = requests.get(url + 'resource_show', params={'id': resource_id}, headers=dict(Authorization=key))
    return response


def resource_delete(url, key, resource_id):
    logger.info('Deleting resource ' + resource_id)
    response = requests.post(url + 'resource_delete', data={'id': resource_id}, headers=dict(Authorization=key))
    return response


def package_delete(url, key, dataset_id):
    logger.info('Deleting dataset ' + dataset_id)
    response = requests.post(url + 'package_delete', data={'id': dataset_id}, headers=dict(Authorization=key))
    return response


def dataset_purge(url, key, dataset_id):
    logger.info('Purging dataset ' + dataset_id)
    response = requests.post(url + 'dataset_purge', data={'id': dataset_id}, headers=dict(Authorization=key))
    return response


def package_create(url, key, data):
    logger.info('Creating dataset ' + data['name'])
    response = requests.post(url + 'package_create', data=_urlencode_json(data),
                             headers={'Authorization': key,
                                      'Content-Type': 'application/x-www-form-urlencoded'}
                             )
    return response


def package_update(url, key, data):
    logger.info('Updating dataset ' + data['name'])
    response = requests.post(url + 'package_update', data=_urlencode_json(data),
                             headers={'Authorization': key,
                                      'Content-Type': 'application/x-www-form-urlencoded'}
                             )
    return response


def resource_create(url, key, data):
    logger.info('Creating resource ' + data['name'])
    response = requests.post(url + 'resource_create', data=_urlencode_json(data),
                             headers={'Authorization': key,
                                      'Content-Type': 'application/x-www-form-urlencoded'}
                             )
    return response


def resource_update(url, key, data):
    logger.info('Updating resource ' + data['id'])
    response = requests.post(url + 'resource_update', data=_urlencode_json(data),
                             headers={'Authorization': key,
                                      'Content-Type': 'application/x-www-form-urlencoded'}
                             )
    return response


class AttributeUpdater(object):
    def __init__(self):
        self._edit_count = 0

    @property
    def edit_count(self):

        return self._edit_count

    @edit_count.setter
    def edit_count(self, count):

        if isinstance(count, int):
            self._edit_count = count

    def update(self, data_to_update: dict, new_data: dict):
        if new_data is None:
            return data_to_update

        updated_data = data_to_update.copy()

        for key, new_value in new_data.items():
            if data_to_update.get(key) != new_value:
                logger.info(
                    'Modify "{}": {} -> {}'.format(key, str(data_to_update.get(key, ''))[:500], new_value))
                updated_data[key] = new_value
                self.edit_count += 1

        if self.edit_count > 0:
            logger.info('{} edits made'.format(self.edit_count))
            return updated_data
        else:
            return data_to_update


class CKANEditor(object):
    def __init__(self, url, key):
        self.url = url
        self.key = key

    def put_dataset(self, data, skip_existing=True) -> CKANResponse:
        res_show = CKANResponse(package_show(self.url, self.key, data['name']))

        # either its not there and we create it, its there and we skip it, its there and we update it,
        if not res_show.ok:

            required_attrs = ['name', 'notes', 'owner_org', 'extra:identifier']
            for attr in required_attrs:
                if attr not in data:
                    raise UserException('Resource attribute missing: {}'.format(attr))

            res_create = CKANResponse(package_create(self.url, self.key, data))
            return res_create

        elif res_show.ok and skip_existing:
            logger.info('Dataset {} exists, skipping'.format(data['name']))
            return res_show

        logger.info('Updating newly provided attributes for dataset {}'.format(data['name']))

        au = AttributeUpdater()
        new_ckan_content = au.update(res_show.result, data)

        if au.edit_count > 0:
            # hotfix to remove organisation markdown formatting that triggers firewall
            #  it will get replaced server-side by CKAN anyway
            new_ckan_content['organization'] = new_ckan_content['organization']['name']

            res_update = CKANResponse(package_update(self.url, self.key, new_ckan_content))
            return res_update
        else:
            logger.info('No change; update not requested')
            return res_show

    def delete_dataset(self, dataset_id) -> CKANResponse:
        logger.info('Deleting and purging dataset ' + dataset_id + ' and its resources')
        res_show = CKANResponse(package_show(self.url, self.key, dataset_id))
        if res_show.ok:
            for resource in res_show.result.get('resources', []):
                CKANResponse(resource_delete(self.url, self.key, resource['id']))

            CKANResponse(package_delete(self.url, self.key, dataset_id))
            CKANResponse(dataset_purge(self.url, self.key, dataset_id))
        return CKANResponse(None)

    def put_resource_from_s3(self, data: dict, s3_path: str, skip_existing=True) -> CKANResponse:
        res_show = CKANResponse(package_show(self.url, self.key, data['name']))

        current_resources = res_show.result.get('resources', [])

        existing_resource_id = ''
        current_resource_data = dict()
        for cr in current_resources:
            if cr['name'] == data['resource:name']:
                if not existing_resource_id:
                    current_resource_data = cr
                    existing_resource_id = cr['id']
                else:
                    logger.warning('Multiple resources have been matched, skipping this update...')
                    return res_show

        if existing_resource_id:
            # logger.info(
            #     'Matched existing resource {} ({})'.format(data['resource:name'], existing_resource_id))
            if skip_existing:
                logger.info(
                    'Matched existing resource {} ({}), skipping...'.format(data['resource:name'], existing_resource_id)
                )
                return res_show
            else:
                logger.info(
                    'Matched existing resource {} ({}), updating...'.format(data['resource:name'], existing_resource_id)
                )

        s3r = boto3.resource('s3')

        s3_path_parsed = urlparse(s3_path)
        s3_bucket = s3_path_parsed.netloc
        s3_key = s3_path_parsed.path[1:]  # Drop leading /

        try:
            s3_object_summary = s3r.ObjectSummary(
                bucket_name=s3_bucket,
                key=s3_key
            )
            # An exception will only trigger if missing attributes are requested
            obj_name = os.path.basename(s3_object_summary.key)
            obj_size = s3_object_summary.size
        except ClientError as e:
            logger.error('Invalid S3 object: {}'.format(e))
            return CKANResponse(None)

        # Create or update resource
        new_resource_data = {
            'package_id': data['name'],
            'url': obj_name,
            'name': data['resource:name'],
            'description': data['resource:description'],
            'resource:description': data['resource:description'],
            'multipart_name': obj_name,
            'url_type': 'upload',
            'size': obj_size
        }

        if existing_resource_id:
            data['id'] = existing_resource_id
            updated_resource_data = AttributeUpdater().update(current_resource_data, new_resource_data)
            response = CKANResponse(resource_update(self.url, self.key, updated_resource_data))
        else:
            required_attrs = ['name', 'resource:name', 'resource:description']
            for attr in required_attrs:
                if attr not in data:
                    raise UserException('Resource attribute missing: {}'.format(attr))

            response = CKANResponse(resource_create(self.url, self.key, new_resource_data))

        if response.ok and s3_path is not None:
            # logger.info('An S3 path has been provided and will be uploaded')
            resource = response.result
            res_upload = self._upload_s3_resource(resource.get('id', ''), s3_object_summary)
            return res_upload
        else:
            return response

    def _upload_s3_resource(self, resource_id: str, s3_object_summary: boto3.resource('s3').ObjectSummary) -> CKANResponse:

        filename = os.path.basename(s3_object_summary.key)
        logger.info('Uploading resource {}, size {:.1f} MB from source {}'.format(
            filename,
            s3_object_summary.size / 1024 / 1024,
            's3://{}/{}'.format(s3_object_summary.bucket_name, s3_object_summary.key))
        )

        # initiate multipart upload
        multipart_res = CKANResponse(
            requests.post(url=self.url + 'cloudstorage_initiate_multipart',
                          data=_urlencode_json(dict(id=resource_id, name=filename, size=s3_object_summary.size)),
                          headers={'Authorization': self.key,
                                   'Content-Type': 'application/x-www-form-urlencoded'}))

        multipart_id = multipart_res.result['id']

        part = 0
        chunk_size = 1024 * 1024 * 5
        data_obj_body = s3_object_summary.get()["Body"]

        chunker = functools.partial(data_obj_body.read, chunk_size)

        for chunk in iter(chunker, b''):
            part += 1
            fragment = CKANResponse(
                requests.post(
                    url=self.url + 'cloudstorage_upload_multipart',
                    data=dict(uploadId=multipart_id, partNumber=part),
                    files=dict(upload=BytesIO(chunk)),
                    headers=dict(Authorization=self.key)
                )
            )
            if fragment.ok:
                logger.info('Fragment #{} uploaded'.format(str(part)))
            else:
                logger.info(fragment.result)

        res_finish = CKANResponse(
            requests.post(
                url=self.url + 'cloudstorage_finish_multipart',
                data=dict(id=resource_id, uploadId=multipart_id),
                headers=dict(Authorization=self.key)
            )
        )

        if not res_finish.ok:
            CKANResponse(resource_delete(self.url, self.key, resource_id))
        return res_finish


class CKANEditorSession(object):
    def __init__(self, url=None, key=None):
        if url is None or key is None:
            raise UserException('The CKAN URL and/or API Key was not provided')

        self.key = key
        url_parsed = urlparse(url)

        # Use start and end because it may contain an optional version number
        if url_parsed.path.startswith('/api/') and url_parsed.path.endswith('/action/'):
            self.url = url
        elif url_parsed.path == '':
            self.url = url + '/api/action/'
        elif url_parsed.path == '/':
            self.url = url + 'api/action/'
        else:
            raise UserException('The CKAN URL provided is not valid')

        if url_parsed.params + url_parsed.query + url_parsed.fragment != '':
            raise UserException('The CKAN URL provided is not valid')

    def __enter__(self):
        self.ckaneditor = CKANEditor(self.url, self.key)
        return self.ckaneditor

    def __exit__(self, exc_type, exc_val, exc_tb):
        del self.ckaneditor


if __name__ == '__main__':
    pass
else:
    logger.info('Imported ckan_editor_utils')
