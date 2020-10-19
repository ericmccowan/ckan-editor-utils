from ckan_editor_utils import __version__
# from ckan_editor_utils import __version__
import os

def test_version():
    assert __version__ == '0.1.6'
    assert os.environ.get('CKAN_API_KEY') is not None

import os
import sys
import logging

import pytest
import requests
import boto3
import hashlib
import ckan_editor_utils
from io import BytesIO
from botocore.exceptions import ClientError

logging.basicConfig()
logger = logging.getLogger()
logger.handlers = []
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logger.setLevel(logging.INFO)
# stdout handler
hs = logging.StreamHandler(sys.stdout)
hs.setFormatter(logging.Formatter(log_format))
logger.addHandler(hs)

session = boto3.session.Session()
s3 = session.client('s3')
s3r = session.resource('s3')

CKAN_URL = 'https://uat-external.dnrme-qld.links.com.au/api/action/'
CKAN_ORG = 'geological-survey-of-queensland'
CKAN_KEY = os.environ.get('CKAN_API_KEY')
if CKAN_KEY is None:
    sys.exit('CKAN_API_KEY environment variable missing')
BUCKET_NAME = os.environ.get('S3_BUCKET')
if BUCKET_NAME is None:
    sys.exit('S3_BUCKET environment variable missing')

def test_create_package_direct():
    package_id = 'devtest_create'
    notes = 'devtest1 description'

    res_create = ckan_editor_utils.package_create(CKAN_URL, CKAN_KEY,
                              {
                                  'name': package_id,
                                  'extra:identifier': package_id,
                                  'notes': notes,
                                  'owner_org': CKAN_ORG
                              })
    res_show = requests.get(CKAN_URL + 'package_show', params={'id': package_id}, headers=dict(Authorization=CKAN_KEY))

    requests.post(CKAN_URL + 'dataset_purge', data={'id': package_id}, headers=dict(Authorization=CKAN_KEY))

    assert res_create.ok
    assert res_show.json()['result']['notes'] == notes

def test_show_missing_dataset():
    package_id = 'devtest_del'
    requests.post(CKAN_URL + 'dataset_purge', data={'id': package_id}, headers=dict(Authorization=CKAN_KEY))
    res_show = ckan_editor_utils.CKANResponse(ckan_editor_utils.package_show(CKAN_URL, CKAN_KEY, package_id))
    assert not res_show.ok

def test_create_package_managed():
    package_id = 'devtest_createmanaged'
    notes = 'devtest_createmanaged description'

    with ckan_editor_utils.CKANEditorSession(CKAN_URL, CKAN_KEY) as cm:
        res_put1 = cm.put_dataset(
            {
                'name': package_id,
                'extra:identifier': package_id,
                'notes': notes,
                'owner_org': CKAN_ORG,
            },
            skip_existing=False
        )
        res_put2 = cm.put_dataset(
            {
                'name': package_id,
                'extra:identifier': package_id,
                'notes': notes,
                'owner_org': CKAN_ORG,
            },
            skip_existing=True
        )

    res_show = requests.get(CKAN_URL + 'package_show', params={'id': package_id}, headers=dict(Authorization=CKAN_KEY))

    requests.post(CKAN_URL + 'dataset_purge', data={'id': package_id}, headers=dict(Authorization=CKAN_KEY))

    assert res_put1.ok
    assert res_show.json()['result']['notes'] == notes
    assert res_put2.result['notes'] == notes

def _test_resource_upload():
    s3_obj_summary = s3r.ObjectSummary(
        bucket_name=BUCKET_NAME,
        key='Dev/_DSC5121_stitcha.jpg'
    )

    filename = os.path.basename(s3_obj_summary.key)
    package_id = 'devtest_resourceupload_direct'

    ckan_editor_utils.package_create(CKAN_URL, CKAN_KEY,
                              {
                                  'name': package_id,
                                  'extra:identifier': package_id,
                                  'notes': 'some description',
                                  'owner_org': CKAN_ORG
                              })

    res_create = ckan_editor_utils.resource_create(CKAN_URL, CKAN_KEY,
                               {
                                   'package_id': package_id,
                                   'url': filename,
                                   'name': filename,
                                   'notes': 'my photo',
                                   'multipart_name': filename,
                                   'url_type': 'upload',
                               })

    resource_id = res_create.json()['result']['id']

    # upload to ckan
    with ckan_editor_utils.CKANEditorSession(CKAN_URL, CKAN_KEY) as cm:
        res_put = cm._upload_s3_resource(resource_id, s3_obj_summary)

    # download from s3 to get md5
    md5_orig = hashlib.md5(s3_obj_summary.get()["Body"].read()).hexdigest()
    # logger.info('md5 from s3 ' + md5_orig)

    # download from ckan to get md5
    res_show = requests.get(CKAN_URL + 'package_show', params={'id': package_id}, headers=dict(Authorization=CKAN_KEY))
    # logger.info(res_show.json())
    resource_data = res_show.json()['result']['resources'][-1]

    ckan_obj = requests.get(resource_data['url'])

    md5_ckan = hashlib.md5(ckan_obj.content).hexdigest()

    requests.post(CKAN_URL + 'resource_delete', data={'id': resource_data['id']}, headers=dict(Authorization=CKAN_KEY))
    requests.post(CKAN_URL + 'dataset_purge', data={'id': package_id}, headers=dict(Authorization=CKAN_KEY))
    assert md5_ckan == md5_orig


def test_managed_delete():
    package_id = 'devtest_manageddelete'
    with ckan_editor_utils.CKANEditorSession(CKAN_URL, CKAN_KEY) as cm:
        cm.put_dataset(
            {
                'name': package_id,
                'extra:identifier': package_id,
                'notes': 'some description',
                'owner_org': CKAN_ORG
            }
        )
        cm.delete_dataset(package_id)

    res_show = ckan_editor_utils.package_show(CKAN_URL, CKAN_KEY, package_id)
    assert res_show.status_code == 404

def _test_create_resource_managed_one():
    package_id = 'devtest_managedcreateresourceone'
    s3_obj_summary = s3r.ObjectSummary(
        bucket_name=BUCKET_NAME,
        key='Dev/_DSC5121_stitcha.jpg'
    )
    with ckan_editor_utils.CKANEditorSession(CKAN_URL, CKAN_KEY) as cm:
        cm.put_dataset(
            {
                'name': package_id,
                'extra:identifier': package_id,
                'notes': 'some description',
                'owner_org': CKAN_ORG
            }
        )
        resp_res = cm.put_resource_from_s3(
            {
                'name': package_id,
                'resource:name': 'myphoto',
                'resource:description': 'myphoto description'
            },
            skip_existing=True,
            s3_path='s3://sra-data-extract-copy/Dev/_DSC5121_stitcha.jpg'
        )
    md5_orig = hashlib.md5(s3_obj_summary.get()["Body"].read()).hexdigest()
    # logger.info('md5 from s3 ' + md5_orig)

    # download from ckan to get md5
    res_show = requests.get(CKAN_URL + 'package_show', params={'id': package_id}, headers=dict(Authorization=CKAN_KEY))
    # logger.info(res_show.json())
    resource_data = res_show.json()['result']['resources'][-1]

    ckan_obj = requests.get(resource_data['url'])

    md5_ckan = hashlib.md5(ckan_obj.content).hexdigest()

    requests.post(CKAN_URL + 'resource_delete', data={'id': resource_data['id']}, headers=dict(Authorization=CKAN_KEY))
    requests.post(CKAN_URL + 'dataset_purge', data={'id': package_id}, headers=dict(Authorization=CKAN_KEY))

    assert md5_ckan == md5_orig

def _test_create_resource_managed_two():
    package_id = 'devtest_managedcreateresourcetwo'
    s3_obj_summary = s3r.ObjectSummary(
        bucket_name=BUCKET_NAME,
        key='Dev/_DSC5121_stitcha.jpg'
    )
    with ckan_editor_utils.CKANEditorSession(CKAN_URL, CKAN_KEY) as cm:
        cm.put_dataset(
            {
                'name': package_id,
                'extra:identifier': package_id,
                'notes': 'some description',
                'owner_org': CKAN_ORG
            }
        )
        cm.put_resource_from_s3(
            {
                'name': package_id,
                'resource:name': 'myphoto',
                'resource:description': 'myphoto description'
            },
            skip_existing=True,
            s3_path='s3://sra-data-extract-copy/Dev/_DSC5121_stitcha.jpg'
        )
        res_put = cm.put_resource_from_s3(
            {
                'name': package_id,
                'resource:name': 'myphoto',
                'resource:description': 'myphoto description'
            },
            skip_existing=True,
            s3_path='s3://sra-data-extract-copy/Dev/_DSC5121_stitcha.jpg'
        )

    requests.post(CKAN_URL + 'resource_delete', data={'id': res_put.result['resources'][-1]['id']},
                  headers=dict(Authorization=CKAN_KEY))
    requests.post(CKAN_URL + 'dataset_purge', data={'id': package_id}, headers=dict(Authorization=CKAN_KEY))

    assert res_put.result['num_resources'] == 1

def test_s3_object_prefix():
    with pytest.raises(ClientError):
        s3_obj_summary = s3r.ObjectSummary(
            bucket_name=BUCKET_NAME,
            key='Dev/'
        )
        obj_size = s3_obj_summary.size

# def test_s3_object_prefix_try():
#     try:
#         s3_obj_summary = s3r.ObjectSummary(
#             bucket_name=BUCKET_NAME,
#             key='Dev/'
#         )
#         assert s3_obj_summary.key == 'Dev/'
#         assert os.path.basename(s3_obj_summary.key) == ''
#         assert s3_obj_summary.size == 0
#     except ClientError as e:
#         logger.error('Invalid S3 object: {}'.format(e))
#         assert True is None

"""
todo test this object
s3://gdmp-qa-ap-southeast-2-staging-bucket/Migration/data-files/map-collections/20085/Diamantina Lakes_2015_2_Compilation/
does it count as an object? what to we need to validate, size?
should we fail it or just log an error and continue?

for now we could delete from the csv, for sure, but we need to validate objects in s3 properly as others may be missing
"""


def test_unit_update_attributes_same():
    au = ckan_editor_utils.AttributeUpdater()
    assert au.update({'a':1}, {'a':2}) == {'a': 2}
    assert au.edit_count == 1

def test_unit_update_attributes_new():
    au = ckan_editor_utils.AttributeUpdater()
    assert au.update({'a':1}, {'b':2}) == {'a':1, 'b':2}
    assert au.edit_count == 1

def test_unit_update_attributes_multiple():
    au = ckan_editor_utils.AttributeUpdater()
    assert au.update({'a':1}, {'a': 2, 'b':2}) == {'a':2, 'b':2}
    assert au.edit_count == 2

def test_unit_update_attributes_nochange():
    au = ckan_editor_utils.AttributeUpdater()
    assert au.update({'a':1}, {'a':1}) == {'a':1}
    assert au.edit_count == 0

def test_unit_update_attributes_none():
    au = ckan_editor_utils.AttributeUpdater()
    assert au.update({'a':1}, None) == {'a':1}
    assert au.edit_count == 0

def test_unit_update_attributes_twice():
    au = ckan_editor_utils.AttributeUpdater()
    assert au.update({'a':1}, {'b':2}) == {'a':1, 'b':2}
    assert au.update({'a': 1}, {'c': 3}) == {'a': 1, 'c': 3}
    assert au.edit_count == 2

def test_unit_update_attributes_setedits_int():
    au = ckan_editor_utils.AttributeUpdater()
    au.edit_count = 2
    assert au.edit_count == 2

def test_unit_update_attributes_setedits_none():
    au = ckan_editor_utils.AttributeUpdater()
    assert au.update({'a': 1}, {'b': 2}) == {'a': 1, 'b': 2}
    au.edit_count = None
    assert au.edit_count == 1