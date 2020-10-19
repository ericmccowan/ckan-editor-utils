## Introduction
This library assists CKAN editors with doing batch edits and pairs well with a library like pandas.

## Installation
```shell script
pip install ckan-editor-utils
```
The `requests` package is used for all the underlying API calls.  
The `boto3` AWS SDK package is used for accessing and uploading files from S3. 

## Features and examples
```python
>>> import ckan_editor_utils
```
### Simple API commands
For the basic API commands, much of the `requests` boilerplate code is done for you. 
However, the URL must already have a suffix like `/api/action/`.
```python
>>> url = 'https://horizon.uat.gsq.digital/api/action/'
>>> api_key = os.environ.get('CKAN_API_KEY')
>>> dataset_id = 'my-test-dataset'

>>> res_create = ckan_editor_utils.package_create(
...            url, 
...            api_key, 
...            {
...                'name': dataset_id,                   'extra:identifier': dataset_id,
...                'notes': 'my description',
...                'owner_org': 'geological-survey-of-queensland',
...            })
>>> res_create
<Response [200]>
# This requests response can be viewed by using the .text or .json() methods
>>> res_create.json()
{"help": "https://uat-external.dnrme-qld.links.com.au/api/3/action/help_show?name=package_create", "success": true, "result": {...
```
The response text shows the entire package as CKAN has recorded it. It will populate additional items like 
the Organisation description automatically. 

We can use `package_show` to get the metadata for an existing dataset:
```python
>>> res_show = ckan_editor_utils.package_show(url, api_key, dataset_id)
>>> res_show.json()
{'help': 'https://uat-external.dnrme-qld.links.com.au/api/3/action/help_show?name=package_show', 'success': True, 'result': {'extra:theme': []...
```

Always check the HTTP status before interacting with the data payload. 
For example, a 409 code will be received if it already exists or if
we did not provide enough information for the type of dataset we want to be created, among other reasons. 
If we have the dataset ID wrong, we will get a 404 from CKAN. This is the default `requests` response:
```python
>>> res_missing = ckan_editor_utils.package_show(url, api_key, 'missingdataset')
>>> res_missing
<Response [404]>
>>> res_missing.json()
{'help': 'https://uat-external.dnrme-qld.links.com.au/api/3/action/help_show?name=package_show', 'success': False, 'error': {'message': 'Not found', '__type': 'Not Found Error'}}
```
The next section helps simplify the response using a `CKANResponse` object, which is particularly useful when errors occur.

More examples of basic API usage can be found 
[at the GSQ Open Data API GitHub page](https://github.com/geological-survey-of-queensland/open-data-api#using-python), 
and the official documentation page at [docs.ckan.org](https://docs.ckan.org/en/latest/api/)

### Simplified CKAN Responses
When interacting with the CKAN API, it can be difficult to get a consistent result. Some errors are text not JSON, and 
the JSON errors sometimes contain different attributes depending on the context.
Managing the variety of these responses means a lot of extra logic is needed, which clutters up your script.

This library offers a new `CKANReponse` object that can convert `requests` responses from CKAN into something 
more consistent and manageable. To use it, simply pass it a CKAN response you received when using `requests`.
```python
>>> check_res_show = ckan_editor_utils.CKANResponse(res_show)  # (response from earlier example)
>>> print(check_res_show)
Response 200 OK
>>> check_res_show.ok
True
>>> check_res_show.result
{'extra:theme': [], 'license_title': None, 'maintainer': None, ...
``` 
A JSON response will always be present in the `result` attribute of the CKAN response.
This means you can reliably use `result` to capture output and it will always be relevant.
Furthermore the API action made will be logged to stdout/the console, so you can easily track progress. 

Continuing the 404 example from above, the response can be changed to something easier to manage:
```python
>>> cr = ckan_editor_utils.CKANResponse(res_missing)
Response 404 not OK: {"message": "Not found"}
>>> cr.result
{'message': 'Not found'}
>>> cr.ok
False
```
These simplified CKAN responses are included in the managed actions described in the next section. 

### Managed API actions
Some common workflows have been developed and make it easier to do simple actions.

The following managed actions are available via the `CKANEditorSession` context manager class:
* put_dataset (create or update)
* delete_dataset (delete and purge)
* put_resource_from_s3 (automatically does multipart uploads)

Additionally, the `CKANEditorSession` will fix up the provided CKAN URL if it is missing the required `api/action/` path.

```python
with ckan_editor_utils.CKANEditorSession(url, api_key) as ckaneu:
    return ckaneu.delete_dataset(dataset_id).result
```
Here we are able to get the `result` attribute without any extra logic or coding because the response object has been simplified.

#### Adding a dataset using put_dataset()
As an editor doing bulk changes, you might not be sure if every package already exists before you can safely 
call `package_update()`. Instead, you can just call `put_dataset()`, and the managed session will either create or 
update the dataset depending on what it finds.

```python
data = {
    'name': 'ds000001',
    'extra:identifier': 'DS000001',
    'notes': 'Some description about this dataset.',
    'owner_org': 'my-organisation-lowercase-with-dashes',
    # include any other required and known fields
}
with ckan_editor_utils.CKANEditorSession(url, api_key) as ckaneu:
    res = ckaneu.put_dataset(data, skip_existing=True)
    print(res.result)
```
Including `skip_existing=True` means if a dataset exists, it will not be modified. 
Passing `False` will update the existing dataset with any attributes you pass in, leaving all others intact.

#### Adding a resource from S3 using put_resource_from_s3()
This tool helps you upload a data object located in S3 to CKAN. The following fields are required:
```python
resource = {
    'name': 'ds000001',
    'resource:name': 'My New Resource to Share',
    'resource:description': 'Some description about the particular resource.'
}
```
The size and format are automatically calculated for you. We use `resource:name` because a common workflow is to load
data from a CSV that includes both the dataset `name` and the resource `name`, which have the same label in CKAN.  

You also need an `s3_path` value to pass in, like so:
```python
's3_path' = 's3://mybucket/myprefix/myfile1.zip'
```
Then you can call the function using the same context manager session:
```python
with ckan_editor_utils.CKANEditorSession(url, api_key) as ckaneu:
    res = ckaneu.put_resource_from_s3(resource, s3_path, skip_existing=True)
    print(res.result)
```
Including `skip_existing=True` means if a resource exists, it will not be modified. 
Passing `False` will update the existing resource with any attributes and data objects you pass in, leaving all others intact.



