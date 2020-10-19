# Development instructions

## To publish on PyPi
1. Poetry must be installed via https://python-poetry.org/docs/
2. Make sure `poetry --version` shows correctly and you have configured your PyPi API key 
using `poetry config pypi-token.pypi my-token`
3. Bump the version in `ckan_editor_utils/__init__.py` and `pyproject.toml`
4. `poetry build`
5. `poetry publish`


## To publish on conda-forge
1. Note the version number above as x.y.z
2. Run `openssl sha256 dist/ckan-editor-utils-x.y.z.tar.gz`
3. Update the metadata file in the forked `staged-recipes` feeder for conda-forge: 
https://github.com/ericmccowan/staged-recipes/blob/master/recipes/ckan-editor-utils/meta.yaml  
Both the version and the SHA256 are required.
4. More steps to follow once the initial merge request is complete


#### Troubleshooting Poetry
* Use `source $HOME/.poetry/env` to fix path if it is installed
* To avoid SSL certificate errors when running `get_poetry.py` on a Mac run this:
```shell script
sudo /Applications/Python\ 3.7/Install\ Certificates.command
```