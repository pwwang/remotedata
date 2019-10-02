# remotedata

Accessing and caching remote data for python.
May be used in the cases that:
1. The remote data is being updated frequently
2. You don't want to sync all the data but just per your request
3. You want to cache the data locally for some time
4. Especially, when the files are used for testing

## Installation

```shell
pip install remotedata
```

## Usage

Currently, data from `github` and `dropbox` are supported

### Github

```python
from remotedata import remotedata
rdata = remotedata(dict(
	source = 'github',
	cachedir = '/tmp/cached/',
	## if branch is not master: pwwang/remotedata/branch
	repos  = 'pwwang/remotedata',
	## optional, default is first part of repos
	# user = 'pwwang',
	## github token, in case you have > 60 requests per hours to github API
	# token = 'xxx',
))
readme = rdata.get('README.md')
# README.md is downloaded to /tmp/cache/github/pwwang.remotedata@master/README.md
# Now you can use it as a local file

# readme will be cached, we don't have to download it again,
# until it has been changed remotely.

# remove cached file
rdata.remove('README.md')
# clear up all caches
rdata.clear()
```

### Dropbox

```python
from remotedata import remotedata
rdata = remotedata(dict(
	source = 'dropbox',
	cachedir = '/tmp/cached/',
	dropbox_token = 'xxx'
))
rdata.get('/somefile') # or
rdata.get('somefile')
```