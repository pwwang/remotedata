"""Accessing and caching remote data"""
import warnings
import hashlib
import shutil
import base64
from datetime import datetime
from urllib.parse import quote
from pathlib import Path
import requests
from simpleconf import Config

CONFIG = Config(with_profile = False)
CONFIG._load(dict(
	source   = 'github',
	cachedir = '/tmp/',
	repos    = '',
), '~/.remotedata.yaml', './.remotedata.yaml', 'REMOTEDATA.osenv')

def sha1File(filepath):
	"""Get sha1 for file"""
	bufsize = 2**16
	ret = hashlib.sha1()
	with open(filepath, 'rb') as fhand:
		block = fhand.read(bufsize)
		while block:
			ret.update(block)
			block = fhand.read(bufsize)
	return ret.hexdigest()

def sha1(filepath):
	"""Get SHA1 of a file or directory"""
	filepath = Path(filepath)
	if not filepath.is_dir():
		return sha1File(filepath)
	ret = hashlib.sha1()
	for path in sorted(filepath.glob('*')):
		if path.suffix == '.meta':
			continue
		ret.update(sha1(path).encode())
		if path.is_dir():
			ret.update(path.name.encode())
	return ret.hexdigest()

def metafile(path):
	"""The metafile of the file"""
	if '/' in path:
		path = Path(path)
		return str(path.parent / ('.' + path.name + '.meta'))
	return '.' + path + '.meta'

class RemoteManager:
	"""A manager to download/query remote files"""
	def __init__(self, conf):
		self.conf     = conf
		self.session  = requests
		self.api      = conf.get('api')
		self.cachedir = Path(conf['cachedir'])
		self.reqcache = {}

	def _get(self, url, params):
		"""Get a request object, try to cache it"""
		cachekey = url + '@' + str(sorted(params))
		if cachekey in self.reqcache:
			return self.reqcache[cachekey]
		req = self.session.get(url, params = params)
		self.reqcache[cachekey] = req
		return req

	def get(self, path, **kwargs):
		"""Get a request object"""
		return self._get(self.remote(path), kwargs)

	def exists(self, path, **kwargs):
		"""Tell if a remote path exists"""
		return self.get(path, **kwargs).status_code == 200

	def save(self, path, **kwargs):
		"""Save remote to local file"""
		localfile = self.local(path)
		with localfile.open('wb') as floc:
			floc.write(self.content(path, **kwargs))

	def content(self, path, **kwargs): # pragma: no cover
		"""Get the content as bytes of path"""
		return self.get(path, **kwargs).content

	def json(self, path, **kwargs):
		"""Get the json data of the response"""
		return self.get(path, **kwargs).json()

	def text(self, path, **kwargs):
		"""Get text of the response"""
		return self.content(path, **kwargs).decode()

	def local(self, path):
		"""Get the local path of the file"""
		return self.cachedir / path

	def remote(self, path):
		"""Get the remote path of the file"""
		return self.api.replace('{/path}', '/' + str(path))

	def remove(self, path, clean = False):
		"""Remote the local cache"""
		localmeta = self.cachedir / metafile(path)
		if localmeta.exists():
			localmeta.unlink()
		if clean and self.local(path).exists():
			self.local(path).unlink()

class RemoteFile:
	"""Remote file"""

	def __init__(self, conf, manager = None):
		self.conf    = conf
		self.manager = manager if manager else RemoteManager(conf)

	def remoteMetadata(self, path):
		"""Get the remote meta data including modified time and sha1"""
		if not self.manager.exists(path) or not self.manager.exists(metafile(path)):
			return None, None
		mtime, sha = self.manager.text(metafile(path)).strip().split('|', 1)
		return int(float(mtime)), sha

	def localMetadata(self, path):
		"""Get the local meta data including modified time and sha1"""
		localmeta = self.manager.cachedir / metafile(path)
		if not localmeta.exists():
			return None, None
		mtime, sha = localmeta.read_text().strip().split('|', 1)
		return int(float(mtime)), sha

	def isCached(self, path):
		"""Tell if a path is cached locally"""
		local_mtime, local_sha = self.localMetadata(path)
		if not local_mtime:
			return False
		remote_mtime, remote_sha = self.remoteMetadata(path)
		if not remote_mtime:
			remote_mtime = int(datetime.now().timestamp())
		if remote_mtime > local_mtime:
			return False
		return local_sha == remote_sha

	def updateRemoteMetafile(self, path):
		"""Update remote meta file"""

	def updateLocalMetafile(self, path, remotemeta = None):
		"""Update local metafile"""
		localmeta = self.manager.cachedir / metafile(path)
		with localmeta.open('w') as fmeta:
			if remotemeta:
				fmeta.write(remotemeta)
			else:
				fmeta.write('%s|%s' % (
					int((self.manager.cachedir / metafile(path)).stat().st_mtime),
					sha1(self.manager.local(path))))

	def download(self, path):
		"""Download remote file to local"""
		# make sure directories have been created
		self.manager.local(path).parent.mkdir(exist_ok=True, parents=True)
		self.manager.save(path)

		meta = metafile(path)
		if not self.manager.exists(meta):
			self.updateLocalMetafile(path)
			self.updateRemoteMetafile(path)
		else:
			self.updateLocalMetafile(path, remotemeta = self.manager.text(meta))

	def get(self, path):
		"""Get the local path of the file.
		If it's not cached, download it."""
		if not self.isCached(path):
			self.download(path)
		return self.manager.local(path)

class RemoteDir(RemoteFile):
	"""A remote directory manager"""

	def listDir(self, path): # pragma: no cover
		"""List the directories"""
		raise NotImplementedError()

	def download(self, path):
		"""Download a remote directory"""
		self.manager.local(path).mkdir(exist_ok = True, parents = True)
		paths = self.listDir(path)
		for path, ptype in paths:
			if ptype == 'dir':
				self.download(path)
			else:
				super().download(path)

class RemoteData:
	"""Base class for remote data"""

	def __init__(self, conf, manager = RemoteManager,
		fileclass = RemoteFile, dirclass = RemoteDir):
		self.manager = manager(conf)
		self.remotefile = fileclass(conf, self.manager)
		self.remotedir  = dirclass(conf, self.manager)

	# pylint: disable=unused-argument
	def gettype(self, path): # pragma: no cover
		"""Get the type of the path"""
		return 'file'

	def get(self, path):
		"""Get the local path, if file not cached, download it"""
		if self.gettype(path) == 'dir':
			return self.remotedir.get(path)
		return self.remotefile.get(path)

	def clear(self):
		"""Clear the cache"""
		for path in self.manager.cachedir.glob('*'):
			try:
				if path.is_dir():
					shutil.rmtree(path)
				else:
					path.unlink()
			except (PermissionError, OSError): # pragma: no cover
				pass

	def remove(self, path, clean = False):
		"""Remove the local cache"""
		self.manager.remove(path, clean)

class GithubManager(RemoteManager):
	"""A manager to download/query remote files"""

	def __init__(self, conf):
		super().__init__(conf)
		self.repos  = ''
		self.branch = 'master'

	def _init(self):
		repos = self.conf['repos'].rstrip('/')
		slashcount = repos.count('/')
		if slashcount not in (1,2):
			raise ValueError('Invalid github repository, need "<user>/<repos>"')
		if repos.count('/') == 1:
			self.branch = 'master'
			self.repos = repos
		else:
			rparts = repos.rpartition('/')
			self.repos, self.branch = rparts[0], rparts[-1]
		self.api = 'https://api.github.com/repos/%s/contents{/path}' % self.repos
		if 'token' in self.conf:
			self.session = requests.session()
			self.session.auth = (
				self.conf.get('user', repos.split('/')[0]),
				self.conf['token'])

	def get(self, path, **kwargs):
		"""Get a request object"""
		if not self.repos:
			self._init()
		kwargs['ref'] = kwargs.get('branch', self.branch)
		return super().get(path, **kwargs)

	def content(self, path, **kwargs):
		res = self.json(path, **kwargs)
		if 'message' in res and 'Not Found' in res['message']:
			raise ValueError('Resource not found at %r' % path)
		if 'message' in res and 'This API returns blobs up to 1 MB in size.' in res['message']:
			if '/' not in path:
				api = 'https://api.github.com/repos/%s/git/trees/%s' % (self.repos, self.branch)
				tree = self.session.get(api).json()['tree']
				blob = [t for t in tree if t['path'] == path][0]
				return self.session.get(blob['url']).content
			path = Path(path)
			api = 'https://api.github.com/repos/%s/git/trees/%s:%s' % (
				self.repos, self.branch, quote(str(path.parent)))
			tree = self.session.get(api).json()['tree']
			blob = [t for t in tree if t['path'] == path.name][0]
			return self.session.get(blob['url']).content
		return base64.b64decode(res['content'])

class GithubRemoteFile(RemoteFile):
	"""Remote file from github"""
	def updateRemoteMetafile(self, path):
		if not self.conf.get('token'):
			warnings.warn('No token provided, will always download %r without remote meta file.' % path)
			return
		# try to upload a metafile
		lmeta = self.manager.cachedir / metafile(path)
		api = 'https://api.github.com/repos/%s/contents/%s' % (self.manager.repos, metafile(path))
		res = self.manager.session.put(api, data = dict(
			message   = 'Create metafile for %s' % path,
			content   = base64.b64encode(lmeta.read_bytes()).decode(),
			sha       = sha1(lmeta),
			branch    = self.manager.branch,
			committer = {
				"name": self.conf.get('uesr', self.conf['repos'].split('/')[0]),
				"email": self.conf.get('email', '')
			}
		))

		if res.status_code != 201:
			warnings.warn(
				'Cannot put meta information for %r, will always download it.\n'
				'RESPONSE: %s' % (path, res.text))

class GithubRemoteDir(RemoteDir, GithubRemoteFile):
	"""Remote directory from github"""

	def listDir(self, path):
		"""List directory of remote path"""
		req = self.manager.json(path)
		if not isinstance(req, list):
			raise ValueError('Not a directory or failed to fetch information: %r\n'
				'RESPONSE: %r' % (path, req))
		ret = []
		for item in req:
			if item['name'].endswith('.meta'):
				continue
			ret.append((item['path'], item['type']))
		return ret

class GithubRemoteData(RemoteData):
	"""Remote data from Github"""
	def __init__(self, conf):
		super().__init__(conf, GithubManager, GithubRemoteFile, GithubRemoteDir)

	def gettype(self, path):
		req = self.manager.get(path).text
		return 'dir' if req[0] == '[' else 'file'

if CONFIG.source == 'github':
	remotedata = GithubRemoteData(CONFIG) # pylint: disable=invalid-name
else: # pragma: no cover
	remotedata = RemoteData(CONFIG) # pylint: disable=invalid-name
