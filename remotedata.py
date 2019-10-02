"""Accessing and caching remote data"""
import hashlib
import shutil
import base64
from functools import lru_cache
from pathlib import Path
import requests
import cmdy

def _requireConfig(config, key):
	if key not in config:
		raise KeyError('%s required in configuration.' % key)

def _hashfile(path, method = 'git-sha'):
	if method == 'git-sha':
		return cmdy.git('hash-object', str(path)).split()[0]
	# pragma: no cover, will be used in the future.
	if method == 'sha' or method == 'sha1': # pragma: no cover
		return cmdy.sha1sum(str(path)).split()[0]
	if method == 'sha256': # pragma: no cover
		return cmdy.sha256sum(str(path)).split()[0]
	if method == 'md5': # pragma: no cover
		return cmdy.md5sum(str(path)).split()[0]
	raise ValueError('Unsupported hash type.') # pragma: no cover

class RemoteFile:
	"""APIs to access, operate and download remote files."""

	def __init__(self, path, cachedir, **config):
		self.path     = Path(path)
		self.cachedir = Path(cachedir)
		self.config   = config

	@property
	def local(self):
		"""Get corresponding local path of the file"""
		return self.cachedir.joinpath(self.path)

	@property
	def localHashFile(self):
		"""Get local hash file"""
		return self.cachedir / '.remotedata-hash' / (str(self.path).replace('/', '.') + '.hash')

	def remoteHash(self): # pragma: no cover
		"""Get hash from remote"""
		raise NotImplementedError("Don't know how to get remote hash.")

	def hash(self):
		"""Get hash for file"""
		if not self.local.exists():
			return ''
		return _hashfile(self.local, self.config['hashtype'])

	def localHash(self):
		"""Get local hash"""
		if self.localHashFile.exists():
			return self.localHashFile.read_text().strip()

		if not self.localHashFile.exists():
			self.localHashFile.parent.mkdir(parents = True, exist_ok = True)
		hashsum = self.hash()
		self.localHashFile.write_text(hashsum)
		return hashsum

	def isCached(self):
		"""Tell if a file is cached"""
		return self.localHash() == self.remoteHash()

	def updateHash(self):
		"""Update local hash"""
		if not self.localHashFile.exists():
			self.localHashFile.parent.mkdir(parents = True, exist_ok = True)
		self.localHashFile.write_text(self.hash())

	def download(self): # pragma: no cover
		"""Download remote file to local"""
		raise NotImplementedError("Don't know how to download remote file.")

	def get(self):
		"""Get the file, use it directly if cached, otherwise download it"""
		if not self.isCached():
			self.download()
			self.updateHash()
		return self.local

class RemoteData:

	def __init__(self, config):
		self.fileclass = RemoteFile
		self.config    = config
		self.cachedir  = config['cachedir']

	def _fileobj(self, path):
		return self.fileclass(
			path, self.cachedir,
			**{key:val for key,val in self.config.items() if key != 'cachedir'})

	def get(self, path):
		return self._fileobj(path).get()

	def clear(self):
		for path in self.cachedir.glob('*'):
			try:
				if path.is_dir():
					shutil.rmtree(path)
				else:
					path.unlink()
			except (PermissionError, OSError): # pragma: no cover
				pass

	def remove(self, path):
		fileobj = self._fileobj(path)
		if fileobj.local.exists():
			fileobj.local.unlink()
		if fileobj.localHashFile.exists():
			fileobj.localHashFile.unlink()

class GithubRemoteFile(RemoteFile):

	@lru_cache()
	def json(self, api = None, withpath = False):
		api    = api or self.config['contents_api']
		return self.config['session'].get(
			api + (str(self.path) if not withpath else ''),
			params = {'ref': self.config['branch']}).json()

	def remoteHash(self):
		return self.json().get('sha', 'SHA-FETCHING-FAILURE')

	def download(self):
		json = self.json()
		if 'message' in json and 'Not Found' in json['message']:
			raise ValueError('Resource not found at %r' % self.path)
		if 'message' in json and 'This API returns blobs up to 1 MB in size.' in json['message']:
			parent_json = self.json(
				api = self.config['contents_api'] + str(self.path.parent),
				withpath = True)
			for item in parent_json:
				if item['name'] == self.path.name:
					json = self.json(api = self.config['blobs_api'] + item['sha'], withpath = True)
					break
		self.local.parent.mkdir(exist_ok = True, parents = True)
		with self.local.open('wb') as f:
			f.write(base64.b64decode(json['content']))

class GithubRemoteData(RemoteData):

	def __init__(self, config):
		super().__init__(config)

		_requireConfig(config, 'repos')

		self.fileclass = GithubRemoteFile
		self.config    = config.copy()
		token     = config.get('token')
		branch    = 'master'

		repos = config['repos']
		slashcount = repos.count('/')
		if slashcount not in (1,2):
			raise ValueError('Invalid github repository, expect "<user>/<repos>"')
		if repos.count('/') == 1:
			branch = 'master'
		else:
			rparts = repos.rpartition('/')
			repos, branch = rparts[0], rparts[-1]
		session = requests.session()
		if token:
			session.auth = (config.get('user', repos.split('/')[0]), token)

		self.config['session']      = session
		self.config['branch']       = branch
		self.config['contents_api'] = 'https://api.github.com/repos/%s/contents/' % repos # + path
		self.config['blobs_api']    = 'https://api.github.com/repos/%s/git/blobs/' % repos   # + sha
		self.config['hashtype']     = config.get('hashtype', 'git-sha')

		self.cachedir = Path(config['cachedir']).joinpath(
			'github',
			repos.replace('/', '.') + '@' + branch)
		self.cachedir.mkdir(exist_ok = True, parents = True)

class GithubRDRemoteFile(GithubRemoteFile):
	"""Remote file from github remotedata format repository"""
	@property
	def remoteHashFile(self):
		"""Get remote hash file: .remotedata-hash/path.hash"""
		return '.remotedata-hash/%s.hash' % str(self.path).replace('/', '.')

	def remoteHash(self):
		shafile = GithubRemoteFile(
			self.remoteHashFile,
			self.cachedir,
			session      = self.config['session'],
			branch       = self.config['branch'],
			contents_api = self.config['contents_api'])
		return base64.b64decode(shafile.json()['content'])

class GithubRDRemoteData(GithubRemoteData):
	"""Github repository with remotedata format"""
	def __init__(self, config):
		super().__init__(config)
		self.fileclass = GithubRDRemoteFile

def remotedata(config):
	_requireConfig(config, 'source')
	_requireConfig(config, 'cachedir')
	if config['source'] == 'github':
		return GithubRemoteData(config)
	if config['source'] == 'github.remotedata':
		return GithubRDRemoteData(config)
	# if config['source'] == 'restful':
	# 	return RestfulRemoteData(config)
	# if config['source'] == 'ssh':
	# 	return SshRemoteData(config)
	raise ValueError('Unsupported source: %s.' % config['source'])

def console(): # pragma: no cover
	"""Generate shas in .remotedata-hash"""
	import sys
	from fnmatch import fnmatch
	if len(sys.argv) < 2:
		workdir, hashtype = Path('.'), 'git-sha'
	elif sys.argv[1] in ('-h', '--help'):
		print('Usage: remotedata-hash [dir=./] [hashtype=git-sha(default)|sha|sha1|sha256|md5]')
		sys.exit(1)
	elif len(sys.argv) < 3:
		workdir, hashtype = Path(sys.argv[1]), 'git-sha'
	else:
		workdir, hashtype = Path(sys.argv[1]), sys.argv[2]
	gitignore = workdir / '.gitignore'
	patterns = ['.remotedata-hash/', '.git/']
	if gitignore.is_file():
		with gitignore.open('r') as f:
			for line in f:
				line = line.strip()
				if not line or line.startswith('#'):
					continue
				patterns.append(line)
	patterns = ['**/' + pattern if '/' not in pattern else \
		pattern + '*' if pattern.endswith('/') else \
		pattern for pattern in patterns]
	ignored = lambda fn: any(fnmatch(fn, pattern) for pattern in patterns)
	shadir = workdir / '.remotedata-hash'
	shadir.mkdir(exist_ok = True)
	print('- WORKDING DIRECTORY: %s' % workdir)
	print('- HASH TYPE: %s\n' % hashtype)
	for path in workdir.rglob('*'):
		if not path.is_file():
			continue
		rpath = path.relative_to(workdir)
		if ignored(rpath):
			continue
		print('- Working on %s' % rpath)
		shafile = workdir / '.remotedata-hash' / (str(rpath).replace('/', '.') + '.hash')
		oldsha = shafile.read_text() if shafile.is_file() else '[]'
		newsha = _hashfile(path, hashtype)
		if oldsha != newsha:
			shafile.write_text(newsha)
			print('  UPDATED: %s -> %s (%s)' % (oldsha, newsha, shafile.relative_to(workdir)))
		else:
			print('  UNCHANGED: %s (%s)' % (oldsha, shafile.relative_to(workdir)))
	print('- Done!')
