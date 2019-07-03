"""Accessing and caching remote data"""
import warnings
import hashlib
import shutil
import json
import base64
from datetime import datetime
from pathlib import Path
import requests
from simpleconf import Config

CONFIG = Config(with_profile = False)
CONFIG._load(dict(
	source   = 'github',
	cachedir = '/tmp/',
	repos    = None
), '~/.remotedata.yaml', './.remotedata.yaml', 'REMOTEDATA.osenv')

def sha1File(filepath):
	"""Get sha1 for file"""
	bufsize = 2**16
	ret = hashlib.sha1()
	with open(filepath, 'rb') as fhand:
		block = fhand.read(bufsize)
		while len(block) != 0:
			ret.update(block)
			block = fhand.read(bufsize)
	return ret.hexdigest()

def sha1(filepath):
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

class RemoteFile:

	@property
	def url(self): # pragma: no cover
		raise NotImplementedError()

	@staticmethod
	def metafile(filename):
		if '/' in filename:
			filename = Path(filename)
			return str(filename.parent / ('.' + filename.name + '.meta'))
		return '.' + filename + '.meta'

	def __init__(self, conf):
		self.conf     = conf
		self.cachedir = Path(conf.cachedir)

	def sha1(self, filename):
		return sha1(self.cachedir / filename)

	def remoteMetafile(self, filename):
		return self.url.replace('{{file}}', RemoteFile.metafile(filename))

	def localMetafile(self, filename):
		return self.cachedir / RemoteFile.metafile(filename)

	def remoteMetaExists(self, filename):
		ret = requests.get(self.remoteMetafile(filename))
		return ret.status_code == 200, ret

	def localMetaExists(self, filename):
		return self.localMetafile(filename).exists()

	def remoteMetadata(self, filename):
		rmexists, req = self.remoteMetaExists(filename)
		if not rmexists:
			return None, None
		mtime, sha = req.text.strip().split('|', 1)
		return int(float(mtime)), sha

	def localMetadata(self, filename):
		if not self.localMetaExists(filename):
			return None, None
		mtime, sha = self.localMetafile(filename).read_text().strip().split('|', 1)
		return int(float(mtime)), sha

	def isCached(self, filename):
		local_mtime, local_sha = self.localMetadata(filename)
		if not local_mtime:
			return False
		remote_mtime, remote_sha = self.remoteMetadata(filename)
		if not remote_mtime:
			warnings.warn(
				'Cannot fetch meta information for %r, will always download it.' % filename)
			remote_mtime = int(datetime.now().timestamp())
		if remote_mtime > local_mtime:
			return False
		return local_sha == remote_sha

	def updateRemoteMetafile(self, filename): # pragma: no cover
		raise NotImplementedError()

	def updateLocalMetafile(self, filename, remote = None):
		with self.localMetafile(filename).open('w') as fmeta:
			if remote:
				fmeta.write(remote)
			else:
				fmeta.write('%s|%s' % (
					int((self.cachedir / filename).stat().st_mtime),
					self.sha1(filename)))

	def download(self, filename):
		localfile  = self.cachedir / filename
		remotefile = self.url.replace('{{file}}', filename)
		# make sure directories have been created
		localfile.parent.mkdir(exist_ok=True, parents=True)
		r = requests.get(remotefile)
		with localfile.open('wb') as fhand:
			fhand.write(r.content)

		rmexists, req = self.remoteMetaExists(filename)
		if not rmexists:
			self.updateLocalMetafile(filename)
			self.updateRemoteMetafile(filename)
		else:
			self.updateLocalMetafile(filename, remote = req.text)

	def get(self, filename):
		if not self.isCached(filename):
			self.download(filename)
		return self.cachedir / filename

class RemoteDir(RemoteFile):

	def listDir(self, filename): # pragma: no cover
		raise NotImplementedError()

	def download(self, filename):
		localdir = self.cachedir / filename
		# make sure local dir is created
		localdir.mkdir(exists_ok = True, parents = True)
		paths = self.listDir(filename)
		for path, ptype in paths:
			if ptype == 'dir':
				self.download(path)
			else:
				super().download(path)

class RemoteData:
	"""Base class for remote data"""

	def __init__(self, conf, fileclass = RemoteFile, dirclass = RemoteDir):
		self.remotefile = fileclass(conf)
		self.remotedir  = dirclass(conf)

	def gettype(self, filename):
		return 'file'

	def get(self, filename):
		if self.gettype(filename) == 'dir':
			return self.remotedir.get(filename)
		return self.remotefile.get(filename)

	def clear(self):
		for path in self.remotefile.cachedir.glob('*'):
			try:
				if path.is_dir():
					shutil.rmtree(path)
				else:
					path.unlink()
			except (PermissionError, OSError):
				pass

	def remove(self, filename):
		if self.remotefile.localMetaExists(filename):
			self.remotefile.localMetafile(filename).unlink()

class GithubRemoteFile(RemoteFile):

	@property
	def repository(self):
		repos = self.conf.repos.rstrip('/')
		slashcount = repos.count('/')
		if slashcount not in (1,2):
			raise ValueError('Invalid github repository, need "<user>/<repos>"')
		if repos.count('/') == 1:
			return repos, 'master'
		rparts = repos.rpartition('/')
		return rparts[0], rparts[-1]

	@property
	def url(self):
		if not self.conf.repos:
			raise ValueError('Github remote data needs "repos" to be set.')
		return 'https://raw.githubusercontent.com/%s/%s/{{file}}' % (self.repository)

	def updateRemoteMetafile(self, filename):
		# try to upload a metafile
		repos, branch = self.repository
		api = 'https://api.github.com/repos/%s/contents/%s' % (repos, filename)
		metafile = self.localMetafile(filename)
		requests.put(api, data = dict(
			message = 'Create metafile for %s' % filename,
			content = base64.b64encode(metafile.read_bytes()),
			sha     = sha1(metafile),
			branch  = branch,
			committer = {}
		))

class GithubRemoteDir(GithubRemoteFile):

	def listDir(self, filename):
		repos, branch = self.repository
		api = 'https://api.github.com/repos/%s/contents/%s' % (repos, filename)
		req = requests.get(api, params = {'ref': 'branch'})
		data = json.loads(req.text)
		ret = []
		for item in data:
			ret.append((item['path'], item['type']))
		return ret

class GithubRemoteData(RemoteData):

	def __init__(self, conf, fileclass=GithubRemoteFile, dirclass=GithubRemoteDir):
		self.contents = None
		return super().__init__(conf, fileclass=fileclass, dirclass=dirclass)

	def gettype(self, filename):
		repos, branch = self.remotefile.repository
		api = 'https://api.github.com/repos/%s/contents/%s' % (repos, filename)
		req = requests.get(api, params = {'ref': 'branch'})
		if req.text[0] == '[':
			return 'dir'
		return 'file'

if CONFIG.source == 'github':
	remotedata = GithubRemoteData(CONFIG)
else: # pragma: no cover
	remotedata = RemoteData(CONFIG)

