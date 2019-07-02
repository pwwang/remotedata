import warnings
import requests
import hashlib
from datetime import datetime
from pathlib import Path
from simpleconf import Config

config = Config()
config._load({
	source   = 'github',
	cachedir = '/tmp/',
	repos    = None
}, '~/.remotedata.yaml', './.remotedata.yaml', 'REMOTEDATA.osenv')

class RemoteData:

	@staticmethod
	def sha1(filepath):
		bufsize = 2**16
		ret = hashlib.sha1()
		with open(filepath, 'rb') as f:
			block = f.read(bufsize)
			while len(block) != 0:
				ret.update(block)
				block = f.read(bufsize)
		return ret.hexdigest()

	def __init__(self, conf):
		self.conf     = conf
		self.cachedir = Path(conf.cachedir)

	@property
	def url(self):
		raise NotImplementedError()

	def remoteMeta(self, filename):
		meta = self.url.replace('{file}', '.' + filename + '.meta')
		r = requests.get(meta)
		if r.status_code == 404:
			return None, None
		return r.content.split('|', 1)

	def localMeta(self, filename):
		meta = self.cachedir / ('.' + filename + '.meta')
		if not meta.exists() or not (self.cachedir / filename).exists():
			return None, None
		return meta.read_text().split('|', 1)

	def isCached(self, filename):
		local_mtime, local_sha = self.localMeta()
		if not local_mtime:
			return False
		remote_mtime, remote_sha = self.remoteMeta()
		if not remote_mtime:
			warnings.warn(
				'Cannot fetch meta information for %r, will always download it.' % filename)
		if int(remote_mtime) > int(local_mtime):
			return False
		return local_sha == remote_sha

	def download(self, filename):
		localfile = self.cachedir / filename
		fileurl   = self.url.replace('{file}', filename)
		r = requests.get(fileurl)
		with localfile.open('wb') as f:
			f.write(r.content)

		meta = self.url.replace('{file}', '.' + filename + '.meta')
		localmeta = self.cachedir / ('.' + filename + '.meta')
		r = requests.get(meta)
		if r.status_code == 404:
			mtime = datetime.now().timestamp()
			sha   = RemoteData.sha1(localfile)
			with localmeta.open('wb') as f:
				r.write(str(mtime) + '|' + sha)
		else:
			with localmeta.open('wb') as f:
				f.write(r.content)

	def get(self, filename):
		if not self.isCached():
			self.download(filename)
		return self.cachedir / filename

class GithubRemoteData(RemoteData):

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
		return 'https://raw.githubusercontent.com/%s/%s/{file}'.format(self.repository)


if config.source == 'github':
	remotedata = GithubRemoteData(config)
else:
	remotedata = RemoteData(config)


