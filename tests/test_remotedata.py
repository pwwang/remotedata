import pytest
from pathlib import Path
import requests
from simpleconf import Config
from remotedata import remotedata, GithubRemoteData, GithubRemoteFile, _hashfile

def setup_module(module):
	pytest.config = Config()
	pytest.config._load({
		'default': {
			'repos': 'pwwang/remotedata',
		},
		'standard': {
			'source': 'github'
		},
		'remotedata': {
			'source': 'github.remotedata'
		},
		'invalidrepos': {
			'source': 'github',
			'repos': 'a'
		},
		'withbranch': {
			'source': 'github',
			'repos': 'pwwang/remotedata/notmaster'
		}
	}, '~/.remotedata.yaml', 'REMOTEDATA.osenv') # get token from osenv

@pytest.fixture
def config_standard(tmp_path):
	print('\nCACHEDIR: %s\n' % tmp_path)
	ret = pytest.config._use('standard', copy = True)
	ret.cachedir = tmp_path
	return ret

@pytest.fixture
def config_remotedata(tmp_path):
	ret = pytest.config._use('remotedata', copy = True)
	ret.cachedir = tmp_path
	return ret

@pytest.fixture
def config_invalidrepos(tmp_path):
	ret = pytest.config._use('invalidrepos', copy = True)
	ret.cachedir = tmp_path
	return ret

@pytest.fixture
def config_withbranch(tmp_path):
	ret = pytest.config._use('withbranch', copy = True)
	ret.cachedir = tmp_path
	return ret

def test_githubremotedata_noreqkey():
	with pytest.raises(KeyError):
		remotedata({'source': 'github'})

def test_githubremotedata_invalidrepos(config_invalidrepos):
	with pytest.raises(ValueError):
		remotedata(config_invalidrepos)

def test_githubremotedata_withbranch(config_withbranch, tmp_path):
	ghremotedata = remotedata(config_withbranch)
	assert isinstance(ghremotedata, GithubRemoteData)
	assert ghremotedata.config['branch'] == 'notmaster'
	assert ghremotedata.config['hashtype'] == 'git-sha'
	assert isinstance(ghremotedata.config['session'], requests.sessions.Session)
	assert ghremotedata.config['contents_api'] == 'https://api.github.com/repos/pwwang/remotedata/contents/'
	assert ghremotedata.config['blobs_api'] == 'https://api.github.com/repos/pwwang/remotedata/git/blobs/'
	assert ghremotedata.cachedir == tmp_path / 'github' / 'pwwang.remotedata@notmaster'
	assert ghremotedata.cachedir.is_dir()

def test_githubremotedata_standard(config_standard, tmp_path):
	ghremotedata = remotedata(config_standard)
	assert isinstance(ghremotedata._fileobj('tests/data/test.txt'), GithubRemoteFile)
	path = 'tests/data/test.txt'
	ghobj = ghremotedata._fileobj(path)
	assert ghobj.local == ghobj.cachedir / 'tests/data/test.txt'
	assert ghobj.localHashFile == ghobj.cachedir / '.remotedata-hash/tests.data.test.txt.hash'
	assert ghobj.remoteHash() == ghobj.json()['sha']
	ghobj.download()
	assert ghobj.local.is_file()
	ghobj.updateHash()
	assert ghobj.isCached()

	ghremotedata.remove(path)
	assert not ghobj.isCached()

	ghremotedata.get(path)
	assert ghobj.isCached()

	ghremotedata.get('pyproject.toml')
	ghremotedata.clear()
	assert not ghobj.isCached()

	with pytest.raises(ValueError) as error:
		ghremotedata.get('NoSuchFile')
	assert 'Resource not found' in str(error.value)

	# large file
	path = 'tests/data/largefile.txt'
	ghobj = ghremotedata._fileobj(path)
	ghremotedata.get(path)
	assert ghobj.local.is_file()





