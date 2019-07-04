import pytest
from datetime import datetime
from pathlib import Path
from remotedata import remotedata, RemoteData, sha1, GithubManager, GithubRemoteData, CONFIG

@pytest.fixture
def datadir():
	return Path(__file__).resolve().parent / 'data'

@pytest.fixture(scope="function")
def rdfile(tmp_path):
	remotedata.manager.cachedir = tmp_path
	remotedata.manager.conf.repos = 'pwwang/remotedata'
	return remotedata.remotefile

@pytest.fixture(scope="function")
def rddir(tmp_path):
	remotedata.manager.cachedir = tmp_path
	remotedata.manager.conf.repos = 'pwwang/remotedata'
	return remotedata.remotedir

def test_remotemeta(datadir, rdfile):
	mtime, sha = rdfile.remoteMetadata('tests/data/file.txt')
	metafile = datadir / '.file.txt.meta'
	mtime2, sha2 = metafile.read_text().strip().split('|', 1)
	assert mtime == int(mtime2)
	assert sha == sha2

	assert rdfile.remoteMetadata('__not__exists__') == (None, None)

def test_localmeta(datadir, rdfile):
	assert rdfile.localMetadata('__not__exists__') == (None, None)
	localmetafile = rdfile.manager.cachedir / 'tests' / 'data' / '.file.txt.meta'
	localmetafile.parent.mkdir(exist_ok=True, parents=True)
	(localmetafile.parent / 'file.txt').write_text('')
	with open(localmetafile, 'w') as f1, \
		open(datadir / '.file.txt.meta') as f2:
		f1.write(f2.read())
	metafile = datadir / '.file.txt.meta'
	mtime, sha = rdfile.localMetadata('tests/data/file.txt')
	mtime2, sha2 = metafile.read_text().strip().split('|', 1)
	assert mtime == int(mtime2)
	assert sha == sha2

def test_iscached(datadir, rdfile):
	localmetafile = rdfile.manager.cachedir / 'tests' / 'data' / '.file.txt.meta'
	localmetafile2 = rdfile.manager.cachedir / 'tests' / 'data' / '.file2.txt.meta'
	if localmetafile.exists():
		localmetafile.unlink()
	assert not rdfile.isCached('tests/data/file.txt')

	localmetafile2.parent.mkdir(parents = True, exist_ok = True)
	(localmetafile2.parent / 'file2.txt').write_text('')
	with open(localmetafile2, 'w') as f1, \
		open(datadir / '.file.txt.meta') as f2:
		f1.write(f2.read())

	with open(localmetafile, 'w') as f1, \
		open(datadir / '.file.txt.meta') as f2:
		f1.write(f2.read())
	(localmetafile.parent / 'file.txt').write_text('')
	assert rdfile.isCached('tests/data/file.txt')

def test_iscached_updated(rdfile):
	localmetafile = rdfile.manager.cachedir / 'tests' / 'data' / '.file.txt.meta'
	localfile     = rdfile.manager.cachedir / 'tests' / 'data' / 'file.txt'
	(rdfile.manager.cachedir / 'tests' / 'data').mkdir(parents = True, exist_ok = True)
	localmetafile.write_text('1|')
	localfile.write_text('')
	assert not rdfile.isCached('tests/data/file.txt')

def test_download(datadir, rdfile):
	rdfile.download('tests/data/file.txt')
	assert (rdfile.manager.cachedir / 'tests/data/file.txt').read_text() == (datadir / 'file.txt').read_text()

	rdfile.download('tests/data/nometa.txt')
	now = datetime.now().timestamp()
	sha = sha1(rdfile.manager.cachedir / 'tests/data/nometa.txt')
	mtime, shahash = rdfile.localMetadata('tests/data/nometa.txt')
	assert now - float(mtime) < 30
	assert shahash == sha

def test_get_remove(datadir, rdfile, tmp_path):
	remotedata.clear()
	filetxt = rdfile.get('tests/data/file.txt')
	assert filetxt == rdfile.manager.cachedir / 'tests/data/file.txt'
	assert filetxt.exists()
	assert rdfile.isCached('tests/data/file.txt')
	remotedata.remove('tests/data/file.txt', True)
	assert not rdfile.isCached('tests/data/file.txt')
	assert rdfile.manager.cachedir.exists()
	assert tmp_path.exists()

def test_clear(rdfile):
	rdfile.get('tests/data/file.txt')
	nometa = rdfile.get('tests/data/nometa.txt')
	assert rdfile.isCached('tests/data/file.txt')
	assert not rdfile.isCached('tests/data/nometa.txt')
	assert nometa.exists()
	(rdfile.manager.cachedir / 'file').write_text('')
	remotedata.clear()
	assert not rdfile.isCached('tests/data/file.txt')
	assert not rdfile.isCached('tests/data/nometa.txt')

def test_listdir(rddir):
	files = rddir.listDir('tests/data/')
	assert len(files) == 3
	assert ('tests/data/dir', 'dir') in files
	assert ('tests/data/file.txt', 'file') in files
	assert ('tests/data/nometa.txt', 'file') in files

	with pytest.raises(ValueError):
		rddir.listDir('tests/data/file.txt')

def test_download_dir(rddir):
	rddir.download('tests/data')
	assert (rddir.manager.cachedir / 'tests/data/dir').is_dir()
	assert (rddir.manager.cachedir / 'tests/data/dir/file2.txt').is_file()
	assert (rddir.manager.cachedir / 'tests/data/file.txt').is_file()
	assert (rddir.manager.cachedir / 'tests/data/nometa.txt').is_file()

def test_sha1(datadir):
	assert sha1(datadir) == '94e0773ad948f2debd6318f9c648ef1565d87639'

def test_github(rdfile, rddir, tmp_path):
	remotedata.clear()
	remotedata.get('tests/data/file.txt')
	assert (remotedata.manager.cachedir / 'tests/data/file.txt').is_file()

	remotedata.get('tests/data/dir')
	assert (remotedata.manager.cachedir / 'tests/data/dir').is_dir()
	assert (remotedata.manager.cachedir / 'tests/data/dir/file2.txt').is_file()

	with pytest.raises(ValueError):
		remotedata.get('__not__exists__')

	ghm = GithubManager(dict(repos = '', cachedir = tmp_path))
	with pytest.raises(ValueError):
		ghm._init()

	ghm = GithubManager(dict(repos = 'pwwang/remotedata/branch', cachedir = tmp_path))
	ghm._init()
	assert ghm.repos == 'pwwang/remotedata'
	assert ghm.branch == 'branch'

	rdfile.conf['token'] = ''
	with pytest.warns(UserWarning):
		rdfile.updateRemoteMetafile('')


def test_large_file(tmp_path):
	config = CONFIG.copy()
	config.update(dict(cachedir = tmp_path, repos = 'simonw/github-large-file-test'))
	rdata = GithubRemoteData(config)
	largefile = rdata.get('1.5mb.txt')
	assert largefile.stat().st_size > 1200000

	config.update(dict(cachedir = tmp_path, repos = 'RabadanLab/arcasHLA'))
	# try large file under directory
	rdata1 = GithubRemoteData(config)
	largefile1 = rdata1.get('dat/info/hla_freq.tsv')
	assert largefile1.stat().st_size > 1000000

def test_nometa(rdfile):
	rdfile.download('tests/data/nometa.txt')
