import pytest
from datetime import datetime
from pathlib import Path
from remotedata import remotedata, RemoteData, sha1


@pytest.fixture
def datadir():
	return Path(__file__).resolve().parent / 'data'

@pytest.fixture(scope="function")
def rdfile(tmp_path):
	remotedata.remotefile.cachedir = tmp_path
	remotedata.remotefile.conf.repos = 'pwwang/remotedata'
	return remotedata.remotefile

def test_remotemeta(datadir, rdfile):
	mtime, sha = rdfile.remoteMetadata('tests/data/file.txt')
	metafile = datadir / '.file.txt.meta'
	mtime2, sha2 = metafile.read_text().strip().split('|', 1)
	assert mtime == int(mtime2)
	assert sha == sha2

	assert rdfile.remoteMetadata('__not__exists__') == (None, None)

def test_localmeta(datadir, rdfile):
	assert rdfile.localMetadata('__not__exists__') == (None, None)
	localmetafile = rdfile.cachedir / 'tests' / 'data' / '.file.txt.meta'
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
	localmetafile = rdfile.cachedir / 'tests' / 'data' / '.file.txt.meta'
	localmetafile2 = rdfile.cachedir / 'tests' / 'data' / '.file2.txt.meta'
	if localmetafile.exists():
		localmetafile.unlink()
	assert not rdfile.isCached('tests/data/file.txt')

	localmetafile2.parent.mkdir(parents = True, exist_ok = True)
	(localmetafile2.parent / 'file2.txt').write_text('')
	with open(localmetafile2, 'w') as f1, \
		open(datadir / '.file.txt.meta') as f2:
		f1.write(f2.read())
	with pytest.warns(UserWarning):
		assert not rdfile.isCached('tests/data/file2.txt')

	with open(localmetafile, 'w') as f1, \
		open(datadir / '.file.txt.meta') as f2:
		f1.write(f2.read())
	(localmetafile.parent / 'file.txt').write_text('')
	assert rdfile.isCached('tests/data/file.txt')

def test_download(datadir, rdfile):
	rdfile.download('tests/data/file.txt')
	assert (rdfile.cachedir / 'tests/data/file.txt').read_text() == (datadir / 'file.txt').read_text()

	rdfile.download('tests/data/nometa.txt')
	now = datetime.now().timestamp()
	sha = sha1(rdfile.cachedir / 'tests/data/nometa.txt')
	mtime, shahash = rdfile.localMetadata('tests/data/nometa.txt')
	assert now - float(mtime) < 30
	assert shahash == sha

def test_get_remove(datadir, rdfile, tmp_path):
	remotedata.clear()
	filetxt = rdfile.get('tests/data/file.txt')
	assert filetxt == rdfile.cachedir / 'tests/data/file.txt'
	assert filetxt.exists()
	assert rdfile.isCached('tests/data/file.txt')
	remotedata.remove('tests/data/file.txt')
	assert not rdfile.isCached('tests/data/file.txt')
	assert rdfile.cachedir.exists()
	assert tmp_path.exists()

def test_clear(rdfile):
	rdfile.get('tests/data/file.txt')
	nometa = rdfile.get('tests/data/nometa.txt')
	assert rdfile.isCached('tests/data/file.txt')
	assert not rdfile.isCached('tests/data/nometa.txt')
	assert nometa.exists()
	(rdfile.cachedir / 'file').write_text('')
	remotedata.clear()
	assert not rdfile.isCached('tests/data/file.txt')
	assert not rdfile.isCached('tests/data/nometa.txt')

def test_repository(rdfile):
	rdfile.conf.repos = ''
	with pytest.raises(ValueError):
		rdfile.repository
	rdfile.conf.repos = 'pwwang/remotedata/develop'
	assert rdfile.repository == ('pwwang/remotedata', 'develop')

def test_url(rdfile):
	rdfile.conf.repos = ''
	with pytest.raises(ValueError):
		rdfile.url

def test_sha1(datadir):
	assert sha1(datadir / 'dir') == '6e3d422104ceecf644d494550b7f9d32b8fd48de'
