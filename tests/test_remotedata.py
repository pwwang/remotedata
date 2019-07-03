import pytest
from pathlib import Path
from remotedata import remotedata


@pytest.fixture
def datadir():
	return Path(__file__).resolve().parent / 'data'

@pytest.fixture
def rd(tmp_path):
	remotedata.cachedir = tmp_path
	remotedata.conf.repos = 'pwwang/remotedata'
	return remotedata

def test_remotemeta(datadir, rd):
	x = rd.remoteMeta('tests/data/file.txt')
	metafile = datadir / '.file.txt.meta'
	assert x == metafile.read_text().strip().split('|', 1)

	assert rd.remoteMeta('__not__exists__') == (None, None)

def test_localmeta(datadir, rd):
	assert rd.localMeta('__not__exists__') == (None, None)
	localmetafile = rd.cachedir / 'tests' / 'data' / '.file.txt.meta'
	localmetafile.parent.mkdir(exist_ok=True, parents=True)
	(localmetafile.parent / 'file.txt').write_text('')
	with open(localmetafile, 'w') as f1, \
		open(datadir / '.file.txt.meta') as f2:
		f1.write(f2.read())
	metafile = datadir / '.file.txt.meta'
	assert rd.localMeta('tests/data/file.txt') == metafile.read_text().strip().split('|', 1)

def test_iscached(datadir, rd):
	localmetafile = rd.cachedir / 'tests' / 'data' / '.file.txt.meta'
	localmetafile2 = rd.cachedir / 'tests' / 'data' / '.file2.txt.meta'
	if localmetafile.exists():
		localmetafile.unlink()
	assert not rd.isCached('tests/data/file.txt')

	localmetafile2.parent.mkdir(parents = True, exist_ok = True)
	(localmetafile2.parent / 'file2.txt').write_text('')
	with open(localmetafile2, 'w') as f1, \
		open(datadir / '.file.txt.meta') as f2:
		f1.write(f2.read())
	with pytest.warns(UserWarning):
		assert not rd.isCached('tests/data/file2.txt')

	with open(localmetafile, 'w') as f1, \
		open(datadir / '.file.txt.meta') as f2:
		f1.write(f2.read())
	(localmetafile.parent / 'file.txt').write_text('')
	assert rd.isCached('tests/data/file.txt')

def test_download(datadir, rd):
	rd.download('tests/data/file.txt')
	assert (rd.cachedir / 'tests/data/file.txt').read_text() == (datadir / 'file.txt').read_text()

