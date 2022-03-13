from pathlib import Path


def data_source_names():
	srcdir = Path(__file__).parent
	return [ d.stem for d in srcdir.iterdir()
	                if not d.name.startswith('_' ) ]