from pathlib import Path


def project_names():
	projdir = Path(__file__).parent
	return [ d.stem for d in projdir.iterdir()
	                if not d.name.startswith('_' ) ]