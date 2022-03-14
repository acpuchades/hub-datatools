from pathlib import Path
from typing  import List


def project_names() -> List[str]:
	projdir = Path(__file__).parent
	return [ d.stem for d in projdir.iterdir()
	                if not d.name.startswith('_' ) ]