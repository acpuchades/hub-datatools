from pathlib  import Path
from typing   import List


def data_source_names() -> List[str]:
	srcdir = Path(__file__).parent
	return [ d.stem for d in srcdir.iterdir()
	                if not d.name.startswith('_' ) ]