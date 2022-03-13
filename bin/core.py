#!/usr/bin/env python3

import pickle
import sys
from pathlib import Path


DEBUG = True


def load_snapshot_data(tag, name):
	fname = Path(tag) / f'{name}.pickle'
	with open(fname, 'rb') as f:
		return pickle.load(f)
