import pandas as pd


NA_VALUES = ('', '-', 'NS/NC', 'NA')

TRUE_VALUES  = ('Sí', 'TRUE')
FALSE_VALUES = ('No', 'FALSE')


def transform_opt(data, na_values=NA_VALUES, inplace=False, **kwargs):
	if not inplace:
		data = data.copy()

	data.replace(na_values, None, inplace=True)
	return data


def transform_bool(data, true_values=TRUE_VALUES, false_values=FALSE_VALUES, inplace=False, **kwargs):
	if not inplace:
		data = data.copy()

	data.replace(true_values, True, inplace=True)
	data.replace(false_values, False, inplace=True)
	return data


def transform_strip(data, **kwargs):
	return data.str.strip()


def transform_fix_common_typos(data, **kwargs):
	data = data.str.replace(r'^º', '', regex=True)
	data = data.str.replace(r'º$', '', regex=True)
	return data


def transform_fix_date_typos(data, **kwargs):
	data = data.str.replace(r'^\?\?', '01', regex=True)
	data = data.str.replace(r'-+', '/', regex=True)
	data = data.str.replace(
		r'^(\d{1,2})/(\d{1,2})(\d{2,4})$', r'\1/\2/\3', regex=True)
	return data


def transform_date(data, yearfirst=False, dayfirst=True, format=None, exact=True, **kwargs):
	return pd.to_datetime(data, yearfirst=yearfirst, dayfirst=dayfirst, format=format, exact=exact)


def transform_number(data, errors='raise', **kwargs):
	data = data.str.replace(',', '.', regex=False)
	data = data.str.replace('..', '.', regex=False)
	return pd.to_numeric(data, errors=errors)


def apply_transform_pipeline(df, field, pipeline, inplace=False, **kwargs):
	data = df[field]
	for fn in pipeline:
		data = fn(data, **kwargs, inplace=inplace)

	if inplace:
		df[field] = data
	return data


OPT_BOOL_PIPELINE   = (transform_strip, transform_fix_common_typos, transform_opt, transform_bool)
OPT_DATE_PIPELINE   = (transform_strip, transform_fix_common_typos, transform_opt, transform_fix_date_typos, transform_date)
OPT_NUMBER_PIPELINE = (transform_strip, transform_fix_common_typos, transform_opt, transform_number)