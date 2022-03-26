#!/usr/bin/env python3

import os
from collections import namedtuple
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


OUTPUT_DIR = 'Análisis de supervivencia'

DATE_COLUMNS = [
	'clinical_onset',
	'als_dx',
	'last_followup',
	'kings_1',
	'kings_2',
	'kings_3',
	'kings_4',
	'mitos_1',
	'mitos_2',
	'mitos_3',
	'mitos_4',
	'walking_aids',
	'cpap_initiation',
	'niv_support',
	'imv_support',
	'peg_colocation',
	'oral_supl_start',
	'enteric_supl_start',
	'death',
]

origin_info = namedtuple('origin_info', 'title fn')

def analyze_survival(origin, event, censored, exclude_negative=True, **kwargs):
	event_times = event - origin
	censored_times = censored - origin
	times = event_times.where(event_times.notna(), censored_times)

	event_status = pd.Series(np.ones(len(event)))
	censored_status = pd.Series(np.zeros(len(censored)))
	status = event_status.where(event.notna(), censored_status)

	input = pd.DataFrame(dict(survival=times, status=status))

	if exclude_negative:
		input = input[input.survival > pd.Timedelta(0)]

	return sm.SurvfuncRight(input.survival.dt.days, input.status, **kwargs)


def analyze_events_survival_from_origin(df, origin, title=None):
	death_or_loss = df[['last_followup', 'death']].min(axis=1)
	kings_ge = df[['kings_1', 'kings_2', 'kings_3', 'kings_4']].bfill(axis=1)
	mitos_ge = df[['mitos_1', 'mitos_2', 'mitos_3', 'mitos_4']].bfill(axis=1)

	return {
		'kings_1': analyze_survival(origin, kings_ge.kings_1, death_or_loss, title=title.format(event='King\'s 1') if title is not None else None),
		'kings_2': analyze_survival(origin, kings_ge.kings_2, death_or_loss, title=title.format(event='King\'s 2') if title is not None else None),
		'kings_3': analyze_survival(origin, kings_ge.kings_3, death_or_loss, title=title.format(event='King\'s 3') if title is not None else None),
		'kings_4': analyze_survival(origin, kings_ge.kings_4, death_or_loss, title=title.format(event='King\'s 4') if title is not None else None),

		'mitos_1': analyze_survival(origin, mitos_ge.mitos_1, death_or_loss, title=title.format(event='MiToS 1') if title is not None else None),
		'mitos_2': analyze_survival(origin, mitos_ge.mitos_2, death_or_loss, title=title.format(event='MiToS 2') if title is not None else None),
		'mitos_3': analyze_survival(origin, mitos_ge.mitos_3, death_or_loss, title=title.format(event='MiToS 3') if title is not None else None),
		'mitos_4': analyze_survival(origin, mitos_ge.mitos_4, death_or_loss, title=title.format(event='MiToS 4') if title is not None else None),

		'walking_aids': analyze_survival(origin, df.walking_aids, death_or_loss, title=title.format(event='walking aids') if title is not None else None),
		'niv_support': analyze_survival(origin, df.niv_support, death_or_loss, title=title.format(event='NIV support') if title is not None else None),
		'imv_support': analyze_survival(origin, df.imv_support, death_or_loss, title=title.format(event='IMV support') if title is not None else None),
		'peg_colocation': analyze_survival(origin, df.peg_colocation, death_or_loss, title=title.format(event='PEG') if title is not None else None),

		'oral_supl_start': analyze_survival(origin, df.oral_supl_start, death_or_loss, title=title.format(event='oral supl.') if title is not None else None),
		'enteric_supl_start': analyze_survival(origin, df.enteric_supl_start, death_or_loss, title=title.format(event='enteric supl.' if title is not None else None)),

		'death': analyze_survival(origin, df.death, df.last_followup, title=title.format(event='death')),
	}


def analyze_group_events_survival(df, from_origin):
	return {
		name: analyze_events_survival_from_origin(df, info.fn(df), title=info.title)
			for name, info in from_origin.items()
	}


def plot_group_events_survival(data, plotdefs):
	figs = {}
	for pattern, groupnames in plotdefs.items():
		for groupname in groupnames:
			origin_events = data.get(groupname)
			for origin, events in origin_events.items():
				for eventname, sf in events.items():
					figname = pattern.format(group=groupname, origin=origin, event=eventname)
					fig = figs.get(figname)
					if fig is None:
						fig = figs[figname] = sf.plot()
						ax = fig.get_axes()[0]
						ax.set_title(sf.title)
					else:
						sf.plot(ax=fig.get_axes()[0])
	return figs


def customize_survival_plot(fig):

	@ticker.FuncFormatter
	def _y_ticks_formatter(x, pos):
		return int(x * 100)

	ax, = fig.get_axes()
	ax.set_xlabel('Time in days')
	ax.set_ylabel('% of patients')
	ax.set_yticks(np.arange(0, 1.1, step=.25))
	ax.yaxis.set_major_formatter(_y_ticks_formatter)


plt.rcParams['axes.spines.top'] = False
plt.rcParams['axes.spines.right'] = False
plt.rcParams['figure.max_open_warning'] = False

df = pd.read_csv('resultados.csv', parse_dates=DATE_COLUMNS)

origins = {
	'dx': origin_info(title='Time to {event} from diagnosis', fn=lambda df: df.als_dx),
	'onset': origin_info(title='Time to {event} from onset', fn=lambda df: df.clinical_onset),
}

survival = {
	'Overall': analyze_group_events_survival(df, origins),
	'C9orf72': analyze_group_events_survival(df[df.c9_status == 'Altered'], origins),
	'SOD1': analyze_group_events_survival(df[df.sod1_status == 'Altered'], origins),
}

figures = plot_group_events_survival(survival, {
	'{event}/{event}_{origin}': ('Overall',),
	'{event}/{event}_{origin}_genes': ('Overall', 'C9orf72', 'SOD1'),
})

for relpath, fig in figures.items():
	path = Path(os.path.join(OUTPUT_DIR, relpath))
	path.parent.mkdir(parents=True, exist_ok=True)
	customize_survival_plot(fig)
	fig.savefig(path.with_suffix('.png'), dpi=300)
	plt.close(fig)