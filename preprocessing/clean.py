#!/usr/bin/env python
# clean.py

####################################################################
# Imports
####################################################################
import os
import pandas as pd
import csv
import sqlite3 as sqlite
from pandas.io import sql

####################################################################
# Globals
####################################################################
cnx = sqlite.connect('oshainsp.db')
cursor = cnx.cursor()

####################################################################
# Open data csv
# Load into Pandas to wrangle
# Dump to database
####################################################################

def clean(path):
	'''
	The most important columns for deduplication are, in order:
	estab_name
	site_state
	site_zip
	sic_code (better than naics because more likely to be populated)
	naics_code
	mail_state (particularly true of construction industry)
	mail_zip (particularly true of construction industry)
	reporting_id

	Then we'll need activity_nr to connect inspections activity across emplrs.
	And open_date to create a timeline.
	'''
	for filename in os.listdir(path):
		if filename.endswith("1.csv"):
			cols   = ["activity_nr", "reporting_id", "estab_name", "site_state",\
			          "site_zip", "sic_code", "naics_code", "mail_state", \
			          "mail_zip", "open_date"]

			pd_path = path + filename
			df = pd.read_csv(pd_path, usecols = cols, low_memory=False)
			sql.to_sql(df, name = 'inspects', if_exists = 'append', con=cnx)


if __name__ == "__main__":
	clean("../dedupe/data/split/")

	# Test to check if working
	# cursor.execute("SELECT * FROM inspects WHERE reporting_id = ?", (950643,))
	# inspection = cursor.fetchone()
	# print inspection