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
	for filename in os.listdir(path):
		if filename.endswith("1.csv"):
			# report = "cl_"+str(filename)
			cols   = ["activity_nr", "reporting_id","estab_name","site_address","site_city","site_state",\
			          "site_zip","owner_type","mail_street","mail_city","mail_state","mail_zip", "open_date"]

			pd_path = path + filename
			df = pd.read_csv(pd_path, usecols = cols, low_memory=False)
			sql.to_sql(df, name = 'oshainsp', if_exists = 'append', con=cnx)


clean("../dedupe/data/split/")

cursor.execute("SELECT * FROM oshainsp WHERE reporting_id = ?", (950643,))
inspection = cursor.fetchone()
print inspection