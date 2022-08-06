import pandas as pd
  
# read CSV file
results = pd.read_csv('to_export.merged.crm_english.stage_5_f.csv')
  
# count no. of lines
print("Number of lines present:-", len(results))
