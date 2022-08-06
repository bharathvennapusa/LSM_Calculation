import pandas as pd
df = pd.read_csv(r'to_export.merged.crm_english.stage_4_f.csv')
print("Printing row frequency: ")
df = df.notnull().sum()
print(df)

