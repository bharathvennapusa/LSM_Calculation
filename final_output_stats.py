import pandas as pd
df = pd.read_csv(r'to_export.merged.crm_english.stage_5_f_initial.csv') # pass the file that contains cleaned post text column

print("Printing row frequency: ")
df_2 = df.notnull().sum()
print(df_2)
