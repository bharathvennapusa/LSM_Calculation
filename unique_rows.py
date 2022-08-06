import pandas as pd
import re
import os

maxi = 0

df = pd.read_csv(r'/Users/bharath/Downloads/Personal/Study/Pitt/Projects/BrandMarketing/Files/test/LIWC/files/to_export.merged.crm_english.stage_3_f_cleaned_final.csv',encoding='utf-8')


print("Printing Brand data:\n")
brand = df['brand_name_id'].unique()
print(brand)
brand_count = len(brand)
print(brand_count)


print("Printing Influencer data:\n")
influencer = df['social_handle_id'].unique()
print(influencer)
influencer_count = len(influencer)
print(influencer_count)


df_2 = pd.read_csv(r'/Users/bharath/Downloads/Personal/Study/Pitt/Projects/BrandMarketing/Files/test/LIWC/files/to_export.merged.crm_english.maskingresult_brand_name.csv',encoding='utf-8')


print("Printing Brand data after masking:\n")
brand = df_2['brand_name'].unique()
print(brand)
brand_count = len(brand)
print(brand_count)


df_3 = pd.read_csv(r'/Users/bharath/Downloads/Personal/Study/Pitt/Projects/BrandMarketing/Files/test/LIWC/files/to_export.merged.crm_english.maskingresult_social_handle.csv',encoding='utf-8')


print("Printing Influencer data after masking:\n")
influencer = df_3['social_handle'].unique()
print(influencer)
influencer_count = len(influencer)
print(influencer_count)


df_4 = pd.read_csv(r'/Users/bharath/Downloads/Personal/Study/Pitt/Projects/BrandMarketing/Files/test/LIWC/files/to_export.merged.crm_english.liwcanalysis_brand_name.csv',encoding='utf-8')


print("Printing Brand data after LIWC analysis:\n")
brand = df_4['final_brand_name'].unique()
print(brand)
brand_count = len(brand)
print(brand_count)


df_5 = pd.read_csv(r'/Users/bharath/Downloads/Personal/Study/Pitt/Projects/BrandMarketing/Files/test/LIWC/files/to_export.merged.crm_english.liwcanalysis_social_handle.csv',encoding='utf-8')


print("Printing Influencer data after LIWC analysis:\n")
influencer = df_5['final_social_handle'].unique()
print(influencer)
influencer_count = len(influencer)
print(influencer_count)
