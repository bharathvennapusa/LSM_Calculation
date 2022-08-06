import pandas as pd
import re
import os

maxi = 0

df = pd.read_csv(r'/Users/bharath/Downloads/Personal/Study/Pitt/Projects/BrandMarketing/Files/test/LIWC/files/to_export.merged.crm_english.stage_3_f_cleaned_final.csv',encoding='utf-8')


#print(df['brand_name_id'].unique())


if maxi < df["brand_name_id"].max():
    maxi = df["brand_name_id"].max()

print(maxi)


for cur_id in range(1015, 1016): ## Iterate through the different IDS in each of the posts
    print(cur_id)
    
    mask = df['brand_name_id'] == cur_id ## Get only the parts of that CSV that contain the brand we are currently working on
    df_2 = df[mask] ## Save the parts that we want to a new variable

    post_text_dict = {'text' : [], 'year' : [], 'final_brand_name' : [], 'final_brand_name_id' : []} ## Define what we want our new CSV/Dataframe to look like
    brand_name_id = ''
    brand_name = ''
    for year in range(1, maxi+1): ## Loop through all the different years that the posts could be from
        post_text = ''

        for index, row in df_2.iterrows(): ## Loop through all the rows in the dataframe to ensure year matching and concatenation of appropriate post_text
            row_year = row['post_date']
            brand_name_id = row['brand_name_id']
            brand_name = row['final_brand_name']
            
            if row_year[0:4] == str(year) and (str(row['Message_cln']) != 'nan'): ## Ensure that the years match before concatenation
                post_text = post_text + ' ' + str(row['Message_cln']) ## Concatenation
        
        comma_pattern = re.compile(',') ## Some of the posts contain commas, this will break our CSVs, so we remove them
        space_pattern = re.compile(r'\s+') ## Most posts contains new lines, excess spaces, and other whitespace abnormalities that we wish to fix
        post_text = re.sub(space_pattern, ' ', post_text) ## Replaces the weird spacing with singular spaces
        post_text = re.sub(comma_pattern, '', post_text) ## Removes the commas
        
        if (post_text != ''):
            ## For later stages: taking the first 16k and last 16k chars
            #if (len(post_text) > 32000):
            #    post_text_final = post_text[:16000] + ' ' + post_text[-16000:]
            #    post_text_dict['text'].append(post_text_final)
            #else:
            #    post_text_dict['text'].append(post_text[:32000])
            post_text_dict['text'].append(post_text[:32000])
            post_text_dict['year'].append(year)
            post_text_dict['final_brand_name'].append(brand_name)
            post_text_dict['final_brand_name_id'].append(brand_name_id)

    post_text_df = pd.DataFrame.from_dict(post_text_dict)
    mod_df = post_text_df.dropna()
    if (cur_id == 1):
        mod_df.to_csv(r'/Users/bharath/Downloads/Personal/Study/Pitt/Projects/BrandMarketing/Files/test/LIWC/files/test_brand_1.csv', mode ='a', index = False, header = True)
    else :
        mod_df.to_csv(r'/Users/bharath/Downloads/Personal/Study/Pitt/Projects/BrandMarketing/Files/test/LIWC/files/test_brand_1.csv', mode ='a', index = False, header = False)
