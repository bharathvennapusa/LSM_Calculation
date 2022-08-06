import pandas as pd
import re
import os

maxi = 0
path = "/home/ubuntu/test/all/MaskTest/cleaned"
print(path)

for file in os.listdir(path):
    if file.startswith("to_export.merged.crm_english.stage_3_f_"):
        finalpath = os.path.join(path, file)
        file_path = f"{finalpath}"
        print (file_path)
        df = pd.read_csv(file_path, encoding='utf-8', low_memory = False)
        if maxi < df["brand_name_id"].max():
            maxi = df["brand_name_id"].max()

print(maxi)

for cur_id in range(1, maxi+1): ## Iterate through the different IDS in each of the posts
    print(cur_id)

    for file in os.listdir(path): ## Loop through all the files in the directory
        if file.startswith("to_export.merged.crm_english.stage_3_f_"): ## If the file has a specific start (the names of our chunks), we store its filepath
            finalpath = os.path.join(path, file)
            file_path = f"{finalpath}"
            print('Currently working on file: ' + file_path)
            df = pd.read_csv(file_path, encoding='utf-8', low_memory = False) ## Read in the CSV of the chunk
            mask = df['brand_name_id'] == cur_id ## Get only the parts of that CSV that contain the brand we are currently working on
            df_2 = df[mask] ## Save the parts that we want to a new variable
            df_2.to_csv(r'/home/ubuntu/test/all/MaskTest/cleaned/test.csv', mode ='a', index = False)

            print('{0} has been pulled'.format(cur_id))
    post_text_dict = {'text' : [], 'year' : [], 'brand_name' : []} ## Define what we want our new CSV/Dataframe to look like
    brand_name = ''
    brand_name_id = ''
    for year in range(2016, 2021): ## Loop through all the different years that the posts could be from
        post_text = ''
        #original_text = ''
        df_2 = pd.read_csv("/home/ubuntu/test/all/MaskTest/cleaned/test.csv", encoding='utf-8', low_memory = False) ## Read in the CSV of the chunk

        for index, row in df_2.iterrows(): ## Loop through all the rows in the dataframe to ensure year matching and concatenation of appropriate post_text
            row_year = row['post_date']
            brand_name = row['final_brand_name']
            brand_name_id = row['brand_name_id']
            if row_year[0:4] == str(year): ## Ensure that the years match before concatenation
                post_text = post_text + ' ' + str(row['Message_cln']) ## Concatenation
                
        comma_pattern = re.compile(',') ## Some of the posts contain commas, this will break our CSVs, so we remove them
        space_pattern = re.compile(r'\s+') ## Most posts contains new lines, excess spaces, and other whitespace abnormalities that we wish to fix
        post_text = re.sub(space_pattern, ' ', post_text) ## Replaces the weird spacing with singular spaces
        post_text = re.sub(comma_pattern, '', post_text) ## Removes the commas
        post_text_dict['text'].append(post_text[:30000]) ## We might be able to get around this if we can directly input text to LIWC
        ## For later stages: first 16k chars and last 16k chars
        #if (len(post_text) > 32000):
        #    post_text_final = post_text[:16000] + ' ' + post_text[-16000:]
        #    post_text_dict['text'].append(post_text_final)
        #else:
        #    post_text_dict['text'].append(post_text[:32000])
        post_text_dict['year'].append(year)
        post_text_dict['final_brand_name'].append(brand_name)
        post_text_dict['brand_name_id'].append(brand_name_id)

    post_text_df = pd.DataFrame.from_dict(post_text_dict)
    if (cur_id == 1):
        post_text_df.to_csv(r'/home/ubuntu/test/all/MaskTest/cleaned/test_brand_withid.csv', mode ='a', index = False, header = True)
    else :
        post_text_df.to_csv(r'/home/ubuntu/test/all/MaskTest/cleaned/test_brand_withid.csv', mode ='a', index = False, header = False)
    os.remove("/home/ubuntu/test/all/MaskTest/cleaned/test.csv")

    #df_2.to_csv(r'result_brand.csv', mode ='a', index = False) if df_2['source'].all() == 'brand' else df_2.to_csv(r'result_influencer.csv', mode ='a', index = False)
    ## Get rid of the above line if we don't need it. Keeping it currently for reference
