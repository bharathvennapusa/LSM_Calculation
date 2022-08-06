import pandas as pd
import re
import os

maxi = 0
path = ""
print(path)

for file in os.listdir():
    if file.startswith("to_export.merged.crm_english.stage_3_f_"):
        finalpath = os.path.join(path, file)
        file_path = f"{finalpath}"
        print (file_path)
        df = pd.read_csv(file_path, encoding='utf-8', low_memory = False)
        if maxi < df["social_handle_id"].max():
            maxi = df["social_handle_id"].max()

print(maxi)

for cur_id in range(1, maxi+1): ## Iterate through the different IDS in each of the posts
    print(cur_id)

    for file in os.listdir(): ## Loop through all the files in the directory
        if file.startswith("to_export.merged.crm_english.stage_3_f_"): ## If the file has a specific start (the names of our chunks), we store its filepath
            finalpath = os.path.join(path, file)
            file_path = f"{finalpath}"
            print('Currently working on file: ' + file_path)
            df = pd.read_csv(file_path, encoding='utf-8', low_memory = False) ## Read in the CSV of the chunk
            mask = df['social_handle_id'] == cur_id ## Get only the parts of that CSV that contain the brand we are currently working on
            df_2 = df[mask] ## Save the parts that we want to a new variable
            df_2.to_csv(r'test_1.csv', mode ='a', index = False)

            print('{0} has been pulled'.format(cur_id))
    
    post_text_dict = {'text' : [], 'year' : [], 'social_handle' : []} ## Define what we want our new CSV/Dataframe to look like
    social_handle = ''
    for year in range(2016, 2021): ## Loop through all the different years that the posts could be from
        post_text = ''
        
        df_2 = pd.read_csv('test_1.csv', encoding='utf-8', low_memory = False) ## Read in the CSV of the chunk

        for index, row in df_2.iterrows(): ## Loop through all the rows in the dataframe to ensure year matching and concatenation of appropriate post_text
            row_year = row['post_date']
            social_handle = row['social_handle']
            #print(row_year)
            if row_year[0:4] == str(year): ## Ensure that the years match before concatenation
                post_text = post_text + ' ' + str(row['Message_cln']) ## Concatenation
        
        comma_pattern = re.compile(',') ## Some of the posts contain commas, this will break our CSVs, so we remove them
        space_pattern = re.compile(r'\s+') ## Most posts contains new lines, excess spaces, and other whitespace abnormalities that we wish to fix
        post_text = re.sub(space_pattern, ' ', post_text) ## Replaces the weird spacing with singular spaces
        post_text = re.sub(comma_pattern, '', post_text) ## Removes the commas
        
        if (post_text != '' and social_handle != 'social_handle'):
            if (len(post_text) > 32000):
                post_text_final = post_text[:16000] + ' ' + post_text[-16000:]
                post_text_dict['text'].append(post_text_final)
            else:
                post_text_dict['text'].append(post_text[:32000])
            post_text_dict['year'].append(year)
            post_text_dict['social_handle'].append(social_handle)

    post_text_df = pd.DataFrame.from_dict(post_text_dict)
    mod_df = post_text_df.dropna()
    if (cur_id == 1):
        mod_df.to_csv(r'test_social.csv', mode ='a', index = False, header = True)
    else :
        mod_df.to_csv(r'test_social.csv', mode ='a', index = False, header = False)
    os.remove('test_1.csv')
