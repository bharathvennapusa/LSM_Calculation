### coed for chunnk and clean

import os
import pandas as pd
import re

def remove_URL(text):
    url = re.compile(r'https?://\S+|www\.\S+')
    return url.sub(r'', text)
def remove_emoji(text):
    emoji_pattern = re.compile(
        '['
        u'\U0001F600-\U0001F64F'  # emoticons
        u'\U0001F300-\U0001F5FF'  # symbols & pictographs
        u'\U0001F680-\U0001F6FF'  # transport & map symbols
        u'\U0001F1E0-\U0001F1FF'  # flags (iOS)
        u'\U00002702-\U000027B0'
        u'\U000024C2-\U0001F251'
        ']+',
        flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)
def remove_html(text):
    html = re.compile(r'<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
    return re.sub(html, '', text)

## Uncomment for breaking whole data into chunks
#df = pd.read_csv('result_3d_merge.csv',encoding='utf-8',chunksize=2500000)
#print('read') ## debug checkpoint : informing us that the file has been read

#for i,chunk in enumerate(df):
#    chunk.to_csv('result_3d_merge_{}.csv'.format(i), index=False)
#    print(i,'check')## debug checkpoint : informing us that chunk number

j = 0
for file in os.listdir():
    # Check whether file is in text format or not
    if file.startswith("to_export.merged.crm_english.stage_3_f.csv"):
        file_path = f"{file}"
        print(file_path) ##debug check point
        df = pd.read_csv(file_path,encoding='utf-8', low_memory = False)
        df['Message_cln'] = df['post_text'].str.replace('http\S+|www.\S+', '', regex = True, case=False)
        df['Message_cln'] = df['Message_cln'].apply(lambda x: remove_emoji(str(x)))
        #df['Message_cln'] = df['Message_cln'].apply(lambda x: remove_html(str(x)))
        df['Message_cln'] = df.Message_cln.str.replace('#','')  #remove hashtags
        df['Message_cln'] = df['Message_cln'].apply(lambda x : re.sub(r'\\n', ' ', x))
        df['Message_cln'] = df['Message_cln'].apply(lambda x : re.sub("#[A-Za-z0-9]+","",x))
        df['Message_cln'] = df['Message_cln'].apply(lambda x : re.sub("@[A-Za-z0-9]+","",x))
        df['Message_cln'] = df['Message_cln'].apply(lambda x : re.sub(r'\\ud...', '', x))
        df['Message_cln'] = df['Message_cln'].apply(lambda x : re.sub('[^A-Za-z0-9{" "}@{#}]+', '', x))
        df['Message_cln'] = df['Message_cln'].apply(lambda x : re.sub(r'img', '', x))
        df['Message_cln'] = df.Message_cln.str.replace(r'\bimg$', '', regex=True).str.strip()
        df['Message_cln'] = df['Message_cln'].apply(lambda x : re.sub(r'src"', '', x))
        df['Message_cln'] = df['Message_cln'].str.replace('"', '')
        df['Message_cln'] = df['Message_cln'].str.replace('@', '')
        #To clean NAN
        #df['Message_cln'] = df['Message_cln'].fillna(' ')
        df.to_csv(r'to_export.merged.crm_english.stage_3_f_cleaned_final.csv', mode ='a', index = False) # final cleaned output file
        print(j)
        j = j + 1


### 27 10 8 6 * /usr/bin/python3 /home/ubuntu/test/all/MaskTest/query_mask_concatenation.py >> /home/ubuntu/test/all/MaskTest/masktest.log 2>&1
        


