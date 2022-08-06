import csv
import pandas as pd
import re
from fuzzywuzzy import fuzz

row_count = 3218286
new_items = {'brand_name' : [], 'brand_id' : [], 'brand_frq': [], 'social_handle' : [], 'social_id' : [], 'social_frq': []}

def clean(string):
    punc = re.compile(r'[^a-z A-Z]')
    puncless = re.sub(punc, '', string)

    return puncless.lower()

def similarity(Str1, Str2):
    Ratio = fuzz.ratio(Str1, Str2)
    Partial_Ratio = fuzz.partial_ratio(Str1, Str2)

    if Partial_Ratio > 60 and Ratio > 50:
        return True
    
    return False

def removeMissing():
    missing_dict = {'year' : ['16', '17', '18', '19', '20'], 'frequency' : [0, 0, 0, 0, 0]}
    global row_count

    with open('to_export.merged.crm_english.stage_1_1.csv', 'r', encoding='utf-8') as read_obj:
        reader = csv.reader(read_obj)

        with open('to_export.merged.crm_english.stage_1_2.csv', 'w', newline='', encoding='utf-8') as write_obj:
            writer = csv.writer(write_obj)

            for r in reader:
                if '' in {r[9], r[10], r[11], r[14], r[36], r[38]}:
                    row_count += -1
                    year = r[22][-2:]
                    print(r[22])

                    if int(year) >= 16 and int(year) <= 20:
                        index = missing_dict['year'].index(year)

                        missing_dict['frequency'][index] += 1
                else:
                    writer.writerow(r)
    
    pd.DataFrame(missing_dict).to_csv('missing_frequency.csv')
    print(row_count)

def engagement():
    engage_dict = {'year' : ['16', '17', '18', '19', '20'], 'sum_engagement' : [0, 0, 0, 0, 0]}

    with open('to_export.merged.crm_english.stage_1_2.csv', 'r', encoding='utf-8') as read_obj:
        reader = csv.reader(read_obj)
        next(reader)

        for r in reader:
            year = r[22][-2:]

            if int(year) >= 16 and int(year) <= 20:
                sum_engagement = int(0 if '' == r[23] else r[23]) + int(0 if '' == r[24] else r[24]) + int(0 if '' == r[26] else r[26]) + int(0 if '' == r[28] else r[28]) + int(0 if '' == r[29] else r[29])
                index = engage_dict['year'].index(year)

                engage_dict['sum_engagement'][index] += sum_engagement
    
    pd.DataFrame(engage_dict).to_csv('sum_engagement.csv')
        
def eliminate_small():
    global row_count

    socials = pd.read_csv('socials.csv')
    socials = socials.to_dict('list')

    brands = pd.read_csv('brands.csv')
    brands = brands.to_dict('list')

    with open('to_export.merged.crm_english.stage_1_2.csv', 'r', encoding='utf-8') as read_obj:
        reader = csv.reader(read_obj)

        with open('to_export.merged.crm_english.stage_1_f.csv', 'w', encoding='utf-8') as write_obj:
            writer = csv.writer(write_obj)

            writer.writerow(next(reader))

            for r in reader:
                print(r[41])
                if r[41] == '1':
                    index = socials['social_id'].index(int(r[40]))

                    print(socials['social_frq'][index])
                    if socials['social_frq'][index] < 100:
                        print('delete')
                        row_count += -1
                    else:
                        writer.writerow(r)
                else:
                    index = brands['brand_id'].index(int(r[39]))

                    if brands['brand_frq'][index] < 100:
                        row_count += -1
                    else:
                        writer.writerow(r)
    print(row_count)

def main():
    global new_items
    brand_id_counter = 1
    social_id_counter = 1

    with open('to_export.merged.crm_english.csv', 'r', encoding='utf-8') as read_obj:
        reader = csv.reader(read_obj)


        with open('to_export.merged.crm_english.stage_1_1.csv', 'w', newline='', encoding='utf-8') as write_obj:
            writer = csv.writer(write_obj)
            t = next(reader)

            for i in ['final_brand_name', 'brand_name_id', 'social_handle_id', 'source_dummy', 'source']:
                t.append(i)

            writer.writerow(t)

            for r in reader:
                print(brand_id_counter, social_id_counter)

                brand = r[8] if r[8] != '' else r[7]
                social = r[14]

                cleaned_brand = clean(brand)
                cleaned_social = clean(social)

                source = 0 if similarity(cleaned_brand, cleaned_social) else 1

                if cleaned_brand not in new_items['brand_name']:
                    new_items['brand_name'].append(cleaned_brand)
                    new_items['brand_id'].append(brand_id_counter)
                    new_items['brand_frq'].append(1)
                    brand_id_counter += 1
                else:
                    i = new_items['brand_name'].index(cleaned_brand)
                    new_items['brand_frq'][i] += 1

                if cleaned_social not in new_items['social_handle']:
                    new_items['social_handle'].append(cleaned_social)
                    new_items['social_id'].append(social_id_counter)
                    new_items['social_frq'].append(1)
                    social_id_counter += 1
                else:
                    i = new_items['social_handle'].index(cleaned_social)
                    new_items['social_frq'][i] += 1

                brand_id = new_items['brand_id'][new_items['brand_name'].index(cleaned_brand)]
                social_id = new_items['social_id'][new_items['social_handle'].index(cleaned_social)]

                for i in [cleaned_brand, brand_id, social_id, source, 'influencer' if source == 1 else 'brand']:
                    r.append(i)

                writer.writerow(r)

    brand_df = pd.DataFrame(new_items, columns=['brand_name', 'brand_id', 'brand_frq'])
    brand_df.sort_values('brand_name').reset_index(drop=True).to_csv('brands.csv')

    social_df = pd.DataFrame(new_items, columns=['social_handle', 'social_id', 'social_frq'])
    social_df.sort_values('social_handle').reset_index(drop=True).to_csv('socials.csv')

main()
removeMissing()
engagement()
eliminate_small()
