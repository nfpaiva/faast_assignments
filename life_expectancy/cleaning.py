import re
import pandas as pd

# Define input and output file paths
input_file_path = './life_expectancy/data/eu_life_expectancy_raw.tsv'
output_file_path = './life_expectancy/data/pt_life_expectancy.csv'

# Define variables cleaning  and filtering data
id_vars = ['unit', 'sex', 'age','region']
region_filter='pt'

def clean_data() -> None:
    # load data
    df_raw = pd.read_csv(input_file_path)

    # Split column that has region and years data into separate columns
    df_geoyears_cols=df_raw.iloc[:,3].str.split('\t', expand=True)
    df_geoyears_cols.columns=df_raw.columns[3].split('\t')
    df_final=pd.concat([df_raw.iloc[:,0:3],df_geoyears_cols], axis=1)
    df_final.columns.values[3]='region'
    
    #Transform data into long format, filter out missings and region
    df_final = pd.melt(df_final,id_vars=id_vars)
    df_final = pd.concat([df_final[df_final['value']!=': '].iloc[:,:5],df_final[df_final['value']!=': '].iloc[:,5].apply(lambda x: re.search(r'\d+(?:\.\d+)?',x).group())], axis=1)
    df_final = df_final[(df_final['region']==region_filter)]

    # Save cleaned data to output file
    df_final.to_csv(output_file_path, index=False)
py
if __name__ == '__main__':
    clean_data()