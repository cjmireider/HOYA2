#!/usr/bin/env python
# coding: utf-8

# # A+E: Hoya 2

# In[1]:


#Load Libraries
import awswrangler as wr #May need to pip install for first use.
import boto3 #May need to pip install for first use.
import pandas as pd
import numpy as np


# ### Configure Credentials in AWS to Access S3 Through Jupyter Notebook
# 
# 1. Unzip files into .csv and upload them to your own S3 bucket.
# 
# 2. Create IAM User with the following policy (navigate to add permissions / create inline policy / json): https://docs.aws.amazon.com/sagemaker/latest/dg/scheduled-notebook-policies-other.html
# 
# 3. Attach S3 Full Access to User, in addition to the policy you just created.
# 
# 4. Create Access Key in user permissions for 3rd party use. Download access keys as a csv.
# 
# 5. Edit bucket policy using the following code (edit according to your bucket name and user credentials):
# 
#     {
#         "Version": "2012-10-17",
#         "Statement": [
#             {
#                 "Effect": "Allow",
#                 "Principal": {
#                     "AWS": "arn:aws:iam::008971644510:user/jupyter-user"
#                 },
#                 "Action": [
#                     "s3:GetObject",
#                     "s3:ListBucket"
#                 ],
#                 "Resource": [
#                     "arn:aws:s3:::kma-capstone-bucket",
#                     "arn:aws:s3:::kma-capstone-bucket/*"
#                 ]
#             }
#         ]
#     }

# In[2]:


#AWS Credentials
config = pd.read_csv('jupyter-user_accessKeys.csv') #Make sure file is saved to working directory
access_key = config.loc[0, 'Access key ID']
secret_key = config.loc[0, 'Secret access key']
region = 'us-east-1'


# In[3]:


#S3 Client
s3_client = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region)


# In[4]:


#Displaying all the buckets in AWS Account.
buckets = s3_client.list_buckets()

for bucket in buckets['Buckets']:
    print(bucket['Name'])


# In[5]:


#Go to desired bucket. 
bucket_name = 'kma-capstone-bucket'

# List the objects in your bucket
response = s3_client.list_objects_v2(Bucket=bucket_name)

# Check if the response contains 'Contents'
if 'Contents' in response:
    # Print out the object keys
    for obj in response['Contents']:
        print(obj['Key'])
else:
    print('No objects found in the bucket.')


# In[6]:


#Define path based on previous output
s3_path1 = 's3://kma-capstone-bucket/cpv_Q12022.csv'
s3_path2 = 's3://kma-capstone-bucket/cpv_Q12023.csv'
s3_path3 = 's3://kma-capstone-bucket/cpv_Q22022.csv'
s3_path4 = 's3://kma-capstone-bucket/cpv_Q22023.csv'
s3_path5 = 's3://kma-capstone-bucket/cpv_Q32022.csv'
s3_path6 = 's3://kma-capstone-bucket/cpv_Q32023.csv'
s3_path7 = 's3://kma-capstone-bucket/cpv_Q42022.csv'
s3_path8 = 's3://kma-capstone-bucket/cpv_Q42023.csv'


# In[7]:


#Create boto3_session for AWS Wrangler
boto3_session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region
)


# In[8]:


# Read Q1 2022 Data
Q122_df = wr.s3.read_csv(path=s3_path1, boto3_session=boto3_session)


# In[9]:


# Read Q1 2023 Data
Q123_df = wr.s3.read_csv(path=s3_path2, boto3_session=boto3_session)


# In[10]:


# Read Q2 2022 Data
Q222_df = wr.s3.read_csv(path=s3_path3, boto3_session=boto3_session)


# In[11]:


# Read Q2 2023 Data
Q223_df = wr.s3.read_csv(path=s3_path4, boto3_session=boto3_session)


# In[12]:


# Read Q3 2022 Data
Q322_df = wr.s3.read_csv(path=s3_path5, boto3_session=boto3_session)


# In[13]:


# Read Q3 2023 Data
Q323_df = wr.s3.read_csv(path=s3_path6, boto3_session=boto3_session)


# In[14]:


# Read Q4 2022 Data
Q422_df = wr.s3.read_csv(path=s3_path7, boto3_session=boto3_session)


# In[15]:


# Read Q4 2023 Data
Q423_df = wr.s3.read_csv(path=s3_path8, boto3_session=boto3_session)


# In[16]:


#Combine eight individual dfs into a complete df. 
df = pd.concat([Q122_df, Q123_df,Q222_df, Q223_df,Q322_df, Q323_df,Q422_df, Q423_df,], ignore_index=True)


# In[17]:


#Check dimensions.
df.shape


# In[19]:


#Check data types
print(df.dtypes)


# ### Prepare for Data Wrangling: Notes from Sponsor
# 
# #### Show Types to focus on: 
# 1. Legacy Series In-Premiere: Defined as a series with more than two season available. The new season is only available on 1st/2nd window platforms, but the library is available on 1st/2nd/3rd window platforms.
# 2. New Series In-Premiere: Defined as a series with less than two seasons. Generally, they are only available on 1st/2nd window platforms. If in its 2nd season, there could be a season one available on 1st/2nd/3rd window platforms, but I can’t think of any that happened like that in FY22/FY23 (aka the data you will be working with).
# 
# #### Show Types to exclude:
# 1. Movies/Specials
# 2. Non-Returning/Cancelled series (ie Dog the Bounty Hunter, and even Dance Moms eventhoug they are trying to revive that this year)
# 
# 
# #### Ad-Sales Platforms to focus on:
# 1. Live +7: This drives about 90% of the total Ad-Sales P2+ mins (this data is generally complete b/c it comes from Nielsen)
# 2. Non-Linear: These top partners drive 80%-90% of the Ad-Sales Non-Linear P2+ Mins. Would try to ensure that the data here is complete and makes sense. I recently had to pull Ad-sales non-linear data for a presentation and attached was my data pull. Thought this would be helpful so you can see the scale of all the platforms at a high level.
#     a. On Demand:
#         (1) O&O
#         (2) vMVPD Hulu Live: HUGE NOTE: Will explain more in person, but we have licensed a large amount of our library content to Hulu SVOD, therefore that inventory does not live on Hulu Live for ad-sales to monetize. (i.e. all Alone library lives on Hulu SVOD, and only the new Alone season is available on Hulu Live each year).
#         (3) vMVPD Philo
#         (4) vMVPD SlingTV
#         (5) STB VOD4+ Comcast
#     b. FAST: Samsung
#     
# #### Primary Levers to focus: 
# 
# 1. Paid Media - Sent by Cristina
# 2. Organic Social Media (A+E under impression that Hoya 2 will retrieve this)
# 3. Partner Earned Media (Still pending with A+E)
# 4. O&O IFW Content - Sent by Cristina
# 5. 3rd Windowing Platforms (FAST/SVOD/AVOD) - Available via CP Data
# 

# ##### Focusing on New / Returning Shows

# In[24]:


df['Program - Current Premiere Date'] #Want to get year shows were originally premiered to determine whether
#they are returning / actively airing

# Convert the column to datetime if it's not already in datetime format
df['Program - Current Premiere Date'] = pd.to_datetime(df['Program - Current Premiere Date'], errors='coerce')

# Extract the year
df['Premiere Year'] = df['Program - Current Premiere Date'].dt.year

# Drop original column
df = df.drop(columns=['Program - Current Premiere Date'])


# In[26]:


# Identify shows with a premiere year in 2022 or 2023
shows_2022_2023 = df[df['Premiere Year'].isin([2022, 2023])]['Program'].unique()

# Filter the original DataFrame to keep all data for those identified shows
df = df[df['Program'].isin(shows_2022_2023)]


# In[27]:


df['Premiere Year'].unique() #Ensuring that data is kept for all years if the show is newly 
#premiering in 2022/2023


# ##### Filtering out Specials and Movies

# In[28]:


#Filter out Specials.
df = df[df["Program - Is Special"] != 1] #Removing observations where the Program is a Special.


# In[30]:


df["Program - Is Special"].unique() #Remaining options include 0 and null values


# In[31]:


df["Program - Is Special"].isna().sum() #136k+ null values left. Dropping this column.


# In[32]:


df = df.drop(columns=['Program - Is Special'])


# In[33]:


#Program - Type variable also seems to be indicative of a special. Continuing to filter down.
df['Program - Type'].unique() #Options include Special and Series
df = df[df["Program - Type"] != "Special"] #Removing observations where the Program is a Special.


# In[34]:


df['Program - Type'].unique() #Remaining Options include Series and Null Values


# In[35]:


df['Program - Type'].isna().sum() #Because there are 136k+ null values aside from Series,
#I am dropping this column


# In[36]:


df = df.drop(columns=['Program - Type']) #Removing column with options for Series or NA


# In[37]:


#Filter out Movies and any outstanding Specials.
df = df[~((df["Program Sub-Type"] == "Movie") | (df["Program Sub-Type"] == "Special"))]


# In[38]:


#Confirm remaining observation options
df['Program Sub-Type'].unique() #Options include Scripted, Mini-Series, etc.. Keeping variable.


# ###### Creating new variable for Legacy/New

# In[39]:


#Create new variable to establish whether season is Legacy or New

# Initialize 'Series Type' column with NaN values
df["Series Type"] = np.nan

# Apply 'New' to rows where 'Program - External Season' is less than 2
df.loc[df["Program - External Season"] < 2, "Series Type"] = "New"

# Apply 'Legacy' to rows where 'Program - External Season' is 2 or more
df.loc[df["Program - External Season"] >= 2, "Series Type"] = "Legacy"


# ###### Removing columns with large amount of outliers

# In[40]:


#Searching for null values:
#Count of missing values per column
missing_count = df.isna().sum()

# Display columns with missing values
missing_columns = missing_count[missing_count > 0]

print("Columns with missing values:")
print(missing_columns)


# In[41]:


#Dropping columns with more than a million missing values
df = df.drop(columns =['Partner - Viewer Code','Partner - Rating Source'])


# In[42]:


#There is a recurring pattern of 136779 missing values. Dropping null values to see if it resolves some
#of the rest.
df = df.dropna(subset=['Program - Series ID'])


# In[43]:


#Reviewing remaining missing values
missing_count = df.isna().sum()

# Display columns with missing values
missing_columns = missing_count[missing_count > 0]

print("Columns with missing values:")
print(missing_columns) #The resulting missing values are much more manageable and will be imputed closer to
#model training. 

#QUESTION FOR TEAM TO CONSIDER: Topic of how the grouping of variables in the season concatenation
#will affect the missing value imputation.


# In[44]:


#Filling NAs with "Unknown" and 0 for purposes of grouping. Beforehand, the grouping was removing Linear
#variables because they had missing values in one category

def fill_na(df):
    # Fill NaN with 'Unknown' for categorical/object columns
    df_cat = df.select_dtypes(include=['category', 'object']).fillna('Unknown')
    
    # Fill NaN with 0 for numerical columns
    df_num = df.select_dtypes(include=['number']).fillna(0)
    
    # Combine both DataFrames
    df_filled = pd.concat([df_cat, df_num], axis=1)
    
    # If there are other data types that we want to preserve, we include them here
    df_other = df.select_dtypes(exclude=['category', 'object', 'number'])
    
    # Add the untouched columns (like datetime) back if any
    if not df_other.empty:
        df_filled = pd.concat([df_filled, df_other], axis=1)
    
    return df_filled

# Apply the function to your DataFrame
df_filled = fill_na(df)


# ###### Concatenate by Season

# In[45]:


#Drop columns relevant to the episode
df_filled = df_filled.drop(columns=['Program - Episode Number','Program - External Air Order','Episode','# of Episode','Distinct Episode',
                      'Year and Month','Month','Partner - Episode','Program - Air Title','Program - Budget Line'])


# In[46]:


#Review variable types after column dropping
df_filled.dtypes 


# In[47]:


categorical_columns = df_filled.select_dtypes(include=['object', 'category']).columns.tolist()

print("Categorical Columns:", categorical_columns)


# In[48]:


grouped = df_filled.groupby(['Program', 'Program - Network', 'Partner - Network', 'Partner - Program', 
                             'Partner - Name', 'Partner - Platform', 'Partner - Device', 'Partner - Demo',
                             'Program - Category', 'Program - Franchise', 'Program - Genre Name', 
                             'Program - Mega Genre', 'Program - Network Name', 'Program - PAC Title', 
                             'Program - Premiere Network Code', 'Program - Status', 'Program - Series Code', 
                             'Program - Series Name', 'Program - Long Form/Short Form', 'Program - Sub Category',
                             'Program - Supplier', 'Program - TMS ID', 'Partner - Data Type', 'Program Sub-Type',
                             'Series Type', 'Premiere Year','Year','Series ID','Program ID Key',
                             'Program - External Season','Program - PAC ID','Program - Production Year',
                             'Program - ID', 'Program - Series ID'],observed=False)

aggregated_seasons = grouped.agg({
    'Partner - Episode Duration': 'sum',      
    'Program - Broadcast Length': 'sum',
    'Program - Broadcast Length SSSSS':'sum',
    'Exposures':'sum',
    'Minutes Viewed':'sum',
    'NO.of Scheduled Minutes':'sum', 
    'NO.of Telecasts':'sum', 
}).reset_index()


# In[49]:


aggregated_seasons['Partner - Platform'].unique() #Confirming that the expected categories are still there


# In[50]:


aggregated_seasons.shape


# ###### Filter Linear to Live+7 by adding Live +3 and DVR 4-7 together

# In[51]:


#Sponsor wants to focus on Live +7 for Linear TV. Confirmed that Live +7 is a combination of Live+3 and
# DVR 4-7. Removing the other linear variables.
aggregated_seasons = aggregated_seasons[~aggregated_seasons['Partner - Platform'].isin(['Linear TV', 'Off Net Linear Licensing'])]


# In[52]:


# Filter the data based on 'Partner - Platform' values
mask = aggregated_seasons['Partner - Platform'].isin(['Live +3', 'DVR 4-7'])

# Define the group columns
group_columns = ['Program', 'Program - Network', 'Partner - Network', 'Partner - Program', 
                             'Partner - Name', 'Partner - Platform', 'Partner - Device', 'Partner - Demo',
                             'Program - Category', 'Program - Franchise', 'Program - Genre Name', 
                             'Program - Mega Genre', 'Program - Network Name', 'Program - PAC Title', 
                             'Program - Premiere Network Code', 'Program - Status', 'Program - Series Code', 
                             'Program - Series Name', 'Program - Long Form/Short Form', 'Program - Sub Category',
                             'Program - Supplier', 'Program - TMS ID', 'Partner - Data Type', 'Program Sub-Type',
                             'Series Type', 'Premiere Year','Year','Series ID','Program ID Key',
                             'Program - External Season','Program - PAC ID','Program - Production Year',
                             'Program - ID', 'Program - Series ID']

# Group by the categorical columns and sum only the numeric ones
df_live7_subset = aggregated_seasons[mask].groupby(group_columns, as_index=False).sum(numeric_only=True)

# After summing, set the 'Partner - Platform' column to 'Live+7'
df_live7_subset['Partner - Platform'] = 'Live+7'

# Combine with the rest of the data
df_rest = aggregated_seasons[~mask]
df_final = pd.concat([df_rest, df_live7_subset], ignore_index=True)


# In[53]:


df_final['Partner - Platform'].unique() #Confirming that Live+7 is now showing up


# ###### Bring in Ad Sales Data

# In[54]:


#Identify unique networks so that we can bring in Ad Sales Data

unique_networks = df_final['Partner - Name'].drop_duplicates()

# Display the result
print(unique_networks) #We will need to group 


# In[55]:


#Renaming certain names to align with ad revenue lever
df_final['Partner - Name'] = df_final['Partner - Name'].replace({'Wurl Samsung-Hub': 'Samsung', 'Cascada Samsung': 'Samsung',
                                                   'Wurl Samsung-mobile':'Samsung',' Wurl Samsung':'Samsung',
                                                    'Wurl Plex':'Plex','Wurl Vizio':'Vizio'})


# In[56]:


#Read in spreadsheet provided by sponsor for Ad sales.
adsales = pd.read_excel('LinearAdSalesbyPartner.xlsx')
adsales.head


# In[57]:


# Split `df` into chunks to facilitate processing
num_chunks = 20  # Adjust based on available memory and dataset size
df_chunks = np.array_split(df_final, num_chunks)

# Initialize an empty list to hold the merged results
merged_results = []

# Iterate over each chunk and merge with `adsales`
for chunk in df_chunks:
    merged_chunk = pd.merge(chunk, adsales, on=['Partner - Name', 'Partner - Platform'], how='left')
    merged_results.append(merged_chunk)

# Concatenate the merged chunks into a final DataFrame
df_final = pd.concat(merged_results, ignore_index=True)


# In[58]:


df_final.head()


# ###### Bring in Marketing Spend Data

# In[ ]:


df_final['Program - Franchise'] = df_final['Program - Franchise'].str.title() #Undo All Caps Column


# In[73]:


#Franchise is currently "History's Greatest". Splitting between "Mysteries" and "Heist" to match
#market spend spreadsheet. 
df_final.loc[df_final['Program'] == "History's Greatest Mysteries", 
             'Program - Franchise'] = "History's Greatest Mysteries"


# In[74]:


#Franchise is currently "History's Greatest". Splitting between "Mysteries" and "Heist" to match
#market spend spreadsheet. 
df_final.loc[df_final['Program'] == "History's Greatest Heists With Pierce Brosnan", 
             'Program - Franchise'] = "History's Greatest Heists"


# In[75]:


#Franchise is currently "Secrets of". Splitting between "Playboy" and "Skinwalker" to match
#market spend spreadsheet. 
df_final.loc[df_final['Program'] == "Secrets of Playboy", 
             'Program - Franchise'] = "Secrets of Playboy"


# In[77]:


#Franchise is currently "Secrets of". Splitting between "Playboy" and "Skinwalker" to match
#market spend spreadsheet. 
df_final.loc[
    (df_final['Program'] == "The Secret of Skinwalker Ranch") | 
    (df_final['Program'] == "The Secret of Skinwalker Ranch: Digging Deeper"), 
    'Program - Franchise'
] = "Secrets Of Skinwalker Ranch"


# In[78]:


#Getting spreadsheet to make sure name formatting matches across spreadsheets
unique_program = df_final['Program - Franchise'].drop_duplicates() #Extract franchise titles
output_excel_path = 'uniqueprograms.xlsx' #Extract Excel to Compare with Marketing Spend
unique_program.to_excel(output_excel_path, index=False)


# In[87]:


#Reading in Marketing Spend Data
marketspendlinear = pd.read_excel('FormattedMarketingSpend.xlsx', sheet_name=0) #Read in Linear Tab
marketspendatv = pd.read_excel('FormattedMarketingSpend.xlsx', sheet_name=1) #Read in ATV Tab


# In[90]:


#Grouping original dataframe by linear/nonlinear, then merging
linear_filtered = df_final[df_final['Partner - Platform']=='Live+7'] #Filtering to Linear 
nonlinear = df_final[df_final['Partner - Platform']!='Live+7'] #Filtering to NonLinear
linear_filtered = pd.merge(marketspendlinear, linear_filtered, on='Program - Franchise', how='left') #Join Tab1
nonlinear = pd.merge(marketspendatv, nonlinear, on='Program - Franchise', how='left') #Join Tab2


# In[92]:


#Rejoining Linear and Nonlinear
df_final = pd.concat([linear_filtered,nonlinear], ignore_index=True)


# In[93]:


#Current Shape
df_final.shape


# In[94]:


#Revert missing values back from Unknown to NA
df_final.replace('Unknown', np.nan, inplace=True)


# ###### Getting Rid of Remaining Irrelevant Columns (In Progress)

# In[95]:


df_final.columns

