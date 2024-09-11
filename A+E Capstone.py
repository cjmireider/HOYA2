#!/usr/bin/env python
# coding: utf-8

# # A+E: Hoya 2

# In[ ]:


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

# In[ ]:


#AWS Credentials
config = pd.read_csv('jupyter-user_accessKeys.csv') #Make sure file is saved to working directory
access_key = config.loc[0, 'Access key ID']
secret_key = config.loc[0, 'Secret access key']
region = 'us-east-1'


# In[ ]:


#S3 Client
s3_client = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region)


# In[ ]:


#Displaying all the buckets in AWS Account.
buckets = s3_client.list_buckets()

for bucket in buckets['Buckets']:
    print(bucket['Name'])


# In[ ]:


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


# In[ ]:


#Define path based on previous output
s3_path1 = 's3://kma-capstone-bucket/cpv_Q12022.csv'
s3_path2 = 's3://kma-capstone-bucket/cpv_Q12023.csv'
s3_path3 = 's3://kma-capstone-bucket/cpv_Q22022.csv'
s3_path4 = 's3://kma-capstone-bucket/cpv_Q22023.csv'
s3_path5 = 's3://kma-capstone-bucket/cpv_Q32022.csv'
s3_path6 = 's3://kma-capstone-bucket/cpv_Q32023.csv'
s3_path7 = 's3://kma-capstone-bucket/cpv_Q42022.csv'
s3_path8 = 's3://kma-capstone-bucket/cpv_Q42023.csv'


# In[ ]:


#Create boto3_session for AWS Wrangler
boto3_session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region
)


# In[ ]:


# Read Q1 2022 Data
Q122_df = wr.s3.read_csv(path=s3_path1, boto3_session=boto3_session)


# In[ ]:


# Read Q1 2023 Data
Q123_df = wr.s3.read_csv(path=s3_path2, boto3_session=boto3_session)


# In[ ]:


# Read Q2 2022 Data
Q222_df = wr.s3.read_csv(path=s3_path3, boto3_session=boto3_session)


# In[ ]:


# Read Q2 2023 Data
Q223_df = wr.s3.read_csv(path=s3_path4, boto3_session=boto3_session)


# In[ ]:


# Read Q3 2022 Data
Q322_df = wr.s3.read_csv(path=s3_path5, boto3_session=boto3_session)


# In[ ]:


# Read Q3 2023 Data
Q323_df = wr.s3.read_csv(path=s3_path6, boto3_session=boto3_session)


# In[ ]:


# Read Q4 2022 Data
Q422_df = wr.s3.read_csv(path=s3_path7, boto3_session=boto3_session)


# In[ ]:


# Read Q4 2023 Data
Q423_df = wr.s3.read_csv(path=s3_path8, boto3_session=boto3_session)


# In[ ]:


#Combine eight individual dfs into a complete df. 
df = pd.concat([Q122_df, Q123_df,Q222_df, Q223_df,Q322_df, Q323_df,Q422_df, Q423_df,], ignore_index=True)


# In[ ]:


#Check dimensions.
df.shape


# In[ ]:


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

# In[ ]:


#Filter out Specials.
df['Program Sub-Type'].unique() #1 indicates a Special, otherwise 0
df = df[df["Program - Is Special"] != 1] #Removing observations where the Program is a Special.


# In[ ]:


#Program - Type variable also seems to be indicative of a special. Continuing to filter down.
df['Program - Type'].unique() #Options include Special and Series
df = df[df["Program - Type"] != "Special"] #Removing observations where the Program is a Special.


# In[ ]:


#Filter out Movies and any outstanding Specials.
df['Program Sub-Type'].unique() #Options include Special and Movie
df = df[~((df["Program Sub-Type"] == "Movie") | (df["Program Sub-Type"] == "Special"))]


# In[ ]:


#IN PROGRESS. Filter out shows that are not active. - Confirm with sponsor that this is how we wouldfilter out
#non-returning or cancelled series. 
df['Program - Status'].unique()


# In[ ]:


#Create new variable to establish whether season is Legacy or New

# Initialize 'Series Type' column with NaN values
df["Series Type"] = np.nan

# Apply 'New' to rows where 'Program - External Season' is less than 2
df.loc[df["Program - External Season"] < 2, "Series Type"] = "New"

# Apply 'Legacy' to rows where 'Program - External Season' is 2 or more
df.loc[df["Program - External Season"] >= 2, "Series Type"] = "Legacy"


# In[ ]:


#IN PROGRESS. Sponsor wants to focus on Live +7 for Linear TV. Identity which variable needed to filter this.
df['Partner - Platform'].unique()


# #### Columns to be removed (working list).
# 1. Program - Is Special. Even though there are 600k+ NaNs, assuming that the post-filtering data contains non-specials. 
# 2. Program - Type. Even though there are 600k_ NaNs, assuming that post-filtering data contains non-specials. 
# 3. Program - Series Code. Unique identifier.
# 4. Program - Series ID.
# 5. Program - ID. Unique identifier. 
# 6. Program - PAC ID. Unique identifier.
# 7. Program - TMS ID. Unique identifier.

# In[ ]:


columns_to_drop = ['Program - Is Special', 'Program - Type', 'Program - Series Code',"Program - Series ID","Program - ID","Program - PAC ID","Program - TMS ID"]
df = df.drop(columns=columns_to_drop)

