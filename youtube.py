from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
from pandas import DataFrame as df
import streamlit as st

# API key connection

def Api_connect():
    Api_Id = "AIzaSyCEHQNj0Mc0TqrbEQMcRtifr-y41YhipxM"

    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name, api_version, developerKey=Api_Id)

    return youtube

youtube = Api_connect()

# Get channel information
def get_channel_info(channel_id):
    request = youtube.channels().list(                                                          
                        part = "snippet,contentDetails,statistics",
                        id = channel_id
    )
    response = request.execute()

    for i in response['items']:
        data = dict(Channel_Name = i['snippet']['title'],
                    Channel_Id = i['id'],
                    Subscribers = i['statistics']['subscriberCount'],
                    Views = i['statistics']['viewCount'],
                    Total_Videos = i['statistics']['videoCount'],
                    Channel_Description = i['snippet']['description'],
                    Playlist_Id = i['contentDetails']['relatedPlaylists']['uploads'])

    return data

# Get video ids

def get_video_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id = channel_id,
                                    part = 'contentDetails').execute()

    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None
    # To get all the video ids while loop is used
    while True:
        response1 = youtube.playlistItems().list(
                                            part = 'snippet',
                                            playlistId = Playlist_Id,
                                            maxResults = 50, 
                                            pageToken = next_page_token).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken') # .get is used so that at the end when there is no nextpage it wont show error

        if next_page_token is None: # To break the while loop when next_page_token is none. That is when last page is reached
            break
    
    return video_ids

# Get video information

def get_video_info(Video_Ids):
    video_data = []
    for video_id in Video_Ids: # Running a for loop to get the details of each video in a particular channel
        request = youtube.videos().list(
            part = 'snippet,contentDetails,statistics',
            id = video_id
        )
        response = request.execute()

        for item in response['items']:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id= item['id'],
                        Title = item['snippet']['title'],
                        Tags = item.get('tags'),
                        Thumbnail = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item.get('viewCount'),
                        Comments = item.get('commentCount'),
                        Favourite_count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_status = item['contentDetails']['caption']
                        )
            video_data.append(data)

    return video_data

# Get comment details

def get_comment_info(video_ids):
    comment_data = []

    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response['items']:
                data = {
                    'Comment_Id': item['snippet']['topLevelComment']['id'],
                    'Video_Id': item['snippet']['topLevelComment']['snippet']['videoId'],
                    'Comment_Text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'Comment_Author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_Published': item['snippet']['topLevelComment']['snippet']['publishedAt']  # Corrected field name
                }
                comment_data.append(data)

    except Exception as e: # As in some cases the comments may be turned off
        print(f"An error occurred: {e}")

    return comment_data

# Get playlist details

def get_playlist_details(channel_id):
    next_page_token = None

    All_data = []
    while True:
        request = youtube.playlists().list(
            part = "snippet,contentDetails",
            channelId = channel_id,
            maxResults = 50,
            pageToken = next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data = dict(Playlist_Id = item['id'],
                        Title = item['snippet']['title'],
                        Channel_Id = item['snippet']['channelId'],
                        Channel_Name = item['snippet']['channelTitle'],
                        PublishedAt = item['snippet']['publishedAt'],
                        Video_Count = item['contentDetails']['itemCount'])
            
            All_data.append(data)

        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break

    return All_data

client = pymongo.MongoClient('mongodb://localhost:27017')
db = client['YouTube_data']
coll1 = db['channel_details']

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids = get_video_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    coll1 = db['channel_details']
    coll1.insert_one({"channel_information": ch_details, "playlist_details": pl_details, "video_details": vi_details, "comment_details": com_details})

    return "upload completed successfully"

# Creating a database in MySQL
# Creating channels table
def channels_table():
    mydb = mysql.connector.connect(
      host="localhost",
      user="root",
      password="Dbtmysql@123",
      database="youtube_data"
    )
    cursor = mydb.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists channels(Channel_Name varchar(100), 
                                              Channel_Id varchar(80) primary key,
                                              Subscribers bigint,
                                              Views bigint,
                                              Total_Videos int,
                                              Channel_Description text,
                                              Playlist_Id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()

    except:
      print("Channels table already created")

    # Extracting data from MongoDB

    ch_list = []

    db = client["YouTube_data"]
    coll1 = db["channel_details"]

    for ch_data in coll1.find({},{"_id":0, "channel_information":1}):
        ch_list.append(ch_data['channel_information'])
    data_frame = df(ch_list)

    for index,row in data_frame.iterrows():
        insert_query = '''insert into channels(Channel_Name, 
                                              Channel_Id,
                                              Subscribers,
                                              Views,
                                              Total_Videos,
                                              Channel_Description,
                                              Playlist_Id)
                                              
                                              values(%s,%s,%s,%s,%s,%s,%s)'''

        values = (row['Channel_Name'],
                  row['Channel_Id'],
                  row['Subscribers'],
                  row['Views'],
                  row['Total_Videos'],
                  row['Channel_Description'],
                  row['Playlist_Id'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print("Channels values are already inserted")


def playlists_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Dbtmysql@123",
        database="youtube_data"
    )
    cursor = mydb.cursor()

    drop_query1 = '''drop table if exists playlists'''
    cursor.execute(drop_query1)
    mydb.commit()

    try:
        create_query = '''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                Title varchar(200),
                                                Channel_Id varchar(100),
                                                Channel_Name varchar(100),
                                                PublishedAt varchar(100),
                                                Video_Count int)'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("playlists table already created")

    pl_list = []
    db = client["YouTube_data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_details":1}):
        for i in range(len(pl_data["playlist_details"])):
            pl_list.append(pl_data["playlist_details"][i])
    data_frame1 = df(pl_list)

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Dbtmysql@123",
        database="youtube_data"
    )
    cursor = mydb.cursor()

    for index,row in data_frame1.iterrows():
        insert_query = '''insert into playlists(Playlist_Id, 
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_Count)
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''

        values = (row['Playlist_Id'],
                    row['Title'],
                    row['Channel_Id'],
                    row['Channel_Name'],
                    row['PublishedAt'],
                    row['Video_Count'])


        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print("videos already exist")

# Creating videos table

def videos_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Dbtmysql@123",
        database="youtube_data"
    )
    cursor = mydb.cursor()

    drop_query1 = '''drop table if exists videos'''
    cursor.execute(drop_query1)
    mydb.commit()

    create_query = '''create table if not exists videos(Channel_Name varchar(100),
                                                            Channel_Id varchar(100),
                                                            Video_Id varchar(30),
                                                            Title varchar(150),
                                                            Tags text,
                                                            Thumbnail varchar(200),
                                                            Duration varchar(200),
                                                            Views bigint,
                                                            Comments int,
                                                            Favourite_count int,
                                                            Definition varchar(10),
                                                            Caption_status varchar(50))'''

    cursor.execute(create_query)
    mydb.commit()

    vi_list = []
    db = client["YouTube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_details":1}):
        for i in range(len(vi_data["video_details"])):
            vi_list.append(vi_data["video_details"][i])
    data_frame11 = df(vi_list)

    for index,row in data_frame11.iterrows():
        insert_query = '''insert into videos(Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Tags,
                                                Thumbnail,
                                                Duration,
                                                Views,
                                                Comments,
                                                Favourite_count,
                                                Definition,
                                                Caption_status)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values = (row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Duration'],
                    row['Views'],
                    row['Comments'],
                    row['Favourite_count'],
                    row['Definition'],
                    row['Caption_status'],)


        cursor.execute(insert_query,values)
        mydb.commit()

# Creating comments table

def comments_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Dbtmysql@123",
        database="youtube_data"
    )
    cursor = mydb.cursor()

    drop_query1 = '''drop table if exists comments'''
    cursor.execute(drop_query1)
    mydb.commit()

    create_query = '''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                Video_Id varchar(100),
                                                Comment_Text text,
                                                Comment_Author varchar(150),
                                                Comment_Published varchar(100))'''

    cursor.execute(create_query)
    mydb.commit()

    com_list = []
    db = client["YouTube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_details":1}):
        for i in range(len(com_data["comment_details"])):
            com_list.append(com_data["comment_details"][i])
    data_frame12 = df(com_list)

    for index,row in data_frame12.iterrows():
        insert_query = '''insert into comments(Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published)
                                                
                                                values(%s,%s,%s,%s,%s)'''

        values = (row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_Published'])

        cursor.execute(insert_query,values)
        mydb.commit()

def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()

    return "Tables created successfully"

def show_channels():
    ch_list = []

    db = client["YouTube_data"]
    coll1 = db["channel_details"]

    for ch_data in coll1.find({},{"_id":0, "channel_information":1}):
        ch_list.append(ch_data['channel_information'])
    data_frame = st.dataframe(ch_list)
    
    return data_frame

def show_playlists():
    pl_list = []
    db = client["YouTube_data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_details":1}):
        for i in range(len(pl_data["playlist_details"])):
            pl_list.append(pl_data["playlist_details"][i])
    data_frame1 = st.dataframe(pl_list)

    return data_frame1
        
def show_videos():
    vi_list = []
    db = client["YouTube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_details":1}):
        for i in range(len(vi_data["video_details"])):
            vi_list.append(vi_data["video_details"][i])
    data_frame2 = st.dataframe(vi_list)
    return data_frame2

def show_comments():
    com_list = []
    db = client["YouTube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_details":1}):
        for i in range(len(com_data["comment_details"])):
            com_list.append(com_data["comment_details"][i])
    data_frame3 = st.dataframe(com_list)
    return data_frame3

# Stream lit UI

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skills Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")


channel_id = st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids = []
    db = client["YouTube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0, "channel_information":1}):
        ch_ids.append(ch_data['channel_information']['Channel_Id'])

    if channel_id in ch_ids:
        st.success("Channel details of given id already exists.")

    else:
        insert = channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to sql"):
    Table = tables()
    st.success(Table)

show_table = st.radio("Select the table for viiew", ("Channels","Playlists","Vidoes","Comments"))

if show_table == "Channels":
    show_channels()

if show_table == "Playlists":
    show_playlists()
    
if show_table == "Vidoes":
    show_videos()   

if show_table == "Comments":
    show_comments()

# SQL Connection

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Dbtmysql@123",
    database="youtube_data"
)
cursor = mydb.cursor()

questions = st.selectbox("Select your question", ("1. All the videos and the channel name",
                                                  "2. Channels with mosth number of videos",
                                                  "3. 10 most viewed videos"))
