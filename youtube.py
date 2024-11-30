from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import psycopg2
import streamlit as st
from streamlit_option_menu import option_menu
import warnings
warnings.filterwarnings('ignore')

#Api_connection

def Api_connect():
    Api_Id ="AIzaSyC_2Hs1bPxIPbPHakmp4T2CuwCzRRPM8tg"
    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()

#getting channel information

def get_channel_info(channel_ids):
    
    channel_ids_str = ','.join(channel_ids)
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_ids_str
    )
    response = request.execute()

    all_data = []
    for i in response['items']:
        data = dict(
            Channel_Name=i["snippet"]["title"],
            Channel_Id=i["id"],
            Subscribers=i['statistics']['subscriberCount'],
            Views=i["statistics"]["viewCount"],
            Total_Videos=i["statistics"]["videoCount"],
            Channel_Description=i["snippet"]["description"],
            Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"]
        )
        all_data.append(data)
        
    return all_data

channel_ids= [ "UCRxAgfYexGLlu1WHGIMUDqw",
                "UCEdFmnwp53XmtODotf788-Q",
                "UCNIuvl7V8zACPpTmmNIqP2A",
                "UCggHoXaj8BQHIiPmOxezeWA",
                "UCv_vLHiWVBh_FR9vbeuiY-A",
                "UCx-dJoP9hFCBloY9qodykvw",
                "UCHdluULl5c7bilx1x1TGzJQ",
                "UCXsQlHGuoWqukC9vz-uonrg",
                "UCVl6ZdslZz2Zj-34bMJFPbg",
                "UCJcycnanWtyOGcz34jUlYZA"]
channel_info = get_channel_info(channel_ids)

df_1 = pd.DataFrame(channel_info)
# Remove duplicates from channels DataFrame
df_channels = df_1.drop_duplicates(subset=['Channel_Id'])
# getting video ids

def get_videos_ids(channel_ids):
    
    video_ids=[]
    
    response=youtube.channels().list(id=channel_ids,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
        
    return video_ids

video_ids= [(video_id) for channel_ids in channel_ids for video_id in get_videos_ids(channel_ids)]

# getting video info

def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)    
    return video_data

video_data = get_video_info(video_ids)

df_2 = pd.DataFrame(video_data)
# Remove duplicates from videos DataFrame
df_videos = df_2.drop_duplicates(subset=['Video_Id'])
# getting comment details

def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data

Comment_data = get_comment_info(video_ids)

df_3 = pd.DataFrame(Comment_data)
# Remove duplicates from comments DataFrame
df_comments = df_3.drop_duplicates(subset=['Comment_Id'])

# getting playlist details

def get_playlist_details_for_multiple_channels(channel_ids):
    all_playlist_data = []

    for channel_id in channel_ids:
        next_page_token = None
        while True:
            try:
                request = youtube.playlists().list(
                    part='snippet,contentDetails',
                    channelId=channel_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                response = request.execute()

                for item in response['items']:
                    data = {
                        'Playlist_Id': item['id'],
                        'Title': item['snippet']['title'],
                        'Channel_Id': item['snippet']['channelId'],
                        'Channel_Name': item['snippet']['channelTitle'],
                        'PublishedAt': item['snippet']['publishedAt'],
                        'Video_Count': item['contentDetails']['itemCount']
                    }
                    all_playlist_data.append(data)

                next_page_token = response.get('nextPageToken')
                if next_page_token is None:
                    break
            
            except HttpError as e:
                if e.resp.status == 404:
                    print(f"Channel not found: {channel_id}")
                    break  
                else:
                    raise  

    return all_playlist_data

all_playlist_data = get_playlist_details_for_multiple_channels(channel_ids)

df_4 = pd.DataFrame(all_playlist_data)
# Remove duplicates from playlists DataFrame
df_playlists = df_4.drop_duplicates(subset=['Playlist_Id'])

#inserting the details in Postgres


    # SQL connection
mydb = psycopg2.connect(host="localhost",
                        port="5432",
                        user="postgres",
                        database="youtube data",
                        password="John@2394")
cursor = mydb.cursor()

# Channel table creation and insertion
create_query_1 = '''CREATE TABLE if not exists channels(
                    Channel_Name varchar ,
                    Channel_Id varchar primary key,
                    Subscribers bigint,
                    Views bigint,
                    Total_Videos int,
                    Description text,
                    Playlist_Id varchar)'''
cursor.execute(create_query_1)
mydb.commit()
insert_query_1 = '''INSERT INTO channels (Channel_Name,
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Description,
                                            Playlist_Id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (Channel_Id) 
                    DO UPDATE SET 
                        Channel_Name = EXCLUDED.Channel_Name,
                        Subscribers = EXCLUDED.Subscribers,
                        Views = EXCLUDED.Views,
                        Total_Videos = EXCLUDED.Total_Videos,
                        Description = EXCLUDED.Description,
                        Playlist_Id = EXCLUDED.Playlist_Id;'''

df_channels = df_1.drop_duplicates(subset=['Channel_Id'])
data_to_insert_1 = [tuple(x) for x in df_channels.itertuples(index=False, name=None)]
cursor.executemany(insert_query_1, data_to_insert_1)
mydb.commit()
    
 # Video table creation
create_query_2 = '''CREATE TABLE IF NOT EXISTS videos(
                        Channel_Name VARCHAR,
                        Channel_id VARCHAR PRIMARY KEY,
                        Video_Id VARCHAR,
                        Title TEXT,
                        Tags TEXT,
                        Thumbnail BYTEA,
                        Description TEXT,
                        Published_Date TIMESTAMPTZ,
                        Duration TEXT,
                        Views BIGINT,
                        Likes BIGINT,
                        Comments BIGINT,
                        Favorite_Count INT,
                        Definition TEXT,
                        Caption_Status TEXT
                    )'''
cursor.execute(create_query_2)
mydb.commit()  

# Insertion query
insert_query_2 = '''
                INSERT INTO videos(Channel_Name, Channel_id, Video_Id, Title, Tags,
                Thumbnail, Description, Published_Date, Duration, Views, Likes, Comments, Favorite_Count, Definition, Caption_Status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (Channel_id) 
                DO UPDATE SET 
                    Video_Id = EXCLUDED.Video_Id,
                    Title = EXCLUDED.Title,
                    Tags = EXCLUDED.Tags,
                    Thumbnail = EXCLUDED.Thumbnail,
                    Description = EXCLUDED.Description,
                    Published_Date = EXCLUDED.Published_Date,
                    Duration = EXCLUDED.Duration,
                    Views = EXCLUDED.Views,
                    Likes = EXCLUDED.Likes,
                    Comments = EXCLUDED.Comments,
                    Favorite_Count = EXCLUDED.Favorite_Count,
                    Definition = EXCLUDED.Definition,
                    Caption_Status = EXCLUDED.Caption_Status
                '''

# Data insertion
df_videos = df_2.drop_duplicates(subset=['Video_Id'])
data_to_insert_2 = [tuple(x) for x in df_videos.itertuples(index=False, name=None)]
cursor.executemany(insert_query_2, data_to_insert_2)
mydb.commit()  

# Comment table creation and insertion
create_query_3 = '''CREATE TABLE IF NOT EXISTS comments(
                        Comment_Id VARCHAR PRIMARY KEY,
                        Video_Id VARCHAR,
                        Comment_Text TEXT,
                        Comment_Arthur VARCHAR,
                        Comment_Published TIMESTAMPTZ)'''
cursor.execute(create_query_3)
mydb.commit()
insert_query_3 = '''INSERT INTO comments (Comment_Id,
                                          Video_Id,
                                          Comment_Text,
                                          Comment_Arthur,
                                          Comment_Published)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (Comment_Id) 
                    DO UPDATE SET
                        Video_Id = EXCLUDED.Video_Id,
                        Comment_Text = EXCLUDED.Comment_Text,
                        Comment_Arthur = EXCLUDED.Comment_Arthur,
                        Comment_Published = EXCLUDED.Comment_Published;'''
    
df_comments = df_3.drop_duplicates(subset=['Comment_Id'])
data_to_insert_3 = [tuple(x) for x in df_comments.itertuples(index=False, name=None)]
cursor.executemany(insert_query_3, data_to_insert_3)
mydb.commit()

# Playlist table creation and insertion
create_query_4 = '''CREATE TABLE IF NOT EXISTS playlists(
                        Playlist_Id VARCHAR PRIMARY KEY,
                        Title TEXT,
                        Channel_Id VARCHAR,
                        Channel_Name TEXT,
                        PublishedAt TIMESTAMPTZ,
                        Video_Count INT)'''
cursor.execute(create_query_4)
mydb.commit()

insert_query_4 = '''INSERT INTO playlists (Playlist_Id,
                                           Title,
                                           Channel_Id,
                                           Channel_Name,
                                           PublishedAt,
                                           Video_Count)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (Playlist_Id) 
                    DO UPDATE SET
                        Title = EXCLUDED.Title,
                        Channel_Id = EXCLUDED.Channel_Id,
                        Channel_Name = EXCLUDED.Channel_Name,
                        PublishedAt = EXCLUDED.PublishedAt,
                        Video_Count = EXCLUDED.Video_Count;'''
    
df_playlists = df_4.drop_duplicates(subset=['Playlist_Id'])
data_to_insert_4 = [tuple(x) for x in df_playlists.itertuples(index=False, name=None)]
cursor.executemany(insert_query_4, data_to_insert_4)
mydb.commit()


    
# Function to connect to PostgreSQL
def connect_to_db():
    return psycopg2.connect(
        host='localhost',
        database='youtube data',
        port='5432',
        user='postgres',
        password='John@2394'
    )

# Function to fetch data from PostgreSQL

def fetch_data(query):
    connection = connect_to_db()
    df = pd.read_sql(query, connection)
    connection.close()
    return df
# Function to fetch channel details using the YouTube Data API
def fetch_channel_details(channel_id):
    api_key = "AIzaSyC_2Hs1bPxIPbPHakmp4T2CuwCzRRPM8tg"  
    youtube = build('youtube', 'v3', developerKey=api_key)
    
    try:
        request = youtube.channels().list(
            part="snippet,statistics,contentDetails",
            id=channel_id
        )
        response = request.execute()
        items = response.get("items", [])
        
        if not items:
            return None

        channel_data = items[0]
        channel_name = channel_data["snippet"]["title"]
        description = channel_data["snippet"]["description"]
        subscribers = channel_data["statistics"].get("subscriberCount", 0) or 0
        views = channel_data["statistics"].get("viewCount", 0) or 0
        total_videos = channel_data["statistics"].get("videoCount", 0) or 0
        playlist_id = channel_data["contentDetails"]["relatedPlaylists"]["uploads"]

        return (channel_name, channel_id, int(subscribers), int(views), int(total_videos), description, playlist_id)
    
    except Exception as e:
        st.error(f"Error fetching channel details: {e}")
        return None

# Function to add channel details to PostgreSQL
def add_channel_to_db(channel_details):
    connection = connect_to_db()
    cursor = connection.cursor()
    
    try:
        insert_query = """
            INSERT INTO channels (Channel_Name, Channel_Id, Subscribers, Views, Total_Videos, Description, Playlist_Id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (Channel_Id) DO UPDATE SET
                Channel_Name = EXCLUDED.Channel_Name,
                Subscribers = EXCLUDED.Subscribers,
                Views = EXCLUDED.Views,
                Total_Videos = EXCLUDED.Total_Videos,
                Description = EXCLUDED.Description,
                Playlist_Id = EXCLUDED.Playlist_Id;
        """
        cursor.execute(insert_query, channel_details)
        connection.commit()
        st.success("Channel data migrated to PostgreSQL successfully!")
    except Exception as e:
        st.error(f"Error migrating channel data to PostgreSQL: {e}")
    finally:
        cursor.close()
        connection.close()

# Streamlit Sidebar for adding and migrating channel IDs
st.sidebar.subheader("Manage Channel IDs")

# Input for a new channel ID
new_channel_id = st.sidebar.text_input("Enter new YouTube Channel ID:")

# Button to fetch and migrate channel data
if st.sidebar.button("Fetch and Migrate"):
    if new_channel_id:
        channel_details = fetch_channel_details(new_channel_id)
        if channel_details:
            st.write("### Fetched Channel Details")
            st.write(pd.DataFrame([channel_details], columns=[
                "Channel Name", "Channel ID", "Subscribers", "Views", "Total Videos", "Description", "Playlist ID"
            ]))
            add_channel_to_db(channel_details)
        else:
            st.warning("No channel data found for the provided ID.")
    else:
        st.sidebar.warning("Please enter a valid Channel ID.")
        
# Function to retrieve channel details from the database
def get_all_channels_from_db():
    connection = connect_to_db()
    cursor = connection.cursor()
    
    try:
        fetch_query = "SELECT Channel_Name, Channel_Id, Subscribers, Views, Total_Videos, Description, Playlist_Id FROM channels;"
        cursor.execute(fetch_query)
        results = cursor.fetchall()
        columns = ["Channel Name", "Channel ID", "Subscribers", "Views", "Total Videos", "Description", "Playlist ID"]
        return pd.DataFrame(results, columns=columns)
    except Exception as e:
        st.error(f"Error retrieving channel data: {e}")
        return None
    finally:
        cursor.close()
        connection.close()        
# Section to display all migrated channel details
st.subheader("Migrated Channel Details")

if st.button("View All Channels"):
    channel_data = get_all_channels_from_db()
    if channel_data is not None and not channel_data.empty:
        st.write("### Channels in PostgreSQL Database")
        st.dataframe(channel_data)
    else:
        st.warning("No channel data available in the database.")  

# Define all the queries in a dictionary
queries = {
    "1.What are the names of all the videos and their corresponding channels?": """
        SELECT 
    COALESCE(v.title, 'No Videos') AS Video_Title, 
    c.channel_name AS Channel_Name
        FROM 
            channels c
        LEFT JOIN 
            videos v 
        ON 
            c.channel_id = v.channel_id
        ORDER BY 
            c.channel_name;
    """,
    "2.Which channels have the most number of videos, and how many videos do they have?": """
        SELECT channel_name AS Channel_Name, total_videos AS Total_Videos
        FROM channels
        ORDER BY total_videos DESC;
    """,
    "3.What are the top 10 most viewed videos and their respective channels?": """
        SELECT channel_name AS Channel_Name, title AS Video_Title, views AS Views
        FROM videos
        ORDER BY views DESC
        LIMIT 10;
    """,
    "4.How many comments were made on each video, and what are their corresponding video names?": """
        SELECT a.video_id AS Video_id, a.title AS Video_Title, b.Total_Comments
        FROM videos AS a
        LEFT JOIN (
            SELECT video_id, COUNT(comment_id) AS Total_Comments
            FROM comments
            GROUP BY video_id
        ) AS b
        ON a.video_id = b.video_id
        ORDER BY b.Total_Comments DESC;
    """,
    "5.Which videos have the highest number of likes, and what are their corresponding channel names?": """
        SELECT channel_name AS Channel_Name, title AS Title, likes AS Likes_Count
        FROM videos
        ORDER BY likes DESC
        LIMIT 10;
    """,
    "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?": """
        SELECT title AS Title, likes AS Likes_Count
        FROM videos
        ORDER BY likes DESC;
    """,
    "7.What is the total number of views for each channel, and what are their corresponding channel names?": """
        SELECT channel_name AS Channel_Name, views AS Views
        FROM channels
        ORDER BY views DESC;
    """,
    "8.What are the names of all the channels that have published videos in the year 2022?": """
        SELECT channel_name AS Channel_Name, COUNT(video_id) AS Total_Videos_Published
        FROM videos
        WHERE EXTRACT(YEAR FROM published_date) = 2022
        GROUP BY channel_name;
    """,
    "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?": """
        WITH parsed_durations AS (
            SELECT 
                channel_name, 
                (REPLACE(REPLACE(REPLACE(REPLACE(duration, 'PT', ''), 'H', ' hours '), 'M', ' minutes '), 'S', ' seconds '))::INTERVAL AS duration_interval
            FROM 
                videos
        ),
        channel_avg_durations AS (
            SELECT 
                channel_name, 
                AVG(duration_interval) AS avg_duration
            FROM 
                parsed_durations
            GROUP BY 
                channel_name
        )
        SELECT 
            channel_name,
            EXTRACT(HOUR FROM avg_duration) AS avg_hours,
            EXTRACT(MINUTE FROM avg_duration) AS avg_minutes,
            EXTRACT(SECOND FROM avg_duration) AS avg_seconds
        FROM 
            channel_avg_durations;
    """,
    "10.Which videos have the highest number of comments, and what are their corresponding channel names?": """
        SELECT channel_name AS Channel_Name, video_id AS Video_ID, comments AS Comments
        FROM videos
        ORDER BY comments DESC
        LIMIT 10;
    """
}

# Streamlit app
st.title("YouTube Data Analysis")

# Dropdown to select the question
question = st.selectbox(
    'Select the question to run:',
    list(queries.keys())
)

# Fetch and display the corresponding data
if question:
    query = queries[question]
    data = fetch_data(query)
    
    # Display the data in a table format
    st.write(f"**{question.split('.')[1].strip()}**")
    st.dataframe(data)
    

# Fetch and display the corresponding data
if question:
    query = queries[question]  # Get the query corresponding to the selected question
    data = fetch_data(query)
    
    # Display the data in a table format
    st.write(f"**{question.split('.')[1].strip()}**")
    st.dataframe(data)
# Section to add new channel IDs

st.sidebar.subheader("Add New Channel ID")
new_channel_id = st.sidebar.text_input("Enter new YouTube Channel ID:")
if st.sidebar.button("Add Channel ID"):
    if new_channel_id:
        add_channel_to_db(new_channel_id)
    else:
        st.sidebar.warning("Please enter a Channel ID.")
