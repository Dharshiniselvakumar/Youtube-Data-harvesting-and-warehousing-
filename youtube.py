
from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
from datetime import datetime,timedelta
import streamlit as st

api_key='AIzaSyBDd8ei9IHIOM_LJx8CV0HlT-FO3yDHa_k'
youtube=build('youtube','v3',developerKey=api_key)

#getting channel info
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    for i in response['items']:
        data=dict(channel_name=i['snippet']['title'],
                channel_id=i['id'],
                subscriber_count=i['statistics']['subscriberCount'],
                view_count=i['statistics']['viewCount'],
                channel_discription=i['snippet']['description'],
                total_videos=i['statistics']['videoCount'],
                playlist_id=i['contentDetails']['relatedPlaylists']['uploads']
                )
        return data
    
#getting video_ids
def get_video_id(channel_id):
    video_ids=[]
    response = youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page_token=None
    
    while True:
        response1 = youtube.playlistItems().list(part='snippet', playlistId=playlist_id,maxResults=50,pageToken=next_page_token).execute()
        
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken') 
        if next_page_token is None:
            break
    return video_ids
                                                                        
        
                                            
#getting video information
def get_video_details(video_details):
    video_data=[]
    for video_id in video_details:
        request = youtube.videos().list(part="snippet,contentDetails,statistics",
                                        id=video_id)
        response = request.execute()
        for i in response['items']:
            data=dict(channel_name=i['snippet']['channelTitle'],
                    channel_id=i['snippet']['channelId'],
                    video_id=i['id'],
                    title=i['snippet']['title'],
                    tags=','.join(i['snippet'].get('tags', [])),
                    thumbnail=i['snippet']['thumbnails']['default']['url'],
                    description=i['snippet'].get('description'),
                    published_date=i['snippet']['publishedAt'],
                    duration=i['contentDetails']['duration'],
                    views=i['statistics'].get('viewCount'),
                    Likes=i['statistics'].get('likeCount'),        
                    Comments=i['statistics'].get('commentCount'),
                    Favorite_count=i['statistics']['favoriteCount'],
                    Definition=i['contentDetails']['definition'],
                    Caption_status=i['contentDetails']['caption'])
        video_data.append(data)
    return video_data


#getting comment information
def get_comment_info(video_details):
    Comment_data=[]
    try:
        for video_id in video_details:
            request = youtube.commentThreads().list(part='snippet',videoId=video_id,maxResults=100)
            response = request.execute()
            for i in response['items']:
                        data=dict(comment_Id=i['snippet']['topLevelComment']['id'],
                                video_Id=i['snippet']['topLevelComment']['snippet']['videoId'],
                                Comment_Text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                                Comment_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                Comment_published=i['snippet']['topLevelComment']['snippet']['publishedAt'])
                        Comment_data.append(data)
    except:
        pass
    
    return Comment_data   
        

#getting playlistid
def getting_playlist_details(channel_id):
    next_page_token=None
    playlist_data=[]
    while True:
        request = youtube.playlists().list(part="snippet,contentDetails",
                                                channelId=channel_id,maxResults=50,pageToken=next_page_token)
        response = request.execute()
        for i in response['items']:
            data=dict(playlist_id=i['id'],
                    Title=i['snippet']['title'],
                    channel_id=i['snippet']['channelId'],
                    channel_name=i['snippet']['channelTitle'],
                    publishedAt=i['snippet']['publishedAt'],
                    video_count=i['contentDetails']['itemCount'])
            playlist_data.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return  playlist_data  



#uploading data to mongoDB

#Creating connection,database & collection

connection=pymongo.MongoClient("mongodb://localhost:27017/")#connection
db=connection['youtube1']
col=db['ychannel_details']

#Inserting into collection

def channel_info(channel_id):
    channel_details=get_channel_info(channel_id)
    playlist_details=getting_playlist_details(channel_id)
    video_ids=get_video_id(channel_id)
    video_details=get_video_details(video_ids)
    comment_details=get_comment_info(video_ids)
    col.insert_one({'channel_information':channel_details,'playlist_information':playlist_details,'video_information':video_details,'comment_information':comment_details})
    return 'uploaded'

  
#Creating SQL tables and migrating data from mongoDB

#Creating channel_table

def channel_table(): 
    mydb=mysql.connector.connect(host='localhost',
                            user='root',
                            password='12345',
                            database='youtube1')#connector
    mycursor=mydb.cursor()
    drop_query='''drop table if exists channels'''
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        sql='''create table if not exists channels(channel_name varchar(100),
                                                channel_id varchar(100)primary key,
                                                subscriber_count bigint,
                                                view_count bigint,
                                                channel_discription text,
                                                total_videos int,
                                                playlist_id varchar(100))'''
        mycursor.execute(sql)
        mydb.commit()
    except:
        print('error')

    #converting collection to dataframe
    
    channel_list=[]
    db=connection['youtube1']
    col=db['ychannel_details']
    for i in col.find({},{'_id':0,'channel_information':1}):
        channel_list.append(i['channel_information'])
    df=pd.DataFrame(channel_list) 

    #Inserting values into table
    
    for index,row in df.iterrows():
        sql='''insert into channels(channel_name,
                                    channel_id,
                                    subscriber_count,
                                    view_count,
                                    channel_discription,
                                    total_videos,
                                    playlist_id)
                                    values(%s,%s,%s,%s,%s,%s,%s)'''
    
        values=(row['channel_name'],
                row['channel_id'],
                row['subscriber_count'],
                row['view_count'],
                row['channel_discription'],
                row['total_videos'],
                row['playlist_id'])
        try:
            mycursor.execute(sql,values)
            mydb.commit()
        except:
            print('ERROR')

#Creating playlist_table

def playlist_table():
    mydb=mysql.connector.connect(host='localhost',
                            user='root',
                            password='12345',
                            database='youtube1')#connector
    mycursor=mydb.cursor()
    drop_query='''drop table if exists playlists'''
    mycursor.execute(drop_query)
    mydb.commit()
    
    sql='''create table if not exists playlists(playlist_id varchar(100) primary key,
                                            Title varchar(500),
                                            channel_id varchar(100),
                                            channel_name varchar(100),
                                            published_At timestamp,
                                            video_count int)'''
    mycursor.execute(sql)
    mydb.commit()
    
#converting collection to dataframe

    playlist_list=[]
    db=connection['youtube1']
    col=db['ychannel_details']
    for i in col.find({},{'_id':0,'playlist_information':1}):
        for n in range(len(i['playlist_information'])):
            playlist_list.append(i['playlist_information'][n])
        
    df1=pd.DataFrame(playlist_list)

#Inserting values into table
    
    for index,row in df1.iterrows():
    # Parse and format the 'publishedAt' value
        publishedAt = datetime.strptime(row['publishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
        
        sql='''insert into playlists(playlist_id,
                                    Title,
                                    channel_id,
                                    channel_name,
                                    published_At,
                                    video_count)
                                    values(%s,%s,%s,%s,%s,%s)'''
        values=(row['playlist_id'],
                row['Title'],
                row['channel_id'],
                row['channel_name'],
                publishedAt,
                row['video_count'])
        
        mycursor.execute(sql,values)
        mydb.commit()

#Creating videolist_table

def videolist_table():
              
        mydb=mysql.connector.connect(host='localhost',
                            user='root',
                            password='12345',
                            database='youtube1')#connector
        mycursor=mydb.cursor()
        drop_existing_table='''drop table if exists videolists'''
        mycursor.execute(drop_existing_table)
        mydb.commit()
        
        sql='''create table if not exists videolists(channel_name varchar(100),
                                                channel_id varchar(100),
                                                video_id varchar(20) primary key,
                                                title varchar(200),
                                                tags text,
                                                thumbnail varchar(200),
                                                description text,
                                                published_date timestamp,
                                                duration int,
                                                views bigint,
                                                Likes bigint,        
                                                Comments int,
                                                Favorite_count int ,
                                                Definition varchar(10),
                                                Caption_status varchar(100))'''
        mycursor.execute(sql)
        mydb.commit()
        
        #converting collection to dataframe

        video_list=[]
        db=connection['youtube1']
        col=db['ychannel_details']
        for i in col.find({},{'_id':0,'video_information':1}):
            for n in range(len(i['video_information'])):
                video_list.append(i['video_information'][n])
        
        df2=pd.DataFrame(video_list)

        #Inserting values into table
        
        for index,row in df2.iterrows():
            #format the 'publishedAt' value
            from datetime import datetime
            published_date = datetime.strptime(row['published_date'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
            
            #format the 'duration' value
            
            duration_str = row['duration']

            try:
                # Try parsing with format "PT%MM%SS"
                duration_timedelta = datetime.strptime(duration_str, "PT%MM%SS").time()
            except ValueError:
                try:
                    # If the first format fails, try parsing with format "PT%SS"
                    duration_timedelta = datetime.strptime(duration_str, "PT%SS").time()
                except ValueError:
                    try:
                        # If the second format fails, try parsing with format "PT%MM"
                        duration_timedelta = datetime.strptime(duration_str, "PT%MM").time()
                    except ValueError:
                        print(f"Invalid duration format: {duration_str}")

            # Calculate the total seconds in the timedelta
            total_seconds = duration_timedelta.minute * 60 + duration_timedelta.second
            # Convert the total seconds to an integer
            duration_seconds = int(total_seconds)

            
            sql='''insert into videolists(channel_name,
                                        channel_id,
                                        video_id,
                                        title,
                                        tags,
                                        thumbnail,
                                        description,
                                        published_date,
                                        duration,
                                        views,
                                        Likes,        
                                        Comments,
                                        Favorite_count,
                                        Definition,
                                        Caption_status)
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            values=(row['channel_name'],
                    row['channel_id'],
                    row['video_id'],
                    row['title'],
                    row['tags'],
                    row['thumbnail'],
                    row['description'],
                    published_date,
                    duration_seconds,
                    row['views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_count'],
                    row['Definition'],
                    row['Caption_status'])
            mycursor.execute(sql,values)
            mydb.commit()
            
            
#Creating comment_list_table           

def comment_list_table():

    mydb=mysql.connector.connect(host='localhost',
                            user='root',
                            password='12345',
                            database='youtube1')#connector
    mycursor=mydb.cursor()
    drop_query='''drop table if exists comment_lists'''
    mycursor.execute(drop_query)
    mydb.commit()
    
    sql='''create table if not exists comment_lists(comment_Id varchar(100) primary key,
                                            video_Id varchar(100),
                                            Comment_Text text,
                                            Comment_author varchar(200),
                                            Comment_published datetime)'''
                                            
    mycursor.execute(sql)
    mydb.commit()

    #converting collection to dataframe
    
    comment_list=[]
    db=connection['youtube1']
    col=db['ychannel_details']
    for i in col.find({},{'_id':0,'comment_information':1}):
        for n in range(len(i['comment_information'])):
            comment_list.append(i['comment_information'][n])
        
    df3=pd.DataFrame(comment_list)
    df3
    
    #Inserting values into table

    for index,row in df3.iterrows():
    # formatting the 'Comment_published' value
        Comment_published = datetime.strptime(row['Comment_published'], '%Y-%m-%dT%H:%M:%SZ')
        sql='''insert into comment_lists(comment_Id,
                                    video_Id,
                                    Comment_Text,
                                    Comment_author,
                                    Comment_published)
                                    values(%s,%s,%s,%s,%s)'''
        values=(row['comment_Id'],
                row['video_Id'],
                row['Comment_Text'],
                row['Comment_author'],
                Comment_published)
        
        mycursor.execute(sql,values)
        mydb.commit()
    
#defining all tables in single function
   
def sql_tables():
    channel_table()
    playlist_table()
    videolist_table()
    comment_list_table()

    return 'created'

#creating function to display table for channel details,playlist,video details & comments

def show_channel_table():
    channel_list=[]
    db=connection['youtube1']
    col=db['ychannel_details']
    for i in col.find({},{'_id':0,'channel_information':1}):
        channel_list.append(i['channel_information'])
    df=st.dataframe(channel_list) 
    return df

def show_playlist_table():
    playlist_list=[]
    db=connection['youtube1']
    col=db['ychannel_details']
    for i in col.find({},{'_id':0,'playlist_information':1}):
        for n in range(len(i['playlist_information'])):
            playlist_list.append(i['playlist_information'][n])
        
    df1=st.dataframe(playlist_list)
    return df1

def show_video_table():
    video_list=[]
    db=connection['youtube1']
    col=db['ychannel_details']
    for i in col.find({},{'_id':0,'video_information':1}):
        for n in range(len(i['video_information'])):
            video_list.append(i['video_information'][n])
    
    df2=st.dataframe(video_list)
    return df2

def show_comment_table():
    comment_list=[]
    db=connection['youtube1']
    col=db['ychannel_details']
    for i in col.find({},{'_id':0,'comment_information':1}):
        for n in range(len(i['comment_information'])):
            comment_list.append(i['comment_information'][n])
        
    df3=st.dataframe(comment_list)
    return df3

#streamlit creation

with st.sidebar:

    st.title(":red[YOUTUBE DATA HARVESTING & WAREHOUSING]")
    st.header("USING SQL, MONGODB & STREAMLIT")
    st.subheader(":blue[Skills take away]")
    st.caption("Python scripting")
    st.caption("Data Collection")
    st.caption("API integration")
    st.caption("Data Management using MongoDB and SQL")
    st.caption("Streamlit")

#creating text box for entering channel id

channel_id=st.text_input("Enter Channel ID")

#creating button for collecting data from youtube API in mongoDB

if st.button("collecting & storing data"):
    c_id=[]
    db=connection['youtube']
    col=db['channel_details']
    for i in col.find({},{'_id':0,'channel_information':1}):
        c_id.append(i['channel_information']['channel_id'])

    if channel_id in c_id:
        st.success("channel id already exists")
    else:
        inserted=channel_info(channel_id)
        st.success(inserted)


#creating button for migrating data to SQL

if st.button("Migrate data to SQL"):
    Table_view=sql_tables()
    st.success(Table_view)

show_table=st.radio("Select Table to view",("Channels","Playlists","Videos","Comments"))

if show_table=="Channels":
    show_channel_table()

elif show_table=="Playlists":
    show_playlist_table()

elif show_table=="Videos":
    show_video_table()
    
else:
    show_comment_table()

mydb=mysql.connector.connect(host='localhost',
                        user='root',
                        password='12345',
                        database='youtube1')#connector
mycursor=mydb.cursor()

#creating questions to be selected to get answers by selectbox

questions=st.selectbox("Select Questions to be answered",("Please Select",
                                                        "1.All the videos and their corresponding channels",
                                                        "2.Channel with most number of videos",
                                                        "3.10 most viewed channel and its name",
                                                        "4.Video names and number of comments in each video",
                                                        "5.videos having highest number of likes, and its channel name",
                                                        "6.Number of likes for each video, and its video name ",
                                                        "7.Number of views for each channel, and its channel name",
                                                        "8.channels that have published videos in 2022",
                                                        "9.average duration of all videos in each channel and channel name",
                                                        "10.videos having highest number of comments and its channel name"))

if questions=='Please Select':
    print('')

elif questions=="1.All the videos and their corresponding channels":
    qus1='''select title as videos,channel_name as channelname from videolists '''
    mycursor.execute(qus1)
    q1=mycursor.fetchall()
    dff1=pd.DataFrame(q1,columns=['videos','channel name'])
    st.write(dff1)

elif questions=="2.Channel with most number of videos":
    qus2='''select channel_name as channelname,total_videos as no_of_videos from channels order by total_videos desc'''
    mycursor.execute(qus2)
    q2=mycursor.fetchall()
    dff2=pd.DataFrame(q2,columns=['channelname','no_of_videos'])
    st.write(dff2)
    

elif questions=="3.10 most viewed channel and its name":
    qus3='''select views as views,channel_name as channelname,title as videotitle from videolists
                where views is not null order by views desc limit 10'''
    mycursor.execute(qus3)
    q3=mycursor.fetchall()
    dff3=pd.DataFrame(q3,columns=['views','channel name','video title'])
    st.write(dff3)

elif questions=="4.Video names and number of comments in each video":
    qus4='''select comments as no_of_comments,title as videotitle from videolists where comments is not null'''
                
    mycursor.execute(qus4)
    q4=mycursor.fetchall()
    dff4=pd.DataFrame(q4,columns=['no of comments','video title'])
    st.write(dff4)

elif questions=="5.videos having highest number of likes, and its channel name":
    qus5='''select title as videotitle,channel_name as channelname,likes as no_of_likes from videolists
                where likes is not null order by likes desc'''
    mycursor.execute(qus5)
    q5=mycursor.fetchall()
    dff5=pd.DataFrame(q5,columns=['video title','channel name','no of likes'])
    st.write(dff5)

elif questions=="6.Number of likes for each video, and its video name ":
    qus6='''select likes as likescount,title as videotitle from videolists where likes is not null'''
                
    mycursor.execute(qus6)
    q6=mycursor.fetchall()
    dff6=pd.DataFrame(q6,columns=['no of likes','video title'])
    st.write(dff6)

elif questions=="7.Number of views for each channel, and its channel name":
    qus7='''select channel_name as channelname,view_count as no_of_views from channels'''
    mycursor.execute(qus7)
    q7=mycursor.fetchall()
    dff7=pd.DataFrame(q7,columns=['channel name','No of views'])
    st.write(dff7)

elif questions=="8.channels that have published videos in 2022":
    qus8='''select channel_name as channelname,title as videotitle,published_date as published_year from videolists
                where extract(year from published_date)=2022'''
    mycursor.execute(qus8)
    q8=mycursor.fetchall()
    dff8=pd.DataFrame(q8,columns=['channel name','video title','published year'])
    st.write(dff8)

elif questions=="9.average duration of all videos in each channel and channel name":
    qus9='''select channel_name as channelname,AVG(duration) as averageduration from videolists group by channel_name'''
                
    mycursor.execute(qus9)
    q9=mycursor.fetchall()
    dff9=pd.DataFrame(q9,columns=['channelname','averageduration'])
    
    t9=[]
    for index,row in dff9.iterrows():
        channel_title=row['channelname']
        average_duration=row['averageduration']
        average_duration_str=str(average_duration)
        t9.append(dict(channeltitle=channel_title,averagduration=average_duration_str))
    
    dfff9=pd.DataFrame(t9)
    st.write(dfff9)


elif questions=="10.videos having highest number of comments and its channel name":
    qus10='''select channel_name as channelname,title as videoname,comments as comments from videolists
                where comments is not null order by comments desc'''
                
    mycursor.execute(qus10)
    
    q10=mycursor.fetchall()
    dff10=pd.DataFrame(q10,columns=['channel name','video name','no of comments'])
    st.write(dff10)
    
    
    
    


    

    
    

    
        


                                                        

                                                        









    
    



    
                                                            
                                                        
    

    
    


    
    