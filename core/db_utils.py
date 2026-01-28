from cassandra.cluster import Cluster
from faker import Faker
import uuid 
import random 
import datetime
import pandas as pd 


nb_users = 20
nb_posts = 20 
fake = Faker()


cluster = Cluster(['localhost'] , port=9042) 
session = cluster.connect() 


######################################################## #########################
###############            initializing schema                  #################
#################################################################################
def init_database(insert_data=False) : 
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS demo WITH replication = {
            'class' : 'SimpleStrategy' , 
            'replication_factor' : '1'
        }
    """)
    session.set_keyspace("demo")

    session.execute("""
        CREATE TABLE IF NOT EXISTS demo.users (
            user_id UUID PRIMARY KEY , 
            full_name text , 
            age int         
        )         
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS demo.posts (
            post_id UUID PRIMARY KEY , 
            content text , 
            publisher UUID    
        )         
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS demo.likes (
            post_id uuid , 
            event_time timestamp , 
            user_id uuid , 
            PRIMARY KEY ((post_id) , event_time , user_id )
        )         
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS  likes_count_by_post (
            post_id uuid PRIMARY KEY,
            likes counter
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS likes_by_day (
            day date,
            post_id uuid,
            likes counter,
            PRIMARY KEY ((day), post_id)
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS user_likes (
            user_id uuid PRIMARY KEY,
            likes counter
        )
    """)


    if insert_data :
        users_pstatement = session.prepare("INSERT INTO users (user_id , full_name ,age) VALUES (?,?,?)")
        posts_pstatement = session.prepare("INSERT INTO posts (post_id , content ,publisher) VALUES (?,?,?)")

        uuids = []
        for _ in range(nb_users ) : 
            id = uuid.uuid4() 
            session.execute(users_pstatement , (id , fake.name() , random.randint(20 , 60))) 
            uuids.append(id)

        for _ in range(nb_posts ) : 
            session.execute(posts_pstatement , (uuid.uuid4() , fake.text(max_nb_chars=100) ,uuids[random.randint(0 , len(uuids) - 1)] )) 
        
#################################################################################
##################      Getting existing posts and users    #####################
#################################################################################

def get_users_uuids() : 
    res = session.execute("SELECT * FROM demo.users") ; 
    data = [] 
    for row in res : 
        data.append(row.user_id) 
    return data 

def get_posts_uuids() : 
    res = session.execute("SELECT * FROM demo.posts") ; 
    data = [] 
    for row in res : 
        data.append(row.post_id) 
    return data


#################################################################################
################        Insert Event (Like) to different tables  ################
#################################################################################
def insert_event(event) : 
    prepared_statement = session.prepare("INSERT INTO demo.likes (post_id , event_time , user_id) VALUES (?,?,?)") 
    prepared_statement_1 = session.prepare("UPDATE demo.likes_count_by_post SET likes = likes + 1 WHERE post_id = ?") 
    prepared_statement_2 = session.prepare("UPDATE demo.likes_by_day SET likes = likes + 1 WHERE day=? AND post_id=?")
    prepared_statement_3 = session.prepare("UPDATE demo.user_likes  SET likes = likes + 1 WHERE user_id=?")



    session.execute(prepared_statement , [uuid.UUID(event['post_id']) , datetime.datetime.now() , uuid.UUID(event['user_id'])])
    session.execute(prepared_statement_1 , [uuid.UUID(event['post_id'])])
    session.execute(prepared_statement_2 , [ datetime.date.today() , uuid.UUID(event['post_id'])])
    session.execute(prepared_statement_3 , [ uuid.UUID(event['user_id'])])


#################################################################################
###########    Queries that's get some statistics , just for testing    #########
#################################################################################

def get_most_liked_post() : 
    res = session.execute("SELECT post_id, likes FROM likes_count_by_post")
    res = sorted(res, key=lambda x: x.likes, reverse=True)
    values = [] 
    columns = []
    for row in res[:5]: 
        values.append(row.likes) 
        columns.append(str(row.post_id))
    

    df = pd.DataFrame(values , columns)
    return df 


def get_most_liked_post_per_day(day) : 
    statement = session.prepare("SELECT post_id, likes FROM likes_by_day WHERE day = ?")

    res = session.execute(statement ,[day])
    res = sorted(res, key=lambda x: x.likes, reverse=True)
    values = [] 
    columns = []
    
    for row in res[:5]: 
        values.append(row.likes) 
        columns.append(str(row.post_id))
    df = pd.DataFrame(values , columns)
    return df 

def get_most_active_users() : 
    res = session.execute("SELECT user_id , likes FROM user_likes")
    res = sorted(res, key=lambda x: x.likes, reverse=True)
    values = [] 
    columns = []
    for row in res[:5]: 
        values.append(row.likes) 
        columns.append(str(row.user_id))

    df = pd.DataFrame(values , columns)
    return df 


###############################################
#######     Getting Entities count   ##########
###############################################
def get_total_likes() : 
    res = session.execute("SELECT count(post_id) as count FROM demo.likes") 
    for row in res : 
        return row.count 

def get_active_users() : 
    res = session.execute("SELECT count(user_id) as count FROM demo.users") 
    for row in res : 
        return row.count 
    
def get_total_posts() : 
    res = session.execute("SELECT count(post_id) as count FROM demo.posts") 
    for row in res : 
        return row.count 