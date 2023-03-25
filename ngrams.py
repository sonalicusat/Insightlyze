#!/usr/bin/env python
# coding: utf-8

# In[9]:


import pandas as pd
import nltk
from nltk import ngrams
from nltk.corpus import stopwords
import sys
import csv
import pymysql
import getpass
from sqlalchemy import create_engine
import sys
import time

#Highly negative customer sentiment,Highly positive customer sentiment, Phone Referral, Chat Transfer, Custom Search, Order Confirmation, Sale Attempt

"""
client = "Kohls"
event_selection_arg = "Phone Referral" 
period_selection = "1 Week"
fuzzy_custom_text = None
"""

client = sys.argv[1]
print("client: ",client)
event_selection_arg = sys.argv[2]
print("event_selection_arg: ",event_selection_arg)
period_selection = sys.argv[3]
print("period_selection: ",period_selection)


fuzzy_custom_text = None
sender_selection = None


fuzzy_custom_text = sys.argv[4] if len(sys.argv) > 4 else None
sender_selection = sys.argv[5] if len(sys.argv) > 5 else None


#sender_selection = "Agent"


if event_selection_arg == "Highly Negative Customer Sentiment":
    event_selection = 'cust_senti_bu'
    senti_type = "HIGHLY NEGATIVE"
elif event_selection_arg == "Highly Positive Customer Sentiment":
    event_selection = 'cust_senti_bu'    
    senti_type = "HIGHLY POSITIVE"
elif event_selection_arg == "Order Confirmation":
    event_selection = 'order_placed'
elif event_selection_arg == "Phone Referral":
    event_selection ='referral'  
elif event_selection_arg =='Custom Search':
    event_selection ='fuzzy_match' 
elif event_selection_arg =='Sale Attempt':
    event_selection ='pitch'   
elif event_selection_arg =='Credit/ Adjustment':
    event_selection ='adjustment'        
else:    
    print("New event type found: ",event_selection_arg)
    sys.exit(1)




username = getpass.getuser()

if client in('ATT Mobility', 'Optus', 'Kohls', 'Best Buy Canada'):

    #------------------------------------------------------------------------
    #DB Connection
    if(username=="delivery"):
        #import decoding
        import base64
        file_pass = "xx"
        f = open(file_pass, "r")
        pass_word = f.read()
        pass_key = pass_word
        password = base64.b64decode(pass_key).decode("utf-8")
        user_id="xx"
    else:
        #user_id = input("Enter your DB UID: ")  
        #password = getpass.getpass(prompt='Enter your db password: ')
        user_id="xx"
        password = "xx"

    db = pymysql.connect(host='xx', user=user_id, passwd=password,local_infile=True)
    cursor = db.cursor()
elif client in('SiriusXM Canada','BJs','Best Buy US'):
    #------------------------------------------------------------------------
    #DB Connection
    if(username=="delivery"):
        #import decoding
        import base64
        file_pass = "xx"
        f = open(file_pass, "r")
        pass_word = f.read()
        pass_key = pass_word
        password = base64.b64decode(pass_key).decode("utf-8")
        user_id="xx"
    else:
        #user_id = input("Enter your DB UID: ")  
        #password = getpass.getpass(prompt='Enter your db password: ')
        user_id="xx"
        password = "xx"

    db = pymysql.connect(host='xx', user=user_id, passwd=password,local_infile=True)
    cursor = db.cursor()
else:
    print("New client name found: ",client)
#------------------------------------------------------------------------
print(db)
event_table_df=pd.read_sql("select client,db,events_table from tools_and_config.ngram_config where client = '"+client+"';",db) 
print(event_table_df.head())

db1 = event_table_df['db'][0]
events_table = event_table_df['events_table'][0]



try:
    events_dict={}
      
    df=pd.read_sql("(select table_type,`column_name`,column_variable from tools_and_config.tmt_column_config where db='"+db1+"'  AND `table_type` = 'events') UNION  (select table_type,`column_name`,column_variable from tools_and_config.tmt_default_column_config where  `table_type` = 'events' and column_variable NOT IN  (SELECT column_variable from tools_and_config.tmt_column_config where db='"+db1+"'  AND `table_type` = 'events'))",db)        

    for i in range(len(df)):
        if df['table_type'][i] == 'events':
            events_dict[df['column_variable'][i]] = df['column_name'][i]


except Exception as e : print(e)


period_df=pd.read_sql("select * from tools_and_config.ngram_periods where client = '"+client+"' and period = '"+period_selection+"';",db)   

date1 = str(period_df['date1'][0])
print(date1)
date2 = str(period_df['date2'][0])
job_date_val =  "between '"+date1+"' AND '"+date2+"'"
print(job_date_val)
"""
if period_selection == "Period 1":
    job_date_val =  "between '"+period_df[date1]+"' AND '"+period_df[date2]+"'"
elif period_selection == "Period 2":
    job_date_val =  "between DATE_SUB(DATE_SUB(CURDATE(), INTERVAL 1 DAY), INTERVAL 14 DAY) AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)"
elif period_selection == "Period 3":
    job_date_val =  "between DATE_SUB(DATE_SUB(CURDATE(), INTERVAL 1 DAY), INTERVAL 21 DAY) AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)"
    """

#job_date_val = "between '2023-01-25' AND '2023-01-31'"
#job_date_val = "between '2023-01-01' AND '2023-03-22'"


#---------------------------

def create_query(event_selection,min_max_val,gt_ls_val,pre_post_table_postfix):    

    query_str = "DROP TABLE if EXISTS test_sbg.hackthon_"+pre_post_table_postfix+";"
    print(query_str)
    cursor.execute(query_str)
    db.commit()
    
    query_str = "DROP TABLE if EXISTS test_sbg.hackthon_prepost_temp;"
    print(query_str)
    cursor.execute(query_str)
    db.commit()

    if event_selection != "cust_senti_bu":
        query_str = "CREATE TABLE test_sbg.hackthon_prepost_temp SELECT A." + events_dict['session_id'] + ", A." + events_dict['line_num'] + " AS event_line_num, "+min_max_val+"(B." + events_dict['line_num'] + ") AS output_line_num FROM "+events_table+" A, "+events_table+" B WHERE A." + events_dict['session_id'] + " = B." + events_dict['session_id'] + " and A."+events_dict['job_date'] +" "+ job_date_val+ " AND A." + events_dict['line_num'] + gt_ls_val + " B." + events_dict['line_num'] + " AND A."+event_selection+" IS NOT NULL AND B.line_by_num <> A.line_by_num AND B." +events_dict['line_by_num'] + " in(1, 2) GROUP BY 1, 2"

        print(query_str)
        cursor.execute(query_str)
        db.commit()
        
    else:
        query_str = "CREATE TABLE test_sbg.hackthon_prepost_temp SELECT A." + events_dict['session_id'] + ", A." + events_dict['line_num'] + " AS event_line_num, "+min_max_val+"(B." + events_dict['line_num'] + ") AS output_line_num FROM "+events_table+" A, "+events_table+" B WHERE A." + events_dict['session_id'] + " = B." + events_dict['session_id'] + " and A."+events_dict['job_date'] +" "+ job_date_val+ " AND A." + events_dict['line_num'] + gt_ls_val + " B." + events_dict['line_num'] + " AND A."+event_selection+" = '" +senti_type +"' AND B.line_by_num <> A.line_by_num AND B." +events_dict['line_by_num'] + " in(1, 2) GROUP BY 1, 2"

        print(query_str)
        cursor.execute(query_str)
        db.commit()

    
    query_str = "create table test_sbg.hackthon_"+pre_post_table_postfix+" SELECT D." + events_dict['session_id'] + ", event_line_num, output_line_num, D." + events_dict['line_text'] + " as event_text FROM  " +events_table+" D, test_sbg.hackthon_prepost_temp C WHERE C." + events_dict['session_id'] + " = D." + events_dict['session_id'] + " AND C.output_line_num = D." + events_dict['line_num'] + ";"
    print(query_str)
    cursor.execute(query_str)
    db.commit()    
    
    
    
def apply_calculation(df_phrase_table, total_lines):
    maxcount = df_phrase_table["count"].max()
    print(maxcount)
    print(total_lines)
    
    #ROUND((50/(maxcount/total_lines*100))*(X/total_lines)*100,0)
    df_phrase_table["new_count"] = df_phrase_table["count"].apply(lambda x: round((50/(maxcount/total_lines*100))*(x/total_lines)*100,0))
    df_phrase_table["share"] = df_phrase_table["count"].apply(lambda x: ((x/total_lines)))
    df_phrase_table.drop(df_phrase_table[df_phrase_table["share"] < ngram_feq_threshold].index, inplace=True)
    return df_phrase_table
    

def ngrams_cal(text_column,final_df):

    # Remove None values from text_column
    text_column = [text for text in text_column if text is not None]
    
    # Tokenize the text
    tokens = [nltk.word_tokenize(text) for text in text_column]

    # Remove special characters
    tokens = [[word for word in token if word.isalnum()] for token in tokens]

    # Remove stop words
    stop_words = set(stopwords.words('english'))
    tokens = [[word for word in token if not word.lower() in stop_words] for token in tokens]

    unique_tokens = [set(token) for token in tokens]

    # Create n-grams
    for n in range(1, 6):

        n_grams = [list(ngrams(token, int(n))) for token in unique_tokens]

        # Convert n-grams to strings
        n_grams = [[' '.join(gram) for gram in token] for token in n_grams]


        phrase_table = {}
        for doc_ngrams in n_grams:

            for ngram in doc_ngrams:
                phrase = ''.join(ngram)
                if phrase in phrase_table:
                    phrase_table[phrase] += 1
                else:
                    phrase_table[phrase] = 1

        # Convert phrase table to dataframe
        df_phrase_table = pd.DataFrame.from_dict(phrase_table, orient='index', columns=['count'])
        df_phrase_table.index.name = 'phrase'
        df_phrase_table.reset_index(inplace=True)

        # Sort phrase table by count
        df_phrase_table.sort_values(by='count', ascending=False, inplace=True)

        apply_calculation(df_phrase_table, total_lines)
        
        final_df = final_df.append(df_phrase_table, ignore_index=True)
        print(final_df.head())
    final_df.sort_values(by='new_count', ascending=False, inplace=True)
    return final_df
    

# Select text column
 
ngram_feq_threshold = 0.02    
#-----------------------------------------------------------
if event_selection != 'fuzzy_match':
    min_max_val = 'MIN'
    gt_ls_val = ' < '
    pre_post_table_postfix = 'post'


    create_query(event_selection,min_max_val,gt_ls_val,pre_post_table_postfix)

    ngrams_df = pd.DataFrame()
    ngrams_df['text'] = pd.read_sql("select lower(event_text) from test_sbg.hackthon_"+pre_post_table_postfix+";",db)
    text_column = ngrams_df['text'] 

    total_lines = len(ngrams_df)
        
    final_df = pd.DataFrame() 
    final_df = ngrams_cal(text_column,final_df)   
    final_df[['phrase','new_count']].to_csv("D:/project/Insightlyze/Insightlyze/static/wc_"+pre_post_table_postfix+".csv", header=False, index = False)

    #-----------------------------------------------------------
    min_max_val = 'MAX'
    gt_ls_val = ' > '
    pre_post_table_postfix = 'prior'
    create_query(event_selection,min_max_val,gt_ls_val,pre_post_table_postfix)

    ngrams_df = pd.DataFrame()
    ngrams_df['text'] = pd.read_sql("select lower(event_text) from test_sbg.hackthon_"+pre_post_table_postfix+";",db)
    text_column = ngrams_df['text'] 

    total_lines = len(ngrams_df)
        
    final_df = pd.DataFrame() 
    final_df = ngrams_cal(text_column,final_df)   
    final_df[['phrase','new_count']].to_csv("D:/project/Insightlyze/Insightlyze/static/wc_"+pre_post_table_postfix+".csv", header=False, index = False)


    #-----------------------------------------------------------

    ngrams_df = pd.DataFrame()

    if event_selection != "cust_senti_bu":
        ngrams_df['text']  = pd.read_sql("SELECT lower(A." + events_dict['line_text'] + ") FROM "+events_table+" A WHERE A."+ event_selection +" IS NOT NULL AND A."+events_dict['job_date'] +" "+ job_date_val+ ";", db)
    else:
        ngrams_df['text']  = pd.read_sql("SELECT lower(A." + events_dict['line_text'] + ") FROM "+events_table+" A WHERE A."+ event_selection +" = '" + senti_type + "' AND A."+events_dict['job_date'] +" "+ job_date_val+ ";", db)

    text_column = ngrams_df['text'] 

    total_lines = len(ngrams_df)

    final_df = pd.DataFrame() 
    final_df = ngrams_cal(text_column,final_df)   
    final_df[['phrase','new_count']].to_csv("D:/project/Insightlyze/Insightlyze/static/wc_event.csv", header=False, index = False) 

    #---------------histograms--------------------------------------------
    def histograms(event_selection):
        query_str = "DROP TABLE if EXISTS test_sbg.hackthon_histogram;"
        print(query_str)
        cursor.execute(query_str)
        db.commit()
        
        if event_selection != "cust_senti_bu":
            query_str = "CREATE TABLE test_sbg.hackthon_histogram SELECT B." + events_dict['session_id'] + ", round((B.HN_line_num / C.line_count) * 100, 0) AS Bucket FROM (SELECT A." + events_dict['session_id'] + ", COUNT(*) AS line_count FROM "+events_table+" A WHERE A."+events_dict['job_date'] +" "+ job_date_val+ " GROUP BY 1) C, (SELECT A. "+ events_dict['session_id'] + ", A. "+ events_dict['line_num'] + " AS HN_line_num FROM "+events_table+" A WHERE A."+event_selection+" IS NOT NULL AND A."+events_dict['job_date'] +" "+ job_date_val+ " GROUP BY 1, 2) B WHERE B." + events_dict['session_id'] + " = C." + events_dict['session_id'] + "; "

            print(query_str)
            cursor.execute(query_str)
            db.commit()
            
        else:
            query_str = "CREATE TABLE test_sbg.hackthon_histogram SELECT B." + events_dict['session_id'] + ", round((B.HN_line_num / C.line_count) * 100, 0) AS Bucket FROM (SELECT A." + events_dict['session_id'] + ", COUNT(*) AS line_count FROM "+events_table+" A WHERE A."+events_dict['job_date'] +" "+ job_date_val+ " GROUP BY 1) C, (SELECT A. "+ events_dict['session_id'] + ", A. "+ events_dict['line_num'] + " AS HN_line_num FROM "+events_table+" A WHERE A."+event_selection+" = '"+senti_type+"' AND A."+events_dict['job_date'] +" "+ job_date_val+ " GROUP BY 1, 2) B WHERE B." + events_dict['session_id'] + " = C." + events_dict['session_id'] + "; "

            print(query_str)
            cursor.execute(query_str)
            db.commit()
            
    histograms(event_selection)
    histogram_df = pd.read_sql("SELECT D.Bucket, if(C.Rec_Count IS NULL, 0, C.Rec_Count) AS Rec_Count FROM test_sbg.line_num_buckets D LEFT JOIN (SELECT B.bucket, COUNT(*) AS Rec_Count FROM test_sbg.hackthon_histogram A, test_sbg.line_num_buckets B WHERE A.Bucket >= B.`min` AND A.Bucket < B.`max` GROUP BY 1 ORDER BY 1) C ON D.bucket = C.bucket;;",db)

    histogram_df.to_csv("D:/project/Insightlyze/Insightlyze/static/histogram.csv", index = False) 

else:

    #---------------fuzzy--------------------------------------------
    import fuzzywuzzy
    from fuzzywuzzy import fuzz
    from fuzzywuzzy import process

    def fuzzy_match_agent_line(fuzzy_custom_text, db,db1, job_date_val,events_table):
        # Compute the number of days in the date range based on the period value
        #days = period * 7

        # Define the date range for the SQL query using the computed number of days
        #date_range = f"job_date BETWEEN DATE_SUB(DATE(NOW()), INTERVAL {days} DAY) AND DATE(NOW())"

        # Execute SQL query to get agent lines
        query = "SELECT A." + events_dict['line_text'] + " as line_text, A." + events_dict['session_id'] + " as session_id, A." + events_dict['line_num'] + " as line_num FROM "+events_table+" A WHERE A." + events_dict['line_by_num'] + " IN (" + str(sender_selection) + ") AND LENGTH(A." + events_dict['line_text'] + ") > 50 AND " + events_dict['job_date'] + " "+ job_date_val+";"
        print(query)
        agent_lines = pd.read_sql(query, con=db)

        # Iterate through agent lines and compute partial ratio score
        for agent_line in agent_lines.itertuples():
            #print(agent_line)
            score = fuzz.partial_ratio(fuzzy_custom_text, agent_line.line_text)
            if score > 80:
                #print(agent_line.engagementID, agent_line.line_num)
                
                query ="UPDATE "+events_table+" A SET A.fuzzy_match = "+str(score)+" WHERE A." + events_dict['session_id'] + " = '" + agent_line.session_id + "' AND A." + events_dict['line_num'] + " = " + str(agent_line.line_num)+";"

                print(query)
                cursor.execute(query)
                db.commit()
                return score, agent_line.session_id, agent_line.line_num
        
        # If no match found, return None
        return None


    # Define custom text
    #custom_text = 'Under federal privacy law, it is your right and our duty to protect your account information. May I use your information during this chat to discuss products offered by AT&T companies? Your decision will not affect your service.'
    #fuzzy_custom_text = 'SYSTEM: [You\'ve requested a secret from the customer]'

    if fuzzy_custom_text is None:
        print("fuzzy_custom_text is not provided")
    else:
        result = fuzzy_match_agent_line(fuzzy_custom_text, db,db1, job_date_val,events_table)
        print("fuzzy_custom_text is:", fuzzy_custom_text)

        min_max_val = 'MIN'
        gt_ls_val = ' < '
        pre_post_table_postfix = 'post'


        create_query(event_selection,min_max_val,gt_ls_val,pre_post_table_postfix)

        ngrams_df = pd.DataFrame()
        ngrams_df['text'] = pd.read_sql("select lower(event_text) from test_sbg.hackthon_"+pre_post_table_postfix+";",db)
        text_column = ngrams_df['text'] 

        total_lines = len(ngrams_df)
            
        final_df = pd.DataFrame() 
        final_df = ngrams_cal(text_column,final_df)   
        final_df[['phrase','new_count']].to_csv("D:/project/Insightlyze/Insightlyze/static/wc_"+pre_post_table_postfix+".csv", header=False, index = False)
    
        #-----------------------------------------------------------
        min_max_val = 'MAX'
        gt_ls_val = ' > '
        pre_post_table_postfix = 'prior'
        create_query(event_selection,min_max_val,gt_ls_val,pre_post_table_postfix)

        ngrams_df = pd.DataFrame()
        ngrams_df['text'] = pd.read_sql("select lower(event_text) from test_sbg.hackthon_"+pre_post_table_postfix+";",db)
        text_column = ngrams_df['text'] 

        total_lines = len(ngrams_df)
            
        final_df = pd.DataFrame() 
        final_df = ngrams_cal(text_column,final_df)   
        final_df[['phrase','new_count']].to_csv("D:/project/Insightlyze/Insightlyze/static/wc_"+pre_post_table_postfix+".csv", header=False, index = False)



        ngrams_df = pd.DataFrame()

        if event_selection == "fuzzy_match":

            ngrams_df['text']  = pd.read_sql("SELECT lower(A." + events_dict['line_text'] + ") FROM "+events_table+" A WHERE A."+ event_selection +" IS NOT NULL AND A."+events_dict['job_date'] +" "+ job_date_val+ ";", db)

            text_column = ngrams_df['text'] 

            total_lines = len(ngrams_df)

            final_df = pd.DataFrame() 
            final_df = ngrams_cal(text_column,final_df)   
            final_df[['phrase','new_count']].to_csv("D:/project/Insightlyze/Insightlyze/static/wc_event.csv", header=False, index = False) 
            #histogram
            
            query_str = "DROP TABLE if EXISTS test_sbg.hackthon_histogram;"
            print(query_str)
            cursor.execute(query_str)
            db.commit()
        
            query_str = "CREATE TABLE test_sbg.hackthon_histogram SELECT B." + events_dict['session_id'] + ", round((B.HN_line_num / C.line_count) * 100, 0) AS Bucket FROM (SELECT A." + events_dict['session_id'] + ", COUNT(*) AS line_count FROM "+events_table+" A WHERE A."+events_dict['job_date'] +" "+ job_date_val+ " GROUP BY 1) C, (SELECT A. "+ events_dict['session_id'] + ", A. "+ events_dict['line_num'] + " AS HN_line_num FROM "+events_table+" A WHERE A."+event_selection+" IS NOT NULL AND A."+events_dict['job_date'] +" "+ job_date_val+ " GROUP BY 1, 2) B WHERE B." + events_dict['session_id'] + " = C." + events_dict['session_id'] + "; "
            print(query_str)
            cursor.execute(query_str)
            db.commit()      
            

            histogram_df = pd.read_sql("SELECT D.Bucket, if(C.Rec_Count IS NULL, 0, C.Rec_Count) AS Rec_Count FROM test_sbg.line_num_buckets D LEFT JOIN (SELECT B.bucket, COUNT(*) AS Rec_Count FROM test_sbg.hackthon_histogram A, test_sbg.line_num_buckets B WHERE A.Bucket >= B.`min` AND A.Bucket < B.`max` GROUP BY 1 ORDER BY 1) C ON D.bucket = C.bucket;;",db)

            histogram_df.to_csv("D:/project/Insightlyze/Insightlyze/static/histogram.csv", index = False)