import pandas as pd
from ModelClasses import *

def readJSON(path):
    df_list = list()
    

def readCSV(path):
    df_list = list()
    D0 = pd.read_csv(path,header=0)

    #store JA_df
    filtered_df = D0.query("type == 'journal-article'")
    JA_df = filtered_df.drop(columns=['chapter',
       'publication_venue', 'venue_type', 'publisher', 'event'])
    #print(JA_df.columns)

    #store BC_df
    filtered_df = D0.query("type == 'book-chapter'")
    BC_df = filtered_df.drop(columns=['issue', 'volume',
       'publication_venue', 'venue_type', 'publisher', 'event'])
    #print(BC_df.head(3))
    #print(BC_df.columns)

    #store PP_df
    filtered_df = D0.query("type == 'proceedings-paper'")
    PP_df = filtered_df.drop(columns=['issue', 'volume','chapter','publication_venue', 'venue_type', 'publisher','event'])
    #print(PP_df.head(3))
    #print(PP_df.columns)

    #store VeB_DF
    filtered_df = D0.query("venue_type == 'book'")
    VeB_df = filtered_df.drop(columns=['title', 'type','publication_year', 'issue', 'volume', 'chapter','event'])

    #store VeJ_DF
    filtered_df = D0.query("venue_type == 'journal'")
    VeJ_df = filtered_df.drop(columns=['title', 'type','publication_year', 'issue', 'volume', 'chapter','event'])

    #store VePE_DF
    filtered_df = D0.query("venue_type == 'proceedings'")
    VePE_df = filtered_df.drop(columns=['title', 'type','publication_year', 'issue', 'volume', 'chapter'])

    return JA_df,BC_df,PP_df,VeB_df,VeJ_df,VePE_df
    

def check_repetedDOI():
    return True

def create_pubOBJ():
    # data for general publications
        #use the id, publicationYear, title, identifiers, publicationVenue, author, cites
    # use the author constructor
    return True

def create_jouartOBJ():
    return True

def create_authOBJ():
    return True

def create_venueOBJ():
    return True

def create_publisherOBJ():
    return True



#testing area
'''
df1,df2,df3,df4,df5,df6 = readCSV("rawData/relational_publications.csv")

print(df1.head(2))
print(df1.columns)
print('/n')
print(df2.head(2))
print(df2.columns)
print('/n')
print(df3.head(2))
print(df3.columns)
print('/n')
print(df4.head(2))
print(df4.columns)
print('/n')
print(df5.head(2))
print(df5.columns)
print('/n')
print(df6.head(2))
print(df6.columns)
'''