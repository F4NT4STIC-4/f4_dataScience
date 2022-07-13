import imp
from itertools import count
import sqlite3 as sql3
from unittest import result
from logging import raiseExceptions
from wsgiref.validate import IteratorWrapper
import pandas as pd
import json
from json import load
from ast import Pass
from asyncio.windows_events import NULL
from cmath import nan
from multiprocessing import dummy
from typing import Final

from pandas import Series, DataFrame, merge, read_csv
from rdflib import URIRef, Graph, Literal, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get

# ===== DEFINITION OF OUR CLASS OBJECTS
class IdentifiableEntity(object):
    def __init__(self, ids):
        self.id = set()
        for item in ids:
            self.id.add(item)

    def getIds(self):
        return list(self.id)

class Person(IdentifiableEntity):
    def __init__(self, ids, givenName, familyName):
        self.givenName = givenName
        self.familyName = familyName
        super().__init__(ids)
    
    def getGivenName(self):
        return self.givenName
    
    def getFamilyName(self):
        return self.familyName

class Organisation(IdentifiableEntity):
    def __init__(self, ids, name):
        self.name = name
        super().__init__(ids)
    
    def getName(self):
        return self.name

class Venue(IdentifiableEntity):
    def __init__(self, ids, title, publisher):
        self.title = title
        self.publisher = publisher
        super().__init__(ids)
    
    def getTitle(self):
        return self.title
    
    def getPublisher(self):
        return self.publisher

class Publication(IdentifiableEntity):
    def __init__(self, ids, publicationYear, title, authors, publicationVenue, pcites):
        self.publicationYear = publicationYear
        self.title = title
        self.author = set()
        for aut in authors:
            self.author.add(aut)
        self.publicationVenue = publicationVenue
        self.cites = set()
        for cit in pcites:
            self.cites.add(cit)
        super().__init__(ids)
    
    def getPublicationYear(self):
        return self.publicationYear
    
    def getTitle(self):
        return self.title
    
    def getCitedPublications(self):
        return list(self.cites)
    
    def getPublicationVenue(self):
        return self.publicationVenue
    
    def getAuthors(self):
        return self.author

class JournalArticle(Publication):
    def __init__(self, ids, publicationYear, title, authors, publicationVenue, pcites, issue, volume):
        self.issue = issue
        self.volume = volume
        super().__init__(ids, publicationYear, title, authors, publicationVenue, pcites)
    
    def getIssue(self):
        return self.issue
    
    def getVolume(self):
        return self.volume

class BookChapter(Publication):
    def __init__(self, ids, publicationYear, title, authors, publicationVenue, pcites, chapterNumber):
        self.chapterNumber = chapterNumber
        super().__init__(ids, publicationYear, title, authors, publicationVenue, pcites)
    
    def getChapterNumber(self):
        return self.chapterNumber

class ProceedingsPaper(Publication):
    pass

class Journal(Venue):
    pass

class Book(Venue):
    pass

class Proceedings(Venue):
    def __init__(self, ids, title, publisher, event):
        self.event = event
        super().__init__(ids, title, publisher)
    
    def getEvent(self):
        return self.event

# ===== QUERYPROCESSOR DEFINITION SUPERCLASS OF THE RELATIONALQUERYPROCESSOR AND THE TRIPLESTOREQUERYPROCESSOR
class QueryProcessor(object):
    def __init__(self):
        pass

# ===== THE MEMORY FUNCTIONS FOR THE PUBLICATION OBJECT CREATION
mem = {} # all the dicts(objs-variable) of each publication
doi_mem = [] #for check for not creating circular loops  #maybe global
             #mem will contain all the values for creating a pub obj
tutti_author = {}
def authorslist(filepath):    #call this function with upload of json
    if filepath.endswith(".json"):
        with open(filepath, "r", encoding="utf-8") as file:
            jsondata = json.load(file)
        #print(jsondata['authors'])
        tutti_author.update(jsondata['authors'])
        #print(tutti_author)
    pass

# IMPORTANT !!! --->> add the triplequery query for the df of triplestore database
#concatinating all dfs into one big one
def df_creator(z):
    sumDF = pd.DataFrame()
    for key in tutti_author:
        singDF1 = z.getPublicationsFromDOI([key])
        #singDF2 = c.getPublicationsFromDOI([key])  #this function is written but the query takes too long and probably goes on infinitely
        
        #sumDF_temp = pd.concat([singDF1,singDF2])
        sumDF = pd.concat([singDF1,sumDF])
        #print("This query takes time, please wait with a hot cup of coffee")
    createPubobjsdict(sumDF)
    return 


def createPubobjsdict(df):     # This DF contains all our publication data for one all our databases.
    for idx, item in df.iterrows():
        if item['id'] in mem:  #checks if we have alreay dealt with the the DOI in the df argument
            pass
        else:
            obj = {
                    "doi": [item['id']],
                    "title": item['title'],
                    "year":item['publication_year'],
                    "issue":item['issue'],
                    "volume":item['volume']
            }
            # ADDING AUTH LIST CREATED IN THE AUTHORLIST FUNCTION
            if item['id'] in tutti_author:    #check key
                auth_list = tutti_author[item['id']]  # json might have incomplete data
                obj.update({"authors":auth_list}) #adding dict of authors for this obj
            else:
                auth_list = {
                            "family": "No Data available",
                            "given": "No Data available",
                            "orcid": "No Data available"}
                obj.update({"authors":auth_list}) #adding dict of authors for this obj

            # CREATING AND ADDING VENUE AND PUBLISHER OBJECT
            if item['publication_venue'] == NULL:   #check syntax
                empt_publisher = {"id":"none//data nt available","title":"none//data nt available"}
                empt_venue = {"id":"none/data nt available", "title":"none/data nt available","publisher":empt_publisher}
                obj.update({'venue': empt_venue })
            else:
                org_data = {"id":[item["publisher"]],"title":item["name_pub"]}
                ven_data = {"id":[item['issn_isbn']],"title": item['publication_venue'], "publisher": org_data}
                obj.update({"venue":ven_data})
            mem.update({item['id']:obj})   #here we add the incomple obj to the mem dict
            
            #creating cites list
            cites_list = []
            cites_list.append(item['ref_doi'])
            obj.update({'cites': cites_list })

citlist = set()
def MANUalrecursion(doilist):
    
    for item in doilist:
        if item == None:
            Noauth = Person("NO-Orcid","NO-Givenname","No-Familyname")
            autlist = set()
            autlist.add(Noauth)
            NoVenue = Venue("NO-VenueId","NO-VenueTitle",Organisation("NO-OrganisationID","NO-OrganisationTitle"))
            ender = Publication("NO-PubID","NO-PubYear","NO-Title",autlist,NoVenue,set())
            citlist.add(ender)
            pass
        else:
            citlist.add(creatPubobj(item))
    #print("Test String to see how the loop works")
    #print(citlist)
    return citlist

def creatPubobj(DOI):   #creates class objects 
    Final_obj = {}
    Final_obj.update(mem[DOI])
    #print(Final_obj)
    #now we use the Final_obj dict to create our class object
    #first we create dependency objects like person object, venue object, etc
    # 1 - author object by iterating over the dict in the Final_obj dict
    aut = []
    for item in Final_obj['authors']:
        person = Person(item['orcid'],item['given'],item['family'])
        aut.append(person)
    #print(Final_obj['venue']['id'])
    if Final_obj['venue']['id'] == None:
        pass
    else:
        orgdata = {}
        orgdata.update(Final_obj['venue']['publisher'])
        publisherobj = Organisation(orgdata['id'],orgdata['title'])
        venueobj = Venue(Final_obj['venue']['id'],Final_obj['venue']['title'],publisherobj)
    
    if Final_obj['cites'] == None:
        pub_obj = Publication(Final_obj['doi'],Final_obj['year'],Final_obj['title'],aut,venueobj,set())
    else:
        pub_obj = Publication(Final_obj['doi'],Final_obj['year'],Final_obj['title'],aut,venueobj,MANUalrecursion(Final_obj['cites']))
    #print(pub_obj)
    
    return pub_obj     

def creatJAobj(DOI):   #creates JA class objects 
    Final_obj = {}
    Final_obj.update(mem[DOI])
    #print(Final_obj)
    #now we use the Final_obj dict to create our class object
    #first we create dependency objects like person object, venue object, etc
    # 1 - author object by iterating over the dict in the Final_obj dict
    aut = []
    for item in Final_obj['authors']:
        person = Person(item['orcid'],item['given'],item['family'])
        aut.append(person)
    #print(Final_obj['venue']['id'])
    if Final_obj['venue']['id'] == None:
        pass
    else:
        orgdata = {}
        orgdata.update(Final_obj['venue']['publisher'])
        publisherobj = Organisation(orgdata['id'],orgdata['title'])
        venueobj = Venue(Final_obj['venue']['id'],Final_obj['venue']['title'],publisherobj)
    
    if Final_obj['cites'] == None:
        ja_obj = JournalArticle(Final_obj['doi'],Final_obj['year'],Final_obj['title'],aut,venueobj,set(),Final_obj['issue'],Final_obj['volume'])
    else:
        ja_obj = JournalArticle(Final_obj['doi'],Final_obj['year'],Final_obj['title'],aut,venueobj,MANUalrecursion(Final_obj['cites']),Final_obj['issue'],Final_obj['volume'])
    #print(pub_obj)
    
    return ja_obj     

# ===== THE RELATIONALQUERYPROCESSOR FUNCTIONS AND CLASSES
class RelationalProcessor(object):
    def __init__(self):
        self.dbPath = ""
    
    def getDbPath(self):
        return self.dbPath
    
    def setDbPath(self,path):
        if type(path)==str:
            self.dbPath = path
            return True
        else:
            return False

class RelationalDataProcessor(RelationalProcessor):
    def __init__(self):
        super().__init__()
    
    def uploadData(self,filepath):
        if type(filepath) != str:
            return False
        else:
            # =============== CSV UPLOAD DATA ===============
            if filepath.endswith(".csv"):
                df_publications = pd.read_csv(filepath,na_filter=False)

                # =============== PUBLICATION DATAFRAMES ===============

                journal_article_df = pd.DataFrame({
                    "issue": pd.Series(dtype="str"),
                    "volume": pd.Series(dtype="str"),
                    "publication_year": pd.Series(dtype="int"),
                    "title": pd.Series(dtype="str"),
                    "publication_venue": pd.Series(dtype="str"),
                    "id": pd.Series(dtype="str")
                })

                book_chapter_df = pd.DataFrame({
                    "chapter_number": pd.Series(dtype="str"),
                    "publication_year": pd.Series(dtype="int"),
                    "title": pd.Series(dtype="str"),
                    "publication_venue": pd.Series(dtype="str"),
                    "id": pd.Series(dtype="str")})

                proceeding_paper_df = pd.DataFrame({
                    "publication_year": pd.Series(dtype="int"),
                    "title": pd.Series(dtype="str"),
                    "publication_venue": pd.Series(dtype="str"),
                    "id": pd.Series(dtype="str")
                })

                journal_article_df['issue'] = df_publications[df_publications['type']== "journal-article"]['issue'].astype('str')
                journal_article_df['volume'] = df_publications[df_publications['type']== "journal-article"]['volume'].astype('str')
                journal_article_df['publication_year'] = df_publications[df_publications['type']== "journal-article"]['publication_year'].astype('int')
                journal_article_df['title'] = df_publications[df_publications['type']== "journal-article"]['title'].astype('str')
                journal_article_df['publication_venue'] = df_publications[df_publications['type']== "journal-article"]['publication_venue'].astype('str')
                journal_article_df['id'] = df_publications[df_publications['type']== "journal-article"]['id'].astype('str')
                journal_article_df.replace(to_replace="nan",value="")

                book_chapter_df['publication_year'] = df_publications[df_publications['type']== "book-chapter"]['publication_year'].astype('int')
                book_chapter_df['title'] = df_publications[df_publications['type']== "book-chapter"]['title'].astype('str')
                book_chapter_df['chapter_number'] = df_publications[df_publications['type']== "book-chapter"]['chapter'].astype('str')
                book_chapter_df['publication_venue'] = df_publications[df_publications['type']== "book-chapter"]['publication_venue'].astype('str')
                book_chapter_df['id'] = df_publications[df_publications['type']== "book-chapter"]['id'].astype('str')
                book_chapter_df.replace(to_replace="nan",value="")

                proceeding_paper_df['publication_year'] = df_publications[df_publications['type']== "proceedings-paper"]['publication_year'].astype('int')
                proceeding_paper_df['title'] = df_publications[df_publications['type']== "proceedings-paper"]['title'].astype('str')
                proceeding_paper_df['publication_venue'] = df_publications[df_publications['type']== "proceedings-paper"]['publication_venue'].astype('str')
                proceeding_paper_df['id'] = df_publications[df_publications['type'] == "proceedings-paper"]['id'].astype('str')
                proceeding_paper_df.replace(to_replace="nan",value="")

                # =============== VENUES DATAFRAMES ===============

                journal_df = pd.DataFrame({
                    "name_venue": pd.Series(dtype="str"),
                    "publisher": pd.Series(dtype="str"),
                    "id_venue": pd.Series(dtype="str")})

                book_df = pd.DataFrame({
                    "name_venue": pd.Series(dtype="str"),
                    "publisher": pd.Series(dtype="str"),
                    "id_venue": pd.Series(dtype="str")})

                proceedings_df = pd.DataFrame({"event": pd.Series(dtype="str"),
                                               "name_venue": pd.Series(dtype="str"),
                                               "publisher": pd.Series(dtype="str"),
                                               "id_venue": pd.Series(dtype="str")})

                journal_df['name_venue'] = df_publications[df_publications['venue_type'] == "journal"]['publication_venue'].astype('str')
                journal_df['publisher'] = df_publications[df_publications['venue_type'] == "journal"]['publisher'].astype('str')
                journal_df['id_venue'] = df_publications[df_publications['venue_type'] == "journal"]['id'].astype('str')
                journal_df.replace(to_replace="nan",value="")

                book_df['name_venue'] = df_publications[df_publications['venue_type'] == "book"]['publication_venue'].astype('str')
                book_df['publisher'] = df_publications[df_publications['venue_type'] == "book"]['publisher'].astype('str')
                book_df['id_venue'] = df_publications[df_publications['venue_type'] == "book"]['id'].astype('str')
                book_df.replace(to_replace="nan",value="")

                proceedings_df['event'] = df_publications[df_publications['venue_type'] == "proceedings"]['event'].astype('str')
                proceedings_df['name_venue'] = df_publications[df_publications['venue_type'] == "proceedings"]['publication_venue'].astype('str')
                proceedings_df['publisher'] = df_publications[df_publications['venue_type'] == "proceedings"]['publisher'].astype('str')
                proceedings_df['id_venue'] = df_publications[df_publications['venue_type'] == "proceedings"]['id'].astype('str')
                proceedings_df.replace(to_replace="nan",value="")

                # =============== DATABASE CONNECTION ===============

                with sql3.connect(self.dbPath) as rdb:
                    journal_article_df.to_sql("JournalArticleTable", rdb, if_exists="append", index=False)
                    book_chapter_df.to_sql("BookChapterTable", rdb, if_exists="append", index=False)
                    proceeding_paper_df.to_sql("ProceedingsPaperTable", rdb, if_exists="append", index=False)
                    journal_df.to_sql("JournalTable", rdb, if_exists="append", index=False)
                    book_df.to_sql("BookTable", rdb, if_exists="append", index=False)
                    proceedings_df.to_sql("ProceedingsTable", rdb, if_exists="append", index=False)
                    rdb.commit()
            
            # =============== JSON UPLOAD DATA ===============

            elif filepath.endswith(".json"):
                authorslist(filepath)
                with open(filepath, "r", encoding="utf-8") as file:
                    jsondata = json.load(file)
                    
                    # =============== AUTHORS DATAFRAME ===============
                    authors_df = pd.DataFrame({
                        "doi_authors": pd.Series(dtype="str"),
                        "family": pd.Series(dtype="str"),
                        "given": pd.Series(dtype="str"),
                        "orcid": pd.Series(dtype="str")
                    })

                    family = []
                    given = []
                    orcid = []
                    doi_authors = []

                    authors = jsondata['authors']
                    for key in authors:
                        for value in authors[key]:
                            doi_authors.append(key)
                            family.append(value['family'])
                            given.append(value['given'])
                            orcid.append(value['orcid'])

                    authors_df['doi_authors'] = doi_authors
                    authors_df['family'] = family
                    authors_df['given'] = given
                    authors_df['orcid'] = orcid

                    # =============== VENUES DATAFRAME ===============

                    venues_id_df = pd.DataFrame({
                        "doi_venues_id": pd.Series(dtype="str"),
                        "issn_isbn": pd.Series(dtype="str"),
                    })

                    doi_venues_id = []
                    issn_isbn = []

                    venues_id = jsondata["venues_id"]
                    for key in venues_id:
                        for value in venues_id[key]:
                            doi_venues_id.append(key)
                            issn_isbn.append(value)

                    venues_id_df["doi_venues_id"] = doi_venues_id
                    venues_id_df["issn_isbn"] = pd.Series(issn_isbn)

                    # =============== REFERENCES DATAFRAME ===============

                    references_df = pd.DataFrame({
                        "og_doi": pd.Series(dtype="str"),
                        "ref_doi": pd.Series(dtype="str"),
                    })

                    og_doi = []
                    ref_doi = []

                    references = jsondata["references"]
                    for key in references:
                        for value in references[key]:
                            og_doi.append(key)
                            ref_doi.append(value)

                    references_df["og_doi"] = pd.Series(og_doi)
                    references_df["ref_doi"] = pd.Series(ref_doi)

                    # =============== PUBLISHER DATAFRAME ===============

                    publishers_df = pd.DataFrame({
                        "crossref": pd.Series(dtype="str"),
                        "id_crossref": pd.Series(dtype="str"),
                        "name_pub": pd.Series(dtype="str")
                    })

                    crossref = []
                    id_crossref = []
                    name_pub = []

                    publishers = jsondata["publishers"]
                    for key in publishers:
                        crossref.append(key)
                        id_crossref.append(publishers[key]["id"])
                        name_pub.append(publishers[key]["name"])

                    publishers_df["crossref"] = pd.Series(crossref)
                    publishers_df["id_crossref"] = pd.Series(id_crossref)
                    publishers_df["name_pub"] = pd.Series(name_pub)
                
                # =============== DATABASE CONNECTION ===============

                with sql3.connect(self.dbPath) as rdb:
                    authors_df.to_sql("AuthorsTable", rdb, if_exists="append", index=False)
                    venues_id_df.to_sql("VenuesIdTable", rdb, if_exists="append", index=False)
                    references_df.to_sql("ReferencesTable", rdb, if_exists="append", index=False)
                    publishers_df.to_sql("PublishersTable", rdb, if_exists="append", index=False)
                    rdb.commit()
            return True

class RelationalQueryProcessor(QueryProcessor,RelationalProcessor): 
    def __init__(self):
        super().__init__()
    
    def getPublicationsPublishedInYear(self,year):
        if type(year) == int:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT publication_year, title, publication_venue, id, issue, volume, NULL AS chapter_number, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalTable.id_venue==VenuesIdTable.doi_venues_id WHERE publication_year='{pub_year}' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, chapter_number, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub FROM BookChapterTable LEFT JOIN AuthorsTable ON BookChapterTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN BookTable ON ReferencesTable.og_doi==BookTable.id_venue LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON BookTable.id_venue==VenuesIdTable.doi_venues_id WHERE publication_year='{pub_year}' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, NULL AS chapter_number, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub FROM ProceedingsPaperTable LEFT JOIN AuthorsTable ON ProceedingsPaperTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN ProceedingsTable ON ReferencesTable.og_doi==ProceedingsTable.id_venue LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON ProceedingsTable.id_venue==VenuesIdTable.doi_venues_id WHERE publication_year='{pub_year}'".format(pub_year=year)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "chapter_number", "family", "given", "orcid", "ref_doi", "name_venue", "issn_isbn", "publisher", "name_pub"])
        else:
            raiseExceptions("The input parameter is not an integer!")
        
    def getPublicationsByAuthorId(self,id):
        if type(id) == str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT publication_year, title, publication_venue, id, issue, volume, NULL AS chapter_number, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalArticleTable.id==VenuesIdTable.doi_venues_id WHERE orcid='{orcid}' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, chapter_number, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub FROM BookChapterTable LEFT JOIN AuthorsTable ON BookChapterTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN BookTable ON ReferencesTable.og_doi==BookTable.id_venue LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON BookChapterTable.id==VenuesIdTable.doi_venues_id WHERE orcid='{orcid}' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, NULL AS chapter_number, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub FROM ProceedingsPaperTable LEFT JOIN AuthorsTable ON ProceedingsPaperTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN ProceedingsTable ON ReferencesTable.og_doi==ProceedingsTable.id_venue LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON ProceedingsPaperTable.id==VenuesIdTable.doi_venues_id WHERE orcid='{orcid}'".format(orcid=id)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "chapter_number", "family", "given", "orcid", "ref_doi", "name_venue", "issn_isbn", "publisher", "name_pub"])
        else:
            raiseExceptions("The input parameter is not a string!")

    def getMostCitedPublication(self):
        with sql3.connect(self.getDbPath()) as qrdb:
            cur = qrdb.cursor()
            query1 = "SELECT ref_doi, COUNT(ref_doi) AS num_citations FROM ReferencesTable GROUP BY ref_doi ORDER BY num_citations DESC"
            cur.execute(query1)
            result_q1 = cur.fetchall()
            max = result_q1[0][1]
            result1 = list()
            for item in result_q1:
                index = result_q1.index(item)
                if result_q1[index][1] == max:
                    tpl = tuple((result_q1[index][0],max))
                    result1.append(tpl)
            df1 = pd.DataFrame(data=result1,columns=["ref_doi","num_citations"])
            query2 = "SELECT publication_year, title, publication_venue, id, issue, volume, NULL AS chapter_number, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalArticleTable.id==VenuesIdTable.doi_venues_id UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, chapter_number, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub FROM BookChapterTable LEFT JOIN AuthorsTable ON BookChapterTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN BookTable ON ReferencesTable.og_doi==BookTable.id_venue LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON BookChapterTable.id==VenuesIdTable.doi_venues_id UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, NULL AS chapter_number, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub FROM ProceedingsPaperTable LEFT JOIN AuthorsTable ON ProceedingsPaperTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN ProceedingsTable ON ReferencesTable.og_doi==ProceedingsTable.id_venue LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON ProceedingsPaperTable.id==VenuesIdTable.doi_venues_id"
            cur.execute(query2)
            result_q2 = cur.fetchall()
            df2 = pd.DataFrame(data=result_q2,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "chapter_number", "family", "given", "orcid", "ref_doi", "name_venue", "issn_isbn", "publisher", "name_pub"])
            final_result = pd.merge(left=df2, right=df1, left_on="id", right_on="ref_doi")
            qrdb.commit()
        return final_result
    
    def getMostCitedVenue(self):
        with sql3.connect(self.getDbPath()) as qrdb:
            cur = qrdb.cursor()
            query1 = "SELECT name_venue, COUNT(name_venue) as num_cit FROM JournalTable LEFT JOIN ReferencesTable ON JournalTable.id_venue==ReferencesTable.ref_doi WHERE ref_doi IS NOT NULL GROUP BY name_venue UNION SELECT name_venue, COUNT(name_venue) as num_cit FROM BookTable LEFT JOIN ReferencesTable ON BookTable.id_venue==ReferencesTable.ref_doi WHERE ref_doi IS NOT NULL GROUP BY name_venue UNION SELECT name_venue, COUNT(name_venue) as num_cit FROM ProceedingsTable LEFT JOIN ReferencesTable ON ProceedingsTable.id_venue==ReferencesTable.ref_doi WHERE ref_doi IS NOT NULL GROUP BY name_venue ORDER BY num_cit DESC"
            cur.execute(query1)
            result_q1 = cur.fetchall()
            max = result_q1[0][1]
            result1 = list()
            for item in result_q1:
                index = result_q1.index(item)
                if result_q1[index][1] == max:
                    tpl = tuple((result_q1[index][0],max))
                    result1.append(tpl)
            df1 = pd.DataFrame(data=result1,columns=["name_venue","num_cit"])
            query2 = "SELECT issn_isbn, NULL AS event, name_venue, publisher, name_pub FROM VenuesIdTable LEFT JOIN JournalTable ON VenuesIdTable.doi_venues_id==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref UNION SELECT issn_isbn, NULL AS event, name_venue, publisher, name_pub FROM VenuesIdTable LEFT JOIN BookTable ON VenuesIdTable.doi_venues_id==BookTable.id_venue LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref UNION SELECT issn_isbn, event, name_venue, publisher, name_pub FROM VenuesIdTable LEFT JOIN ProceedingsTable ON VenuesIdTable.doi_venues_id==ProceedingsTable.id_venue LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref"
            cur.execute(query2)
            result_q2 = cur.fetchall()
            df2 = pd.DataFrame(data=result_q2, columns=["issn_isbn", "event", "name_venue", "publisher", "name_pub"])
            final_result = pd.merge(left=df2, right=df1, left_on="name_venue", right_on="name_venue")
            qrdb.commit()
        return final_result

    def getVenuesByPublisherId(self,id):
        if type(id) == str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT doi_venues_id, issn_isbn, NULL AS event, name_venue, publisher, name_pub FROM VenuesIdTable LEFT JOIN JournalTable ON VenuesIdTable.doi_venues_id==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref WHERE publisher='{pub_id}' UNION SELECT doi_venues_id, issn_isbn, NULL AS event, name_venue, publisher, name_pub FROM VenuesIdTable LEFT JOIN BookTable ON VenuesIdTable.doi_venues_id==BookTable.id_venue LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref WHERE publisher='{pub_id}' UNION SELECT doi_venues_id, issn_isbn, event, name_venue, publisher, name_pub FROM VenuesIdTable LEFT JOIN ProceedingsTable ON VenuesIdTable.doi_venues_id==ProceedingsTable.id_venue LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref WHERE publisher='{pub_id}'".format(pub_id=id)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result, columns=["doi_venues_id", "issn_isbn", "event", "name_venue", "publisher", "name_pub"])
        else:
            raiseExceptions("The input parameter is not string!")

    def getPublicationInVenue(self,venueId):
        if type(venueId)==str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT publication_year, title, publication_venue, id, issue, volume, NULL AS chapter_number, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub, issn_isbn FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalArticleTable.id==VenuesIdTable.doi_venues_id WHERE issn_isbn='{venue_id}' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, chapter_number, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub, issn_isbn FROM BookChapterTable LEFT JOIN AuthorsTable ON BookChapterTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN BookTable ON ReferencesTable.og_doi==BookTable.id_venue LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON BookChapterTable.id==VenuesIdTable.doi_venues_id WHERE issn_isbn='{venue_id}' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, NULL AS chapter_number, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub, issn_isbn FROM ProceedingsPaperTable LEFT JOIN AuthorsTable ON ProceedingsPaperTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN ProceedingsTable ON ReferencesTable.og_doi==ProceedingsTable.id_venue LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON ProceedingsPaperTable.id==VenuesIdTable.doi_venues_id WHERE issn_isbn='{venue_id}'".format(venue_id=venueId)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "chapter_number", "family", "given", "orcid", "ref_doi", "name_venue", "issn_isbn", "publisher", "name_pub", "issn_isbn"])        
        else:
            raiseExceptions("The input parameter is not a string!")

    def getJournalArticlesInIssue(self,issue,volume,journalId):
        if type(issue)==str and type(volume)==str and type(journalId)==str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT publication_year, title, publication_venue, id, issue, volume, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalArticleTable.id==VenuesIdTable.doi_venues_id WHERE issue='{issue}' AND volume='{volume}' AND issn_isbn='{journal_id}'".format(issue=issue, volume=volume, journal_id=journalId)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "family", "given", "orcid", "ref_doi", "name_venue", "issn_isbn", "publisher", "name_pub"]) 
        else:
            raiseExceptions("One or all the input parameter are not strings!")

    def getJournalArticlesInVolume(self,volume,journalId):
        if type(volume)==str and type(journalId)==str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT publication_year, title, publication_venue, id, issue, volume, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalArticleTable.id==VenuesIdTable.doi_venues_id WHERE volume='{volume}' AND issn_isbn='{journal_id}'".format(volume=volume, journal_id=journalId)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "family", "given", "orcid", "ref_doi", "name_venue", "issn_isbn", "publisher", "name_pub"]) 
        else:
            raiseExceptions("One or all the input parameter are not strings!")

    def getJournalArticlesInJournal(self,journalId):
        if type(journalId)==str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT publication_year, title, publication_venue, id, issue, volume, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalArticleTable.id==VenuesIdTable.doi_venues_id WHERE issn_isbn='{journal_id}'".format(journal_id=journalId)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "family", "given", "orcid", "ref_doi", "name_venue", "issn_isbn", "publisher", "name_pub"]) 
        else:
            raiseExceptions("One or all the input parameter are not strings!")

    def getProceedingsByEvent(self,eventPartialName):
        if type(eventPartialName)==str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT event, name_venue, publisher, issn_isbn, name_pub FROM ProceedingsTable LEFT JOIN VenuesIdTable ON ProceedingsTable.id_venue==VenuesIdTable.doi_venues_id LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref WHERE event LIKE '%{event}%'".format(event=eventPartialName)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["event","name_venue","publisher","issn_isbn", "name_pub"])
        else:
            raiseExceptions("The input parameter is not string!")

    def getPublicationAuthors(self,publicationId):
        if type(publicationId)==str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT doi_authors, family, given, orcid FROM AuthorsTable WHERE doi_authors='{pub_doi}'".format(pub_doi=publicationId)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["doi_authors", "family", "given", "orcid"])
        else:
            raiseExceptions("The input parameter is not string!")

    def getPublicationsByAuthorName(self,authorPartialName):
        if type(authorPartialName)==str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT publication_year, title, publication_venue, id, issue, volume, NULL AS chapter_number, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalArticleTable.id==VenuesIdTable.doi_venues_id WHERE family LIKE '%{family}%' OR given LIKE '%{given}%' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, chapter_number, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub FROM BookChapterTable LEFT JOIN AuthorsTable ON BookChapterTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN BookTable ON ReferencesTable.og_doi==BookTable.id_venue LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON BookChapterTable.id==VenuesIdTable.doi_venues_id WHERE family LIKE '%{family}%' OR given LIKE '%{given}%' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, NULL AS chapter_number, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub FROM ProceedingsPaperTable LEFT JOIN AuthorsTable ON ProceedingsPaperTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN ProceedingsTable ON ReferencesTable.og_doi==ProceedingsTable.id_venue LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON ProceedingsPaperTable.id==VenuesIdTable.doi_venues_id WHERE family LIKE '%{family}%' OR given LIKE '%{given}%'".format(family=authorPartialName,given=authorPartialName)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "chapter_number", "family", "given", "orcid", "ref_doi", "name_venue", "issn_isbn", "publisher", "name_pub"]) 
        else:
            raiseExceptions("One or all the input parameter are not strings!")
            
    def getDistinctPublishersOfPublications(self,pubIdList):
        if type(pubIdList) == list and all(isinstance(n, str) for n in pubIdList):
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                result = list()
                for item in pubIdList:
                    query = "SELECT name_venue, id_venue, crossref, name_pub FROM JournalTable LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref WHERE id_venue='{doi}' UNION SELECT name_venue, id_venue, crossref, name_pub FROM BookTable LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref WHERE id_venue='{doi}' UNION SELECT name_venue, id_venue, crossref, name_pub FROM ProceedingsTable LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref WHERE id_venue='{doi}'".format(doi = item)
                    cur.execute(query)
                    result_q = cur.fetchall()
                    result.extend(result_q)
                qrdb.commit()
            return pd.DataFrame(data=result, columns=["name_venue", "id_venue", "crossref", "name_pub"])
        else:
            raiseExceptions("The input parameter is not a list or it is not a list of strings!")


    # ===== ADDITIONAL GET-METHOD FOR HANDLING THE PUBLICATION OBJECTS ===== 
    def getPublicationsFromDOI(self,pubIdList):
            if type(pubIdList) == list and all(isinstance(n, str) for n in pubIdList):
                with sql3.connect(self.getDbPath()) as qrdb:
                    cur = qrdb.cursor()
                    result = list()
                    for item in pubIdList:
                        query = "SELECT publication_year, title, publication_venue, id, issue, volume, NULL AS chapter_number, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalTable.id_venue==VenuesIdTable.doi_venues_id WHERE id = '{doi}' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, chapter_number, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub FROM BookChapterTable LEFT JOIN AuthorsTable ON BookChapterTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN BookTable ON ReferencesTable.og_doi==BookTable.id_venue LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON BookTable.id_venue==VenuesIdTable.doi_venues_id WHERE id = '{doi}' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, NULL AS chapter_number, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub FROM ProceedingsPaperTable LEFT JOIN AuthorsTable ON ProceedingsPaperTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN ProceedingsTable ON ReferencesTable.og_doi==ProceedingsTable.id_venue LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON ProceedingsTable.id_venue==VenuesIdTable.doi_venues_id WHERE id = '{doi}'".format(doi=item)
                        cur.execute(query)
                        result_q = cur.fetchall()
                        result.extend(result_q)
                    qrdb.commit()
                return pd.DataFrame(data=result,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "chapter_number", "family", "given", "orcid", "ref_doi", "issn_isbn", "publisher", "name_pub"])
            else:
                raiseExceptions("The input parameter is not a list or it is not a list of strings!") 

# ===== THE TRIPLESTORQUERYPROCESSOR FUNCTIONS AND CLASSES

class TriplestoreProcessor(object):
    def __init__(self):
        self.endpointUrl = ""

    def getEndpointUrl(self):
        return self.endpointUrl
    
    def setEndpointUrl(self, endpointUrl):
        self.endpointUrl = endpointUrl
        return isinstance(endpointUrl, str)

class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self) -> None: #return annotation because Triplestore data processor doesn't return anything but the boolean value and the populated FF_graph through its method uploadData.
        super().__init__()

    def uploadData(self, path):
        # input: user provided path of a csv or json file.
        # output: a boolean value.
        # Once instanciated all the triples in the FF_graph, the function uploadData also provides the updating of the SPARQL store into the BlazegraphDB.
        
        # === STEP 1: INSTANTIATE THE ENDPOINT AND OPEN-CLOSE THE CONNECTION TO THE STORE === #
        # Firstly, I want to get the endpoint, in order to open the connection to the BlazegraphDB and explore it.
        # Secondly, I want to keep track of the "current situation" of the database before storing in it new triples, e.g. if there are already existent triples in it or not (this is done in the case the function uploadData is called multiple times).
        # Finally, I can close the connection to create new triples.
        endpoint = self.getEndpointUrl()
        store = SPARQLUpdateStore()
        store.open((endpoint, endpoint))
        storedTriples = store.__len__(context=None)
        store.close()

        # === STEP 2: CREATE THE RDF GRAPH AND POPULATE IT === #
        FF_graph = Graph()

        # classes 
        JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
        BookChapter = URIRef("https://schema.org/Chapter")
        Journal = URIRef("https://schema.org/Periodical")
        Book = URIRef("https://schema.org/Book")
        Proceedings = URIRef("https://schema.org/EventSeries")
        ProceedingsPaper = URIRef("http://purl.org/spar/fabio/ProceedingsPaper")

        # attributes related to classes
        doi = URIRef("https://schema.org/identifier") #used for every identifier
        publicationYear = URIRef("https://dbpedia.org/ontology/year")
        title = URIRef("https://schema.org/name")
        issue = URIRef("https://schema.org/issueNumber")
        volume = URIRef("https://schema.org/volumeNumber")
        chapter_num = URIRef("https://schema.org/numberedPosition")
        publisher = URIRef("https://schema.org/publisher")
        publicationVenue = URIRef("https://schema.org/isPartOf")
        author = URIRef("https://schema.org/author")
        name = URIRef("https://schema.org/givenName")
        surname = URIRef("https://schema.org/familyName")
        citation = URIRef("https://schema.org/citation")
        event = URIRef("https://schema.org/event")

        base_url = "https://FF.github.io/res/"
        
        if ".csv" in path: # <<<<<<< CSV FILE FROM PATH >>>>>>>>
            publicationVenue_cache = {} #caches here are counters in the form of dictionaries
            publicationPublisher_cache = {}
            publicationVenueIdx_cache = 0 #counter
            publicationCounter = 0 #I need a publicationCounter to know how many publications are in my Store. See the documentation for more.
            venueProceedings_cache = {}  #cache for proceedings. 
            publications = read_csv (path, keep_default_na=False,
                                    dtype={
                                        "id": "string",
                                        "title": "string",
                                        "type":"string",
                                        "publication_year":"int",
                                        "issue": "string",
                                        "volume": "string",
                                        "chapter":"string",
                                        "publication_venue": "string",
                                        "venue_type": "string",
                                        "publisher":"string",
                                        "event":"string"
                                        })
            if storedTriples == 0: #if my SPARQL store is empty [FIRST CASE SCENARIO]
                # --- PUBLICATIONS --- #
                for idx, row in publications.iterrows():
                    publication_id = "publication-" + str(idx) #instantiate the first node to build the triples
                    subj = URIRef(base_url + publication_id) #publication as subject
                    # basic publication triples:
                    FF_graph.add((subj, doi, Literal(row["id"])))
                    FF_graph.add((subj, title, Literal(row["title"])))
                    FF_graph.add((subj, publicationYear, Literal(row["publication_year"])))
                    #triples for Journal Article class:
                    if row["type"] == "journal-article":
                        FF_graph.add((subj, RDF.type, JournalArticle))
                        FF_graph.add((subj, issue, Literal(row["issue"]))) #when I'll query the database, I'll ask for Publication of type "" that has issue/volume
                        FF_graph.add((subj, volume, Literal(row["volume"]))) 
                    #triples for Book Chapter class:
                    elif row["type"] == "book-chapter":
                        FF_graph.add((subj, RDF.type, BookChapter))
                        FF_graph.add((subj, chapter_num, Literal(row["chapter"])))
                    elif row["type"] == "proceedings-paper":
                        FF_graph.add((subj, RDF.type, ProceedingsPaper))
                    # --- VENUES --- I check whether the Venues are present or not in my cache 
                    publicationVenueValue = row["publication_venue"]
                    if publicationVenueValue not in publicationVenue_cache:
                        venues_id = "venue-" + str(len(publicationVenue_cache)) # since the store is empty, the first venuesubject-related triple I'll add will be the first in absolute.
                        subjVenue = URIRef(base_url + venues_id)  #instantiate the subject node
                        publicationVenue_cache[publicationVenueValue] = subjVenue #the value is now part of my cache
                        #Publication-hasVenue-Venue triples:
                        FF_graph.add((subj, publicationVenue, subjVenue))
                        venueTypeValue = row["venue_type"]
                        #triples for Journal class:
                        if venueTypeValue == "journal":
                            FF_graph.add((subjVenue, RDF.type, Journal))
                        #triples for Book class:
                        elif venueTypeValue == "book":
                            FF_graph.add((subjVenue, RDF.type, Book))
                        #triple for Proceedings class:
                        elif venueTypeValue == "proceedings":
                            FF_graph.add((subjVenue, RDF.type, Proceedings))
                            FF_graph.add((subjVenue, event, Literal(row["event"])))
                        #triple for Publication Venue Value:
                        FF_graph.add((subjVenue, title, Literal(publicationVenueValue)))  
                    elif publicationVenueValue in publicationVenue_cache:
                        subjVenue = publicationVenue_cache[publicationVenueValue]
                        FF_graph.add((subj, publicationVenue, subjVenue))

                    publisherValue = row["publisher"]
                    if publisherValue not in publicationPublisher_cache:
                        subjPublisher = URIRef(base_url + "publisher-" + str(len(publicationPublisher_cache)))
                        publicationPublisher_cache[publisherValue] = subjPublisher #the value is now part of my cache
                        #Publisher related triples -> (Publication-hasPublisher-Publisher(Name)) | (Publisher-isPublisherOf, PublicationDoi) 
                        FF_graph.add((subj, publisher, subjPublisher))
                        FF_graph.add((subjPublisher, doi, Literal(publisherValue)))
                    else:
                        subjPublisher = publicationPublisher_cache[publisherValue]
                        FF_graph.add((subj, publisher, subjPublisher)) # publication has Publisher(Name)

            elif storedTriples > 0: #if my SPARQL store has been already set with some triples, I want to track what element and how many elements are there in it.
                pubQuery = """
                PREFIX schema: <https://schema.org/>
                SELECT ?publication
                WHERE {
                ?publication schema:identifier ?identifier .
                FILTER regex(?identifier, "doi")
                }
                """
                publicationOutput = get(endpoint, pubQuery, True)
                numberOfPublication = publicationOutput.shape[0]

                venQuery ="""
                PREFIX schema: <https://schema.org/>
                SELECT DISTINCT ?venue
                WHERE {
                ?publ schema:isPartOf ?venue .
                }
                """
                publicationVenueIdx_cache = get(endpoint, venQuery, True).shape[0] 
                for idx, row in publications.iterrows():
                    publicationDoiValues = row["id"] #value present in the dataframe, to search in the existing store
                    comparePublications_query = """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publication
                    WHERE {{
                        ?publication schema:identifier "{0}" . 
                    }}
                    """
                    comparePublicationsOutput = get(endpoint, comparePublications_query.format(publicationDoiValues), True)
                    # --- PUBLICATIONS --- #
                    if comparePublicationsOutput.empty: #if there are no doi stored in the output dataframe from the previous query, i.e. if that doi value is not in anytriple as subject:
                        subj = URIRef(base_url + "publication-" + str(numberOfPublication + publicationCounter))
                        publicationCounter +=1
                        FF_graph.add((subj, doi, Literal(publicationDoiValues)))
                        # --- VENUES --- After querying about dois, now in the same condition loop I insert another query, to complete the framework "publication-has-publication-venue". 
                        # I expect that there will be no venue value stored yet, unless it comes from another csv file uploading operation.
                        venuQuery ="""
                        PREFIX schema: <https://schema.org/>
                        SELECT ?venue
                        WHERE {{
                            ?publication schema:isPartOf ?venue .
                            ?venue schema:name "{0}" .
                        }}
                        """ 
                        comparePublicationVenuesOutput = get(endpoint, venuQuery.format(row["publication_venue"]), True)
                        if comparePublicationVenuesOutput.empty: #no venues shared in common
                            if row["publication_venue"] in publicationVenue_cache: # I check also the publication Venue cache
                                subjVenue = publicationVenue_cache[row["publication_venue"]] 
                            else:
                                subjVenue = URIRef(base_url + "venue-" + str(publicationVenueIdx_cache + len(publicationVenue_cache)))
                                publicationVenue_cache[row["publication_venue"]] = subjVenue
                                #Journal Class-related triple
                                if row["venue_type"] == "journal": 
                                    FF_graph.add((subjVenue, RDF.type, Journal))
                                #Book Class-related triples
                                elif row["venue_type"] == "book":
                                    FF_graph.add((subjVenue, RDF.type, Book))
                                #Proceedings Class-related triples
                                elif row["venue_type"] == "proceedings":
                                    FF_graph.add((subjVenue, RDF.type, Proceedings))
                                    FF_graph.add((subjVenue, event, Literal(row["event"])))
                                FF_graph.add((subjVenue, title, Literal(row["publication_venue"])))
                            
                        else: #if the doi value is already stored in the graph:
                            subjVenue = URIRef(comparePublicationVenuesOutput.at[0, "venue"]) 
                            if row["venue_type"] == "journal": 
                                FF_graph.add((subjVenue, RDF.type, Journal))
                            elif row["venue_type"] == "book":
                                FF_graph.add((subjVenue, RDF.type, Book))
                            elif row["venue_type"] == "proceedings":
                                FF_graph.add((subjVenue, RDF.type, Proceedings))
                                FF_graph.add((subjVenue, event, Literal(row["event"])))
                            FF_graph.add((subjVenue, title, Literal(row["publication_venue"])))
                        FF_graph.add((subj, publicationVenue, subjVenue))

                    else: #if I have already some publications in the graph registered as subject [SECOND CASE SCENARIO]
                        publicationQuery = """
                        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        PREFIX schema: <https://schema.org/>

                        SELECT ?publication
                        WHERE {{
                            ?publication schema:identifier "{0}"
                        }}
                        """
                        subj = URIRef(get(endpoint, publicationQuery.format(publicationDoiValues), True).at[0, 'publication'])
                        #I check if a doi is already linked to a venue
                        venueQuery ="""
                        PREFIX schema: <https://schema.org/>
                        SELECT ?venue
                        WHERE {{
                            ?publication schema:isPartOf ?venue .
                            ?publication schema:identifier "{0}" .
                        }}
                        """
                        comparePublicationAndVenues = get(endpoint, venueQuery.format(publicationDoiValues), True)
                        if comparePublicationAndVenues.empty:
                            if row["publication_venue"] in publicationVenue_cache:
                                subjVenue = publicationVenue_cache[row["publication_venue"]]
                            else:
                                venueQuery ="""
                                PREFIX schema: <https://schema.org/>
                                SELECT ?venue
                                WHERE {{
                                    ?publication schema:isPartOf ?venue .
                                    ?venue schema:name "{0}" .
                                }}
                                """
                                compareVenues = get(endpoint, venue_query.format(row["publication_venue"]), True)
                                if compareVenues.empty:
                                    subjVenue = URIRef(base_url + "venue-" + str(publicationVenueIdx_cache + len(publicationVenue_cache)))
                                    publicationVenue_cache[row["publication_venue"]] = subjVenue
                                    if row["venue_type"] == "journal": 
                                        FF_graph.add((subjVenue, RDF.type, Journal))
                                    elif row["venue_type"] == "book":
                                        FF_graph.add((subjVenue, RDF.type, Book))
                                    elif row["venue_type"] == "proceedings":
                                        FF_graph.add((subjVenue, RDF.type, Proceedings))
                                        FF_graph.add((subjVenue, event, Literal(row["event"])))

                                    FF_graph.add((subjVenue, title, Literal(row["publication_venue"])))
                                else:
                                    subjVenue = URIRef(compareVenues.at[0, 'venue'])      
                        else: 
                            subjVenue = URIRef(comparePublicationAndVenues.at[0, 'venue'])

                        if row["venue_type"] == "journal": 
                            FF_graph.add((subjVenue, RDF.type, Journal))
                        elif row["venue_type"] == "book":
                            FF_graph.add((subjVenue, RDF.type, Book))
                        elif row["venue_type"] == "proceedings":
                            FF_graph.add((subjVenue, RDF.type, Proceedings))
                            FF_graph.add((subjVenue, event, Literal(row["event"])))
                        FF_graph.add((subjVenue, title, Literal(row["publication_venue"])))
                        FF_graph.add((subj, publicationVenue, subjVenue))
                        
                    if row["type"] == "journal-article":
                        FF_graph.add((subj, RDF.type, JournalArticle))
                        FF_graph.add((subj, issue, Literal(row["issue"]))) 
                        FF_graph.add((subj, volume, Literal(row["volume"]))) 
                    elif row["type"] == "book-chapter":
                        FF_graph.add((subj, RDF.type, BookChapter))
                        FF_graph.add((subj, chapter_num, Literal(row["chapter"])))
                    elif row["type"] == "proceedings-paper":
                        FF_graph.add((subj, RDF.type, ProceedingsPaper))
                    FF_graph.add((subj, title, Literal(row["title"])))
                    FF_graph.add((subj, publicationYear, Literal(row["publication_year"])))
                    
                    comparePublishers_query="""
                    PREFIX schema:<https://schema.org/>
                    SELECT ?publisher
                    WHERE {{
                        ?publisher schema:identifier "{0}"
                    }}
                    """
                    comparePublishersOutput = get(endpoint, comparePublishers_query.format(row["publisher"]), True)
                    if comparePublishersOutput.empty:
                        if row["publisher"] in publicationPublisher_cache:
                            subjPublisher = publicationPublisher_cache[row["publisher"]]
                            FF_graph.add((subj, publisher, subjPublisher))
                        else:
                            publi_query = """
                            PREFIX schema:<https://schema.org/>
                            SELECT ?publisher
                            WHERE {
                                ?publisher schema:identifier ?pub_id .
                                FILTER regex(?pub_id, "crossref")
                            }
                            """
                            publiOutput = get(endpoint, publi_query, True).shape[0]
                            subjPublisher = URIRef(base_url + "publisher-" + str(len(publicationPublisher_cache) + publiOutput))
                            FF_graph.add((subj, publisher, subjPublisher))
                            FF_graph.add((subjPublisher, doi, Literal(row["publisher"])))
                            publicationPublisher_cache[row["publisher"]] = subjPublisher
                    else:
                        subjPublisher = URIRef(comparePublishersOutput.at[0, "publisher"])
                        FF_graph.add((subj, publisher, subjPublisher))
        
        elif ".json" in path: # <<<<<<< JSON FILE FROM PATH >>>>>>>> 
            with open(path, "r", encoding="utf-8") as file:
                jsondata = load(file) #since json is a hierarchical datatype, I can see it as a dictionary of dictionaries.

            authors = jsondata.get("authors") #json object
            venues = jsondata.get("venues_id") #json object
            references = jsondata.get("references") #json object
            publishers = jsondata.get("publishers") #json object

            #See the documentation for more about empty dictionaries and empty lists instantiated here.
            doi_authors=[]
            family=[]
            given=[]
            orcid=[]
            for key in authors:
                a=[] #list for family value
                b=[] #list for given value
                c=[] #list for orcid value
                authorValue = authors[key] #doi related
                doi_authors.append(key)
                for dict in authorValue: #values of the author
                    a.append(dict["family"]) 
                    b.append(dict["given"]) 
                    c.append(dict["orcid"]) 
                family.append(a) 
                given.append(b)
                orcid.append(c)

                fromDoiAuthors_s=Series(doi_authors) 
            fromFamily_s=Series(family)
            fromGiven_s=Series(given)
            fromOrcid_s=Series(orcid)
            # === AUTHORS DATAFRAME POPULATED:
            authors_df=DataFrame({
                "auth doi" : fromDoiAuthors_s,
                "family name" : fromFamily_s, 
                "given name" : fromGiven_s, 
                "orcid" : fromOrcid_s
            })

            doi_venues_id=[] #a list for venues-related doi. 
            issn_isbn=[]
            for key in venues: #I iterate over the venues json object.
                venuesValue = venues[key]
                doi_venues_id.append(key)
                issn_isbn.append(venuesValue)
            standaloneVenues=[] #list of issn
            doi_Cache=[] #list of doi issn/ibn-related
            for a in range(len(issn_isbn)): #here I iterate over the range of the lenght of the list, to obtain a unique index.
                if issn_isbn[a] not in standaloneVenues: #if the issn/isbn value is not present in the list of venues, add it in the form of a new key.
                    standaloneVenues.append(issn_isbn[a])
                    dois = [] 
                    dois.append(doi_venues_id[a])
                    doi_Cache.append(dois)
                else:
                    b = standaloneVenues.index(issn_isbn[a])
                    doi_Cache[b].append(doi_venues_id[a])

            standaloneDoiVenues_s=Series(doi_Cache) #standalone series
            standaloneIssn_s=Series(standaloneVenues)
            # === VENUES DATAFRAME POPULATED:
            standaloneVenues_df=DataFrame({
                "venues doi" : standaloneDoiVenues_s,
                "issn" : standaloneIssn_s, 
            })
            venuesValue_df = standaloneVenues_df[["venues doi", "issn"]]
            venues_Id = []
            for idx, row in venuesValue_df.iterrows():
                venues_Id.append("venues-" + str(idx))
            venuesValue_df.insert(0, "venues id", Series(venues_Id, dtype="string"))
            venuesIdValue_s=venuesValue_df.filter(["venues id"]) # I extract the data
            venuesIdValue_s=venuesIdValue_s.values.tolist() # Transform it into a list
            doi_venues_id=venuesValue_df.filter(["venues doi"])
            doi_venues_id=doi_venues_id.values.tolist()
            venues_id_issn=venuesValue_df.filter(["issn"])
            venues_id_issn=venues_id_issn.values.tolist()

            internalId=[]
            doiVenues=[]
            issnVenues=[]
            for dv in doi_venues_id: #iterating over every series
                e=doi_venues_id.index(dv)
                for list in dv:
                    for dois in list:
                        doiVenues.append(dois)
                        internalId.append(venuesIdValue_s[e][0])
                        issnVenues.append(venues_id_issn[e][0])
            internalId_s=Series(internalId)
            doi_s=Series(doiVenues)
            issn_s=Series(issnVenues)
            standaloneVenues_ids_df=DataFrame({"venueinternalId_s":internalId_s, "venues doi":doi_s, "issn":issn_s})

            ref_doi=[]
            ref_cit=[]
            for key in references:
                referenceValue = references[key]
                ref_doi.append(key)
                ref_cit.append(referenceValue)
            doiReferences=Series(ref_doi)
            citReferences=Series(ref_cit)
            # === REFERENCES DATAFRAME POPULATED:
            ref_df=DataFrame({
                "ref doi" : doiReferences,
                "citation" : citReferences, 
            })

            publisherCrossref=[]
            pub_id=[]
            pub_name=[]
            for key in publishers:
                pub_val = publishers[key] #crossref
                publisherCrossref.append(key)
                pub_id.append(pub_val["id"])
                pub_name.append(pub_val["name"])
            publisherCrossref_s=Series(publisherCrossref)
            pub_id_s=Series(pub_id)
            pub_name_s=Series(pub_name)
            # === PUBLISHERS DATAFRAME POPULATED:
            publishers_df=DataFrame({
                "crossref" : publisherCrossref_s,
                "publisher id" : pub_id_s, 
                "name" : pub_name_s, 
            })

            venues_authors = merge(standaloneVenues_ids_df, authors_df, left_on="venues doi", right_on="auth doi", how="outer")
            venues_authors_ref = merge(venues_authors, ref_df, left_on="venues doi", right_on="ref doi", how="outer")

            authors_dict = {}
            publications_cache = {} #some publications that may have been instanciated more than once
            _ = [] #an empty list to check the type in the dataframe according to how I'm going to populate the Graph
            venues_dict = {}
            publicationPublisher_cache = {}

            if storedTriples == 0: #if my SPARQL store is empty [FIRST CASE SCENARIO]
                for idx, row in venues_authors_ref.iterrows():
                    publication_id = "publication-" + str(idx) #instantiate the first node, with the publication entity
                    subj = URIRef(base_url + publication_id)
                    #authors-related triples:
                    FF_graph.add((subj, doi, Literal(row["auth doi"]))) #as said in the documentation, the software has been developed in a publication/venue-centric modus operandi.
                    #That's why every time I have the chance to retrieve a doi, I immediately want to store it in a cache.
                    #Also and maybe more than ever when a doi comes from a json file, i.e. a source that usually contains "additional information" about publications.
                    publications_cache[row["auth doi"]] = subj
                    #publishers-related triples
                    if type(row["orcid"]) == type(_):
                        for i in range(len(row["orcid"])):
                            if row["orcid"][i] not in authors_dict:
                                author_subj = URIRef(base_url + "author-" + str(len(authors_dict)))
                                authors_dict[row["orcid"][i]] = author_subj
                                FF_graph.add((author_subj, doi, Literal(row["orcid"][i])))
                                FF_graph.add((author_subj, name, Literal(row["given name"][i])))
                                FF_graph.add((author_subj, surname, Literal(row["family name"][i])))
                            else:
                                author_subj = authors_dict[row["orcid"][i]]
                            FF_graph.add((subj, author, author_subj))
                    
                    #venues-related triples
                    if row["venues doi"] in publications_cache:
                        subj = publications_cache[row["venues doi"]]
                    else: 
                        new_idx = len(publications_cache)
                        subj = URIRef(base_url + "publication-" + str(new_idx))
                        publications_cache[row["venues doi"]] = subj
                    if type(row["issn"]) == type(_):
                        for num in range(len(row["issn"])):
                            if row["venues doi"] in venues_dict: 
                                subjVenue = venues_dict[row["venues doi"]]
                            else:
                                subjVenue = URIRef(base_url + "venue-" + str(len(venues_dict))) 
                                venues_dict[row["venues doi"]] = subjVenue
                            FF_graph.add((subjVenue, doi, Literal(row["issn"][num])))          
                            FF_graph.add((subj, publicationVenue, subjVenue))
                    
                    
                    #citations-related triples
                    if row["ref doi"] in publications_cache:
                        subj = publications_cache[row["ref doi"]]
                    else: 
                        alfa = len(publications_cache)
                        subj = URIRef(base_url + "publication-" + str(alfa))
                        FF_graph.add((subj, doi, Literal(row["ref doi"])))
                        publications_cache[row["ref doi"]] = subj
                    if type(row["citation"]) == type(_):
                        for c in range(len(row["citation"])):
                            if row["citation"][c] in publications_cache:
                                cited_publ = publications_cache[row["citation"][c]]
                            else:
                                other_idx = len(publications_cache)
                                cited_publ = URIRef(base_url + "publication-" + str(other_idx))
                                publications_cache[row["citation"][c]] = cited_publ
                            FF_graph.add((subj, citation, cited_publ))
                
                for idx, row in publishers_df.iterrows():
                    subj = URIRef(base_url + "publisher-" + str(idx))
                    FF_graph.add((subj, doi, Literal(row["crossref"])))
                    FF_graph.add((subj, title, Literal(row["name"])))        
                
            elif storedTriples > 0: #if my SPARQL store has been already populated [SECOND CASE SCENARIO]
                # I check what authors data have been already put in the store
                trackAuthors = """
                PREFIX schema: <https://schema.org/>

                SELECT ?author
                WHERE {
                    ?publication schema:author ?author .
                }
                """
                authors_query_df = (get(endpoint, trackAuthors, True)).shape[0]
                #I check what venues have been already put in the store
                trackVenues = """
                PREFIX schema: <https://schema.org/>

                SELECT ?venue
                WHERE {
                    ?publication schema:isPartOf ?venue .
                }
                """
                venues_query_df = (get(endpoint, trackVenues, True)).shape[0]

                pQuery = """
                PREFIX schema: <https://schema.org/>
                SELECT ?publication
                WHERE {
                    ?publication schema:identifier ?identifier .
                    FILTER regex(?identifier, "doi")
                }
                """
                result_df = get(endpoint, pQuery, True)
                numberOfPublication = result_df.shape[0]
                for idx, row in venues_authors_ref.iterrows():
                    query = """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publication
                    WHERE {{
                        ?publication schema:identifier "{0}" .
                    }}
                    """
                    comparePublications = get(endpoint, query.format(row["auth doi"]), True)
                    if comparePublications.empty:
                        if row["auth doi"] in publications_cache:
                            subj = publications_cache[row["auth doi"]]
                        else:
                            numb = numberOfPublication + len(publications_cache)
                            subj = URIRef(base_url + "publication-" + str(numb))
                            publications_cache[row["auth doi"]] = subj
                        
                    else:
                        subj = URIRef(comparePublications.at[0, "publication"])                 
                    if type(row["orcid"]) == type(_):
                        for r in range(len(row["orcid"])):
                            aQuery = """
                            PREFIX schema: <https://schema.org/>
                            SELECT ?author
                            WHERE {{
                                ?publication schema:author ?author .
                                ?author schema:identifier "{0}"
                            }}
                            """
                            compareAuth = get(endpoint, aQuery.format(row["orcid"][r]), True)   
                            if compareAuth.empty:                         
                                if row["orcid"][r] not in authors_dict:
                                    author_subj = URIRef(base_url + "author-" + str(len(authors_dict) + authors_query_df))
                                    authors_dict[row["orcid"][r]] = author_subj
                                    FF_graph.add((author_subj, doi, Literal(row["orcid"][r])))
                                    FF_graph.add((author_subj, name, Literal(row["given name"][r])))
                                    FF_graph.add((author_subj, surname, Literal(row["family name"][r])))
                                else: 
                                    author_subj = authors_dict[row["orcid"][r]]
                            else:
                                author_subj = URIRef(compareAuth.at[0, "author"])
                            FF_graph.add((subj, author, author_subj))
                    query_two = """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publication
                    WHERE {{
                        ?publication schema:identifier "{0}" .
                    }}
                    """
                    compareAtr = get(endpoint, query_two.format(row["venues doi"]), True)
                    if compareAtr.empty:
                        if row["venues doi"] in publications_cache:
                            subj = publications_cache[row["venues doi"]]
                        else:
                            number_of_index = numberOfPublication + len(publications_cache)
                            subj = URIRef(base_url + "publication-" + str(number_of_index))
                            publications_cache[row["venues doi"]] = subj
                    else:
                        subj = URIRef(compareAtr.at[0, "publication"])
                    if type(row["issn"]) == type(_):
                        for e in range(len(row["issn"])):
                            venue_query = """
                            PREFIX schema: <https://schema.org/>
                            SELECT ?venue
                            WHERE {{
                                <{0}> schema:isPartOf ?venue .
                            }}
                            """
                            compareVenues = get(endpoint, venue_query.format(subj), True)
                            if compareVenues.empty:
                                if row["venues doi"] in venues_dict:
                                    subjVenue = venues_dict[row["venues doi"]]
                                else:
                                    subjVenue = URIRef(base_url + "venue-" + str(venues_query_df +len(venues_dict)))
                                    venues_dict[row["venues doi"]] = subjVenue
                                    FF_graph.add((subjVenue, doi, Literal(row["issn"][e])))
                                FF_graph.add((subj, publicationVenue, subjVenue))
                            else:
                                subjVenue = URIRef(compareVenues.at[0, "venue"])
                                FF_graph.add((subjVenue, doi, Literal(row["issn"][e])))
                    
                    doi_query= """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publication
                    WHERE {{
                        ?publication schema:identifier "{0}" .
                    }}
                    """
                    doi_df = get(endpoint, doi_query.format(row["ref doi"]), True)
                    if doi_df.empty:
                        if row["ref doi"] in publications_cache:
                            subj = publications_cache[row["ref doi"]] 
                        else: 
                            new_idx = len(publications_cache)
                            subj = URIRef(base_url + "publication-" + str(new_idx + numberOfPublication))
                            FF_graph.add((subj, doi, Literal(row["ref doi"])))
                            publications_cache[row["ref doi"]] = subj
                    if type(row["citation"]) == type(_):
                        for w in range(len(row["citation"])):
                            query_four= """
                            PREFIX schema: <https://schema.org/>
                            SELECT ?publication
                            WHERE {{
                                ?publication schema:identifier "{0}" .
                            }}
                            """
                            check_doi_four = get(endpoint, query_four.format(row["citation"][w]), True)
                            if check_doi_four.empty:
                                if row["citation"][a] in publications_cache:
                                    cited_publ = publications_cache[row["citation"][a]]
                                else:
                                    number_of_index = numberOfPublication + len(publications_cache)
                                    cited_publ = URIRef(base_url + "publication-" + str(number_of_index))
                                    publications_cache[row["citation"][a]] = subj
                            else:
                                cited_publ = URIRef(check_doi_four.at[0, "publication"])
                            FF_graph.add((subj, citation, cited_publ))
                for idx, row in publishers_df.iterrows():
                    pub_query = """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publisher
                    WHERE {{
                        ?publisher schema:identifier "{0}" .
                    }}
                    """
                    check_pub = get(endpoint, pub_query.format(row["crossref"]), True)
                    if check_pub.empty:
                        num_publisher = """
                        PREFIX schema: <https://schema.org/>
                        SELECT ?publisher
                        WHERE {
                            ?publication schema:publisher ?publisher .
                        }
                        """
                        pub_idx = get(endpoint, num_publisher, True).shape[0]
                        subj = URIRef(base_url + "publisher-" + str(pub_idx + len(publicationPublisher_cache)))
                        FF_graph.add((subj, doi, Literal(row["crossref"])))
                        FF_graph.add((subj, title, Literal(row["name"])))
                        publicationPublisher_cache[row["crossref"]] = subj
                    else:
                        subj = URIRef(check_pub.at[0, 'publisher'])
                        FF_graph.add((subj, title, Literal(row["name"])))                    
        
        store.open((endpoint, endpoint))
        for triple in FF_graph.triples((None, None, None)):
            store.add(triple)
        store.close()
        return isinstance(path, str)
        
        

class TriplestoreQueryProcessor(TriplestoreProcessor):
    def __init__(self) -> None:
        super().__init__()
    
    def getPublicationsPublishedInYear(self, year): 
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?publication ?title ?type ?publicationYear ?issue ?volume ?chapter ?author ?name ?orcid ?surname ?publisher ?pub_venue ?venue_type ?event
        WHERE {{
            ?publication schema:name ?title ;
            dcterms:date ?publicationYear ;
            rdf:type ?type ;
            schema:isPartOf ?pub_venue ;
            schema:author ?author ;
            schema:publisher ?publisher .
            ?author schema:givenName ?name ; 
            ?author schema:familyName ?surname .  
            ?pub_venue rdf:type ?venue_type ;
            ?author schema:identifier ?orcid .
            ?publication schema:publisher ?publisher .
            FILTER (?publicationYear = {0}) .
            OPTIONAL {
              ?publication schema:issueNumber ?issue }.
          	OPTIONAL {?publication schema:volumeNumber ?volume                                   
                                   }
            OPTIONAL {?publication schema:numberedPosition ?chapter}
        }}
        """
        publ = get(endpoint, new_query.format(year), True)
        return publ
    
    def getPublicationsByAuthorId(self, orcid):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?publication ?title ?type ?publicationYear ?issue ?volume ?chapter ?author ?name ?surname ?orcid ?citation ?publisher ?pub_venue ?venue_type ?pub_name ?event
        WHERE {{
            ?publication schema:name ?title .
            ?publication schema:datePublished ?publicationYear .
            ?publication rdf:type ?type .
            ?publication schema:isPartOf ?pub_venue .
            ?pub_venue rdf:type ?venue_type .
            ?publication schema:author ?author .
            ?author schema:givenName ?name .
            ?author schema:familyName ?surname .
            ?author schema:identifier ?orcid .
            FILTER (?orcid = "{0}") .
            ?publication schema:publisher ?publisher .
            ?publisher schema:name ?pub_name
            OPTIONAL {{?publication schema:issueNumber ?issue .
            ?publication schema:volumeNumber ?volume }} 
            OPTIONAL {{?publication schema:numberedPosition ?chapter}} 
            OPTIONAL {{?publication schema:citation ?citation}} 
        }}
        """
        publ = get(endpoint, new_query.format(orcid), True)
        return publ
    
    def getPublicationsByAuthorId(self, orcid):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?publication ?title ?type ?publicationYear ?issue ?volume ?chapter ?author ?name ?surname ?orcid ?citation ?publisher ?pub_venue ?venue_type ?pub_name ?event
        WHERE {{
            ?publication schema:name ?title .
            ?publication schema:datePublished ?publicationYear .
            ?publication rdf:type ?type .
            ?publication schema:isPartOf ?pub_venue .
            ?pub_venue rdf:type ?venue_type .
            ?publication schema:author ?author .
            ?author schema:givenName ?name .
            ?author schema:familyName ?surname .
            ?author schema:identifier ?orcid .
            FILTER (?orcid = "{0}") .
            ?publication schema:publisher ?publisher .
            ?publisher schema:name ?pub_name
            OPTIONAL {{?publication schema:issueNumber ?issue .
            ?publication schema:volumeNumber ?volume }} 
            OPTIONAL {{?publication schema:numberedPosition ?chapter}} 
            OPTIONAL {{?publication schema:citation ?citation}} 
        }}
        """
        publ = get(endpoint, new_query.format(orcid), True)
        return publ
        
    def getMostCitedPublication(self): 
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?citation (COUNT(?citation) AS ?cited) 
        WHERE { 
        ?publication schema:citation ?citation .
        }
        GROUP BY ?citation
        ORDER BY desc(?cited)
        """
        publ = get(endpoint, new_query, True)
        most_cited = publ.at[0, "citation"]
        return most_cited.head

    def getMostCitedVenue(self): 
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?venue (COUNT(?venue) AS ?cited) 
        WHERE { 
        ?publication schema:citation ?citation .
        ?citation schema:isPartOf ?venue
        }
        GROUP BY ?venue
        ORDER BY desc(?cited) 
        """
        most_cited = get(endpoint, new_query, True).at[0, "venue"]
        return most_cited.head
    
    def getVenuesByPublisherId(self, pub_id):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?pub_venue ?venue_name ?venue_id ?venue_type ?publisher ?pub_id ?pub_name ?event
        WHERE {{
            ?publication schema:isPartOf ?pub_venue ;
            schema:publisher ?publisher .
            ?pub_venue schema:identifier ?venue_id ;
            schema:name ?venue_name .
            
            VALUES ?venue_type {
                  <https://schema.org/ScholarlyArticle>
                  <https://schema.org/Chapter>
                  <https://schema.org/ProceedingsPaper> 
                  ?pub_venue rdf:type ?venue_type .
            ?publication 
            ?publisher schema:identifier ?pub_id .
            FILTER (?pub_id = "{0}") .
            ?publisher schema:name ?pub_name 
        }}
        """
        publ = get(endpoint, new_query.format(pub_id), True)
        return publ
    
    def getPublicationInVenue(self, venue):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT ?publication ?title ?type ?publicationYear ?issue ?volume ?chapter ?author ?name ?surname ?orcid ?citation ?publisher ?pub_venue ?venue_type ?pub_name ?event
        WHERE {{
            ?publication schema:name ?title .
            ?publication schema:datePublished ?publicationYear .
            ?publication rdf:type ?type .
            ?publication schema:isPartOf ?pub_venue .
            ?pub_venue schema:identifier ?pub_id .
            ?pub_venue rdf:type ?venue_type .
            ?publication schema:author ?author .
            ?author schema:givenName ?name .
            ?author schema:familyName ?surname .
            ?author schema:identifier ?orcid .
            FILTER (?pub_id = "{0}") .
            ?publication schema:publisher ?publisher .
            ?publisher schema:name ?pub_name .
            OPTIONAL {{?publication schema:issueNumber ?issue .
            ?publication schema:volumeNumber ?volume }} 
            OPTIONAL {{?publication schema:numberedPosition ?chapter}} 
            OPTIONAL {{?publication schema:citation ?citation}}    
        }}  
        """
        publ = get(endpoint, new_query.format(venue), True)
        return publ

    def getJournalArticlesInIssue (self, issn, volume ,issue):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT ?publication ?title ?type ?publicationYear ?issue ?volume ?chapter ?author ?name ?surname ?orcid ?citation ?publisher ?pub_venue ?venue_type ?pub_name ?event
        WHERE {{
            ?publication schema:name ?title ;
            schema:author ?author ;
            schema:datePublished ?publicationYear ;
            schema:isPartOf ?pub_venue ;
            schema:publisher ?publisher ;
            rdf:type ?type .
            VALUES ?type {
                  <https://schema.org/ScholarlyArticle>
                  <https://schema.org/Chapter>
                  <https://schema.org/ProceedingsPaper>
                  }     
            ?pub_venue schema:identifier ?pub_id ;
            rdf:type ?venue_type .
            VALUES ?venue_type {
                <https://schema.org/Chapter>
                <https://schema.org/Periodical>
                <https://schema.org/Book>
            }
            ?author schema:givenName ?name ;
            schema:familyName ?surname ;
            schema:identifier ?orcid .
            FILTER (?pub_id = "{0}") . 
            ?publisher schema:name ?pub_name .
            ?publication schema:volumeNumber "{1}" .
            ?publication schema:issueNumber "{2}" .
            OPTIONAL {{?publication schema:citation ?citation}}    
        }}  
        """
        publ = get(endpoint, new_query.format(issn, volume, issue), True)
        return publ

    def getJournalArticlesInVolume(self,volume,issn):
        pass
    def getJournalArticlesInJournal(self,issn):
        pass
    def getProceedingsByEvent(eventname):
        pass
    def getPublicationAuthors(self,doi):
        pass
    def getPublicationsByAuthorName(self,partialname):
        pass
    def getDistinctPublishersOfPublications(self,pubidlist):
        pass





# ===== THE GENERICQUERYPROCESSOR FUNCTIONS AND CLASSES
class GenericQueryProcessor(object):
    def __init__(self):
        self.queryProcessor = list()
        

    def cleanQueryProcessors(self):
        self.queryProcessor = []
        return True

    def addQueryProcessor(self,processor):
        pClass = type(processor)
        if issubclass (pClass,QueryProcessor):   #removed rel here
            self.queryProcessor.append(processor)
            df_creator(processor)
            return True
        else:
            return False

    def getPublicationsPublishedInYear(self,year):
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getPublicationsPublishedInYear(year)
            final_DF = pd.concat([final_DF,result_DF])
        
        result = list() # This is the list[Publication] to be returned at the end of the query (see: UML)

        # Since in the result_DF there will be duplicates for the dois (under the column "id"), I first need to get rid of these duplicate values and so I create a set and populate it with the dois (the string values under the column "id" in the final_DF)
        ids = set()
        for idx,row in final_DF.iterrows(): # I iterate over the final_DF (which will contain all the information needed to build each Publication object)
            ids.add(row["id"])  # For each row I add the value under the column "id" (which will be a doi string) to the set
            # As the set is an unordered collection of unique elements, I don't have to worry about duplicates: if the doi is already contained in the set it will NOT be added to it again 
        
        # I iterate over the set and, for each item of the set, I call the additional method and create a Publication object (and add this Publication object to the result list)
        for item in ids:
            result.append(creatPubobj(item))
        return result
        #list[Publication]
    
    def getPublicationsByAuthorId(self,id):
        # The id is going to be a orcid
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getPublicationsByAuthorId(id)
            final_DF = pd.concat([final_DF,result_DF])
        
        result = list()

        ids = set()
        for idx,row in final_DF.iterrows():
            ids.add(row["id"])

        for item in ids:
            result.append(creatPubobj(item))
        return result
        #list[Publication]

    def getMostCitedPublication(self):
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getMostCitedPublication()
            final_DF = pd.concat([final_DF,result_DF])
        
        max_cit = final_DF["num_citations"].loc[final_DF.index[0]]  # This is the highest value of the "num_citations"... FOR NOW
        max_doi = final_DF["id"].loc[final_DF.index[0]]
        dict1 = {}
        max_list = list(dict1)
        for idx,row in final_DF.iterrows():
            cit = row["num_citations"]
            if cit > max_cit:
                max_cit = cit
            elif cit == max_cit:
                tpl = tuple((row["id"],cit))
                dict2 = {"doi":row['id'],"citnum":cit}
                max_list.append(dict2)
        
        result = list()
        
        for i in max_list:
            for key in i.keys():
                if type(i[key]) == int:
                    pass
                else:
                    result.append(creatPubobj(i[key]))
        
        return result
        #Publication -> here we will NOT return just a single Publication (as asked by the UML), but a list[Publication] because there may be multiple venues that are all the most cited (they have the same number of citations and therefore are all at the top of the descending order of cited venues)

    def getMostCitedVenue(self):
        final_DF = pd.DataFrame()
        
        for item in self.queryProcessor:
            result_DF = item.getMostCitedVenue()
            final_DF = pd.concat([final_DF,result_DF])
        
        # CHANGE THE FINAL_DF IN SUCH A WAY THAT IT HAS ONLY THE MOST CITED VENUES ***OF ALL***

        max_cit = final_DF["num_cit"].loc[final_DF.index[0]]  # This is the highest value of the "num_citations"... FOR NOW
        max_ven = final_DF["name_venue"].loc[final_DF.index[0]]
        dict1 = {}
        max_list = list(dict1)


        result = list()
        for idx,row in final_DF.iterrows():
            # Now we need to save all the information needed to create an object of class Venue
            # ["ids"](=issn_isbn), "title"(of the Venue), ["ids"](=crossref), "name"(of the Organisation) 
            title = row["name_venue"] # -> "title"(of the Venue)
            ids_list = list() # -> ["ids"](=issn_isbn)
            for idx2,row2 in final_DF.iterrows():
                title2 = row2["name_venue"]
                if title2 == title:
                    # We are in the same Venue!
                    issn_isbn = row2["issn_isbn"]
                    ids_list.append(issn_isbn)
            crossref_list = list() # -> ["ids"](=crossref)
            for idx3,row3 in final_DF.iterrows():
                title3 = row3["name_venue"]
                if title3 == title:
                    crossref = row3["publisher"]
                    crossref_list.append(crossref)
            name_org = row["name_pub"]
            publ_obj = Organisation(crossref_list,name_org)
            venue_obj = Venue(ids_list,title,publ_obj)
            result.append(venue_obj)
        return result
        #Venue -> here we will NOT return just a single Venue (as asked by the UML), but a list[Venue] because there may be multiple venues that are all the most cited (they have the same number of citations and therefore are all at the top of the descending order of cited venues)
        
    def getVenuesByPublisherId(self,id):
        # The id is going to be a crossref
        final_DF = pd.DataFrame()
        
        for item in self.queryProcessor:
            result_DF = item.getVenuesByPublisherId(id)
            final_DF = pd.concat([final_DF,result_DF])

        result = list()
        for idx,row in final_DF.iterrows():
            title = row["name_venue"] # -> "title"(of the Venue)
            ids_list = list() # -> ["ids"](=issn_isbn)
            for idx2,row2 in final_DF.iterrows():
                title2 = row2["name_venue"]
                if title2 == title:
                    # We are in the same Venue!
                    issn_isbn = row2["issn_isbn"]
                    ids_list.append(issn_isbn)
            crossref_list = list() # -> ["ids"](=crossref)
            for idx3,row3 in final_DF.iterrows():
                title3 = row3["name_venue"]
                if title3 == title:
                    crossref = row3["publisher"]
                    crossref_list.append(crossref)
            name_org = row["name_pub"]
            publ_obj = Organisation(crossref_list,name_org)
            venue_obj = Venue(ids_list,title,publ_obj)
            result.append(venue_obj)
        return result
        #list[Venue]
        
    def getPublicationInVenue(self,venueId):
        # The id is going to be a issn_isbn
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getPublicationInVenue(venueId)
            final_DF = pd.concat([final_DF,result_DF])
        
        result = list()

        ids = set()
        for idx,row in final_DF.iterrows():
            ids.add(row["id"])
            
        for item in ids:
            result.append(creatPubobj(item))
        return result
        #list[Publication]

    def getJournalArticlesInIssue(self,issue,volume,journalId):
        # The id is going to be a doi
        #list[JournalArticle]
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getJournalArticlesInIssue(issue,volume,journalId)
            final_DF = pd.concat([final_DF,result_DF])
        
        result = list() # This is the list[Publication] to be returned at the end of the query (see: UML)

        # Since in the result_DF there will be duplicates for the dois (under the column "id"), I first need to get rid of these duplicate values and so I create a set and populate it with the dois (the string values under the column "id" in the final_DF)
        ids = set()
        for idx,row in final_DF.iterrows(): # I iterate over the final_DF (which will contain all the information needed to build each Publication object)
            ids.add(row["id"])  # For each row I add the value under the column "id" (which will be a doi string) to the set
            # As the set is an unordered collection of unique elements, I don't have to worry about duplicates: if the doi is already contained in the set it will NOT be added to it again 
        
        # I iterate over the set and, for each item of the set, I call the additional method and create a Publication object (and add this Publication object to the result list)
        for item in ids:
            result.append(creatJAobj(item))
        return result
        #list[Publication]

    def getJournalArticlesInVolume(self,volume,journalId):
        # The id is going to be a doi
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getJournalArticlesInVolume(volume,journalId)
            final_DF = pd.concat([final_DF,result_DF])
        
        result = list() # This is the list[Publication] to be returned at the end of the query (see: UML)
        print (final_DF)
        # Since in the result_DF there will be duplicates for the dois (under the column "id"), I first need to get rid of these duplicate values and so I create a set and populate it with the dois (the string values under the column "id" in the final_DF)
        ids = set()
        for idx,row in final_DF.iterrows(): # I iterate over the final_DF (which will contain all the information needed to build each Publication object)
            ids.add(row["id"])  # For each row I add the value under the column "id" (which will be a doi string) to the set
            # As the set is an unordered collection of unique elements, I don't have to worry about duplicates: if the doi is already contained in the set it will NOT be added to it again 
        
        # I iterate over the set and, for each item of the set, I call the additional method and create a Publication object (and add this Publication object to the result list)
        for item in ids:
            result.append(creatJAobj(item))
        return result
        #list[JournalArticle]

    def getJournalArticlesInJournal(self,journalId):
        # The id is going to be a doi
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getJournalArticlesInVolume(journalId,journalId)
            final_DF = pd.concat([final_DF,result_DF])
        
        result = list() # This is the list[Publication] to be returned at the end of the query (see: UML)

        # Since in the result_DF there will be duplicates for the dois (under the column "id"), I first need to get rid of these duplicate values and so I create a set and populate it with the dois (the string values under the column "id" in the final_DF)
        ids = set()
        for idx,row in final_DF.iterrows(): # I iterate over the final_DF (which will contain all the information needed to build each Publication object)
            ids.add(row["id"])  # For each row I add the value under the column "id" (which will be a doi string) to the set
            # As the set is an unordered collection of unique elements, I don't have to worry about duplicates: if the doi is already contained in the set it will NOT be added to it again 
        
        # I iterate over the set and, for each item of the set, I call the additional method and create a Publication object (and add this Publication object to the result list)
        for item in ids:
            result.append(creatJAobj(item))
        return result
        #list[JournalArticle]
        # ^^ currently not working!!

    def getProceedingsByEvent(self,eventPartialName):
        final_DF = pd.DataFrame()
        
        for item in self.queryProcessor:
            result_DF = item.getProceedingsByEvent(eventPartialName)
            final_DF = pd.concat([final_DF,result_DF])

        result = list()
        for idx,row in final_DF.iterrows():
            event = row["event"] # -> additional info "event" str
            title = row["name_venue"] # -> "title"(of the Venue)
            ids_list = list() # -> ["ids"](=issn_isbn)
            for idx2,row2 in final_DF.iterrows():
                title2 = row2["name_venue"]
                if title2 == title:
                    # We are in the same Venue!
                    issn_isbn = row2["issn_isbn"]
                    ids_list.append(issn_isbn)
            crossref_list = list() # -> ["ids"](=crossref)
            for idx3,row3 in final_DF.iterrows():
                title3 = row3["name_venue"]
                if title3 == title:
                    crossref = row3["publisher"]
                    crossref_list.append(crossref)
            name_org = row["name_pub"]
            publ_obj = Organisation(crossref_list,name_org)
            proc_obj = Proceedings(ids_list,title,publ_obj,event)
            result.append(proc_obj)
        return result
        #list[Proceeding]
        
    def getPublicationAuthors(self,publicationId):
        # The id is going to be a doi
        final_DF = pd.DataFrame()
        
        for item in self.queryProcessor:
            result_DF = item.getPublicationAuthors(publicationId)
            final_DF = pd.concat([final_DF,result_DF])

        result = list()
        for idx,row in final_DF.iterrows():
            #["ids"](=orcid), "givenName", "familyName"
            doi = row["doi_authors"]
            ids_list = list() # -> ["ids"](=orcid)
            for idx2,row2 in final_DF.iterrows():
                doi2 = row2["doi_authors"]
                if doi2 == doi:
                    # We are in the same publication
                    id = row2["orcid"]
                    ids_list.append(id)
            given = row["given"] # -> "givenName"
            family = row["family"] # -> "familyName"
            pers_obj = Person(ids_list,given,family)
            result.append(pers_obj)
        return result
        #list[Person]

    def getPublicationsByAuthorName(self,authorPartialName):
        final_DF = pd.DataFrame()
            
        for item in self.queryProcessor:
            result_DF = item.getPublicationsByAuthorName(authorPartialName)
            final_DF = pd.concat([final_DF,result_DF])
        
        result = list()

        ids = set()
        for idx,row in final_DF.iterrows():
            ids.add(row["id"])
            
        for item in ids:
            result.append(creatPubobj(item))
        return result
        #list[Publication]

    def getDistinctPublishersOfPublications(self,pubIdList):
        # The ids are going to be dois
        final_DF = pd.DataFrame()
        
        for item in self.queryProcessor:
            result_DF = item.getDistinctPublishersOfPublications(pubIdList)
            final_DF = pd.concat([final_DF,result_DF])

        result = list()
        for idx,row in final_DF.iterrows():
            #["ids"](=crossref), "name"(of the Organisation)
            doi = row["id_venue"]
            ids_list = list() # -> ["ids"](=crossref)
            for idx2,row2 in final_DF.iterrows():
                doi2 = row2["id_venue"]
                if doi2 == doi:
                    # We are in the same publication
                    id = row2["crossref"]
                    ids_list.append(id)
            name = row["name_pub"] # -> "name"(of the Organisation)
            pub_obj = Organisation(ids_list,name)
            result.append(pub_obj)
        return result
        #list[Organisation]

