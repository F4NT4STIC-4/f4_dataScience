import imp
from itertools import count
import sqlite3 as sql3
from unittest import result
from logging import raiseExceptions
from wsgiref.validate import IteratorWrapper
import pandas as pd
import json
from ast import Pass
from asyncio.windows_events import NULL
from cmath import nan
from multiprocessing import dummy
from typing import Final

from pandas import Series, DataFrame, merge
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

