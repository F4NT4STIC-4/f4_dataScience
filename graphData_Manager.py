from rdflib import Graph, URIRef, Literal, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get

import pandas as pd
import json

from ModelClasses import QueryProcessor
from auxiliary import readCSV, readJSON

# Establishing class object URIs 
JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
BookChapter = URIRef("https://schema.org/Chapter")
ProceedingsPaper = URIRef("http://purl.org/spar/fabio/ProceedingsPaper")

Journal = URIRef("https://schema.org/Periodical")
Book = URIRef("https://schema.org/Book")
Proceedings = URIRef("https://schema.org/EventSeries")

publicationVenue = URIRef("https://schema.org/isPartOf")

publisher = URIRef("https://schema.org/publisher")


id = URIRef("https://schema.org/identifier") # Used for every identifier

# Attributes related to classes
publicationYear = URIRef("https://dbpedia.org/ontology/year")
title = URIRef("https://schema.org/name")
issue = URIRef("https://schema.org/issueNumber")
volume = URIRef("https://schema.org/volumeNumber")
chapter_num = URIRef("https://schema.org/numberedPosition")
event = URIRef("https://schema.org/event")


author = URIRef("https://schema.org/author")
name = URIRef("https://schema.org/givenName")
surname = URIRef("https://schema.org/familyName")
citation = URIRef("https://schema.org/citation")


class TriplestoreProcessor(object):
    def __init__(self):
        # 'db_path' is name we use for the database path 
        self.endpointURL = ""

    def getEndpointURL(self):
        if self.endpointURL == "":
            return "endpointURL is currently unset" + self.endpointURL

    def setEndpointURL(self, new_endpointURL):
        if new_endpointURL is str:
            self.endpointURL = new_endpointURL
            return True
        else:
            return False
        
# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self):
        super().__init__()

    def uploadData(self, filepath):
        # Step-1 : read the data into pandas
        if filepath.endswith(".csv"):
            #df1 -> journal article         // columns = 'id', 'title', 'type', 'publication_year', 'issue', 'volume'
            #df2 -> book-chapter            // columns = 'id', 'title', 'type', 'publication_year', 'chapter'
            #df3 -> proceedings-paper       // columns = 'id', 'title', 'type', 'publication_year'
            #df4 -> Venue_book              // columns = 'id', 'publication_venue', 'venue_type', 'publisher'
            #df5 -> Venue_journal           // columns = 'id', 'publication_venue', 'venue_type', 'publisher'
            #df6 -> Venue_proceedings-event // columns = 'id', 'publication_venue', 'venue_type', 'publisher', 'event
            df1,df2,df3,df4,df5,df6 = readCSV(filepath)
    
        if filepath.endswith(".json"):
            #df7  -> authors                // columns = 
            #df8  -> citations              // columns = 
            #df9  -> publishers             // columns = 
            #df10 -> VenueIDs               // columns = 
            df7,df8,df9,df10 = readJSON(filepath)

        # Step-2 : iterate over the data to create triples
            # Avoid repetition
            #2.1 : store triples for publishers
            #2.2 : store triples for venues
            #2.3 : store triples for authors
            #2.4 : store triples for JA 
            #2.5 : store triples for BC 
            #2.6 : store triples for PP

        # Step-3 : open the connection to the DB and push the triples.

        return True
    
# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

class TriplestoreQueryProcessor(QueryProcessor,TriplestoreProcessor):
    def __init__(self):
        super().__init__()

    def getPublicationsPublishedInYear(self, year):
        QR_1 = pd.DataFrame()
        print("don the things here")
        return QR_1

    def getPublicationsByAuthorId(self, orcid):
        QR_2 = pd.DataFrame()
        print("don the things here")
        return QR_2
    
    def getMostCitedPublication(self):
        QR_3 = pd.DataFrame()
        print("don the things here")
        return QR_3
    
    def getMostCitedVenue(self):
        QR_4 = pd.DataFrame()
        print("don the things here")
        return QR_4
    
    def getVenuesByPublisherId(self, crossref):
        QR_5 = pd.DataFrame()
        print("don the things here")
        return QR_5
    
    def getPublicationInVenue(self, issn_isbn):
        QR_6 = pd.DataFrame()
        print("don the things here")
        return QR_6
    
    def getJournalArticlesInIssue(self, issue, volume, ja_id):
        QR_7 = pd.DataFrame()
        print("don the things here")
        return QR_7
    
    def getJournalArticlesInVolume(self, volume, ja_id):
        QR_8 = pd.DataFrame()
        print("don the things here")
        return QR_8
    
    def getJournalArticlesInJournal(self, ja_id):
        QR_9 = pd.DataFrame()
        print("don the things here")
        return QR_9
    
    def getProceedingsByEvent(self, eventName):
        QR_10 = pd.DataFrame()
        print("don the things here")
        return QR_10
    
    def getPublicationAuthors(self, doi):
        QR_11 = pd.DataFrame()
        print("don the things here")
        return QR_11
    
    def getPublicationsByAuthorName(self, authorName):
        QR_12 = pd.DataFrame()
        print("don the things here")
        return QR_12
    
    def getDistinctPublisherOfPublications(self, doi_list):
        QR_13 = pd.DataFrame()
        print("don the things here")
        return QR_13

# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

# TEST AREA
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
grp_dp = TriplestoreDataProcessor()
grp_dp.setEndpointUrl(grp_endpoint)
grp_dp.uploadData("data/graph_publications.csv")
grp_dp.uploadData("data/graph_other_data.json")

# Checking the superclass is correct or not
print(grp_dp.__bases__)

grp_qp = TriplestoreQueryProcessor()
grp_qp.setEndpointUrl(grp_endpoint)

# Checking the superclass is correct or not
print(grp_qp.__bases__)