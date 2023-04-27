import pandas as pd
import sqlite3 as sql3
import json 

from ModelClasses import QueryProcessor
from auxiliary import readCSV, readJSON

class RelationalProcessor(object):
    def __init__(self):
        # 'db_path' is name we use for the database path 
        self.db_path = ""

    def getDbpath(self):
        if self.db_path == "":
            return "DbPath is currently unset" + self.db_path
        else:
            return self.db_path
        
    def setDbpath(self, new_db_path):
        if new_db_path is str:
            self.db_path = new_db_path
            return True
        else:
            return False
    
# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––
    
class RelationalDataProcessor(RelationalProcessor):
    def __init__(self):
        super().__init__()

    def uploadData(self, filepath):
        # Step-1 : read the data into pandas
        if filepath.endswith(".csv"):
            dataDF = readCSV(filepath)
        elif filepath.endswith(".json"):
            dataDF2 = readJSON(filepath)

        # Step-2 : create tables from the above data
            # Avoid repetition

        # Step-3 : open the connection to the DB and push the tables.

        return True
    
# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

class RelationalQueryProcessor(QueryProcessor,RelationalProcessor):
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
    
    def getJournalArticlesInIssue(self, issue, volume, jo_id):
        QR_7 = pd.DataFrame()
        print("don the things here")
        return QR_7
    
    def getJournalArticlesInVolume(self, volume, jo_id):
        QR_8 = pd.DataFrame()
        print("don the things here")
        return QR_8
    
    def getJournalArticlesInJournal(self, jo_id):
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
rel_path = "relational.db"
rel_dp = RelationalDataProcessor()
rel_dp.setDbPath(rel_path)
rel_dp.uploadData("data/relational_publications.csv")
rel_dp.uploadData("data/relational_other_data.json")

# Checking the superclass is correct or not
print(rel_dp.__bases__)

rel_qp = RelationalQueryProcessor()
rel_qp.setDbPath(rel_path)

# Checking the superclass is correct or not
print(rel_qp.__bases__)