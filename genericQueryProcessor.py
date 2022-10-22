# ==== NEW GENERIC QUERY PROCESSOR (2/7) ====

import pandas as pd
import extraRelationalClasses as rel
# import extraGraphClasses as gra
import dataModelClasses as dm
from pubMemory_full import *

class GenericQueryProcessor(object):
    def __init__(self):
        self.queryProcessor = list()

    def cleanQueryProcessors(self):
        self.queryProcessor = []
        return True

    def addQueryProcessor(self,processor):
        pClass = type(processor)
        if issubclass (pClass,rel.QueryProcessor):
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
        max_doi = final_DF["ref_doi"].loc[final_DF.index[0]]
        tuple1 = tuple((max_doi,max_cit))
        max_list = list(tuple1)
        for idx,row in final_DF:
            cit = row["num_citations"]
            if cit > max:
                max = cit
            elif cit == max:
                tpl = tuple((row["ref_doi"],cit))
                max_list.append(tpl)
        
        # Now we have a list of tuples [("ref_doi","num_citations")]
        #Publication -> here we will NOT return just a single Publication (as asked by the UML), but a list[Publication] because there may be multiple venues that are all the most cited (they have the same number of citations and therefore are all at the top of the descending order of cited venues)
        result = list()

        for tpl in max_list:
            for item in tpl:
                result.append(creatPubobj(item[0]))
                
        return result
        #Publication -> here we will NOT return just a single Publication (as asked by the UML), but a list[Publication] because there may be multiple venues that are all the most cited (they have the same number of citations and therefore are all at the top of the descending order of cited venues)
        
    def getMostCitedVenue(self):
        final_DF = pd.DataFrame()
        
        for item in self.queryProcessor:
            result_DF = item.getMostCitedVenue()
            final_DF = pd.concat([final_DF,result_DF])
        
        # CHANGE THE FINAL_DF IN SUCH A WAY THAT IT HAS ONLY THE MOST CITED VENUES ***OF ALL***

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
            publ_obj = dm.Organisation(crossref_list,name_org)
            venue_obj = dm.Venue(ids_list,title,publ_obj)
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
            publ_obj = dm.Organisation(crossref_list,name_org)
            venue_obj = dm.Venue(ids_list,title,publ_obj)
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
            publ_obj = dm.Organisation(crossref_list,name_org)
            proc_obj = dm.Proceedings(ids_list,title,publ_obj,event)
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
            pers_obj = dm.Person(ids_list,given,family)
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
            pub_obj = dm.Organisation(ids_list,name)
            result.append(pub_obj)
        return result
        #list[Organisation]

'''
# ===== TEST FOR ALL THE QUERIES  

### PERONI TESt

# Once all the classes are imported, first create the relational
# database using the related source data
rel_path = "relational.db"
rel_dp = rel.RelationalDataProcessor()
rel_dp.setDbPath(rel_path)
rel_dp.uploadData("testData/relational_publications.csv")
rel_dp.uploadData("testData/relational_other_data.json")

# In the next passage, create the query processors for both
# the databases, using the related classes
rel_qp = rel.RelationalQueryProcessor()
rel_qp.setDbPath(rel_path)


# Finally, create a generic query processor for asking
# about data
generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)

# QUERIES AND METHODS
q1 = generic.getPublicationsPublishedInYear(2020)
#print("getPublicationsPublishedInYear Query\n",q1)

print("Methods for the objects of class Publication:\n")
for item in q1:
    print("ITEM")

    print("Method getIds()\n",item.getIds())
    print("Method getPublicationYear()\n",item.getPublicationYear())
    print("Method getTitle()\n",item.getTitle())
    print("Method getCitedPublications()\n",item.getCitedPublications())
    print("Method getPublicationVenue()\n",item.getPublicationVenue())
    print("Method getAuthors()\n",item.getAuthors())

q2 = generic.getPublicationsByAuthorId("0000-0001-9857-1511")
#print("getPublicationsByAuthorId Query\n",q2)

print("Methods for the objects of class Publication:\n")
for item in q2:
    print("ITEM")
    
    print("Method getIds()\n",item.getIds())
    print("Method getPublicationYear()\n",item.getPublicationYear())
    print("Method getTitle()\n",item.getTitle())
    print("Method getCitedPublications()\n",item.getCitedPublications())
    print("Method getPublicationVenue()\n",item.getPublicationVenue())
    print("Method getAuthors()\n",item.getAuthors())

q3 = generic.getMostCitedPublication()
#print("getMostCitedPublication Query\n",q3)

print("Methods for the objects of class Publication:\n")
for item in q3:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getPublicationYear()\n",item.getPublicationYear())
    print("Method getTitle()\n",item.getTitle())
    print("Method getCitedPublications()\n",item.getCitedPublications())
    print("Method getPublicationVenue()\n",item.getPublicationVenue())
    print("Method getAuthors()\n",item.getAuthors())

q4 = generic.getMostCitedVenue()
print("getMostCitedVenue Query\n",q4)

print("Methods for the objects of class Venue:\n")
for item in q4:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getTitle()\n",item.getTitle())
    print("Method getPublisher()\n",item.getPublisher())

q5 = generic.getVenuesByPublisherId("crossref:78")
print("getVenuesByPublisherId Query\n",q5)

print("Methods for the objects of class Venue:\n")
for item in q5:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getTitle()\n",item.getTitle())
    print("Method getPublisher()\n",item.getPublisher())

q6 = generic.getPublicationInVenue("issn:0944-1344")
print("getPublicationInVenue Query\n",q6)

print("Methods for the objects of class Publication:\n")
for item in q6:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getPublicationYear()\n",item.getPublicationYear())
    print("Method getTitle()\n",item.getTitle())
    print("Method getCitedPublications()\n",item.getCitedPublications())
    print("Method getPublicationVenue()\n",item.getPublicationVenue())
    print("Method getAuthors()\n",item.getAuthors())


q7 = generic.getJournalArticlesInIssue("9","17","issn:2164-5515")
print("getJournalArticleInIssue Query\n",q7)

print("Methods for the objects of class Publication:\n")
for item in q7:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getPublicationYear()\n",item.getPublicationYear())
    print("Method getTitle()\n",item.getTitle())
    print("Method getCitedPublications()\n",item.getCitedPublications())
    print("Method getPublicationVenue()\n",item.getPublicationVenue())
    print("Method getAuthors()\n",item.getAuthors())


q8 = generic.getJournalArticlesInVolume("17","issn:2164-5515")
print("getJournalArticleInVolume Query\n",q8)

print("Methods for the objects of class Publication:\n")
for item in q8:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getPublicationYear()\n",item.getPublicationYear())
    print("Method getTitle()\n",item.getTitle())
    print("Method getCitedPublications()\n",item.getCitedPublications())
    print("Method getPublicationVenue()\n",item.getPublicationVenue())
    print("Method getAuthors()\n",item.getAuthors())

q9 = generic.getJournalArticlesInJournal("issn:2164-5515")
print("getJournalArticleInJournal Query\n",q9)

print("Methods for the objects of class Publication:\n")
for item in q9:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getPublicationYear()\n",item.getPublicationYear())
    print("Method getTitle()\n",item.getTitle())
    print("Method getCitedPublications()\n",item.getCitedPublications())
    print("Method getPublicationVenue()\n",item.getPublicationVenue())
    print("Method getAuthors()\n",item.getAuthors())

q10 = generic.getProceedingsByEvent("web")
print("getProceedingsByEvent Query\n",q10)

print("Methods for the objects of class Proceedings:\n")
for item in q10:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getTitle()\n",item.getTitle())
    print("Method getPublisher()\n",item.getPublisher())
    print("Method getEvent()\n",item.getEvent())

q11 = generic.getPublicationAuthors("doi:10.1080/21645515.2021.1910000")
print("getPublicationAuthors Query\n",q11)

print("Methods for the objects of class Person:\n")
for item in q11:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getGivenName()\n",item.getGivenName())
    print("Method getFamilyName()\n",item.getFamilyName())

q12 = generic.getPublicationsByAuthorName("sil")
print("getPublicationsByAuthorName Query\n",q12)

print("Methods for the objects of class Publication:\n")
for item in q12:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getPublicationYear()\n",item.getPublicationYear())
    print("Method getTitle()\n",item.getTitle())
    print("Method getCitedPublications()\n",item.getCitedPublications())
    print("Method getPublicationVenue()\n",item.getPublicationVenue())
    print("Method getAuthors()\n",item.getAuthors())

q13 = generic.getDistinctPublishersOfPublications(["doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035"])
print("getDistinctPublisherOfPublications Query\n",q13)

print("Methods for the objects of class Organisation:\n")
for item in q13:
    print("ITEM")
    print("Method getIds()\n",item.getIds())
    print("Method getName()\n",item.getName())

'''