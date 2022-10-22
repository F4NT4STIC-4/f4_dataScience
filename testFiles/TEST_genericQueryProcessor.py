import extraRelationalClasses as rel
import extraGraphClasses as grf
from genericQueryProcessor import GenericQueryProcessor
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

# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
grp_dp = grf.TriplestoreDataProcessor()
grp_dp.setEndpointUrl(grp_endpoint)
grp_dp.uploadData("testData/graph_publications.csv")
grp_dp.uploadData("testData/graph_other_data.json")


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

