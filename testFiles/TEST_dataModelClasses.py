# ===== THIS FILE CONTAINS TESTS =====

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


# ===== LET'S TRY CREATE SOME INSTANCES (= OBJECTS!) =====

id_entity = IdentifiableEntity(["id1-0001-0001","id2-0002-0002"])

autore = Person(["orcid-0001"],"Orsola Maria","Borrini")
autore2 = Person(["orcid-0002"],"Mario","Virdis")
organizzazione = Organisation(["crossref-2044"],"Organizzazione di Orsola")
venue_1 = Venue(["id-venue-1-0001","id-venue-2-0002"],"Titolo Venue Prova",organizzazione)
pub_citata = Publication(["id-pub-4000"],2019,"Titolo Pubblicazione Citata",[autore2],venue_1,[""])
pubblicazione = Publication(["id-pub-1000","id-pub-2000"],2020,"Titolo Pubblicazione Prova",[autore,autore2],venue_1,[pub_citata])

giornale = Journal(["id-j-0001"],"Giornale Titolo",organizzazione)
articolo = JournalArticle(["id-ja-0001","id-ja-0002"],2020,"Articolo Giornale Titolo",[autore],giornale,[""],"1","2")

libro = Book(["id-b-0001","id-b-0002"],"Libro Titolo",organizzazione)
capitolo = BookChapter(["id-capitolo-0001"],2018,"Capitolo Titolo",[autore2],venue_1,[pubblicazione],1)

processione = Proceedings(["id-proce-0001"],"Titolo Processione",organizzazione,"Nome Evento") 
procepapera = ProceedingsPaper(["id-pro-pa-0001"],2020,"Titolo Proceedings Paper",[autore],processione,[""])


print("\nThese are all the methods for the IdentifiableEntity class")
print("getIds()",id_entity.getIds())

print("\nThese are all the methods for the Person class")
print("getIds()",autore.getIds())
print("getGivenName()",autore.getGivenName())
print("getFamilyName()",autore.getFamilyName())

print("\nThese are all the methods for the Organisation class")
print("getIds()",organizzazione.getIds())
print("getName()",organizzazione.getName())

print("\nThese are all the methods for the Venue class")
print(venue_1)
print("getIds()",venue_1.getIds())
print("getPublisher()",venue_1.getPublisher())
print("getTitle()",venue_1.getTitle())

print("\nThese are all the methods for the Publication class")
print("getIds()",pubblicazione.getIds())
print("getPublicationYear()",pubblicazione.getPublicationYear())
print("getTitle()",pubblicazione.getTitle())
print("getCitedPublications()",pubblicazione.getCitedPublications())
print("getPublicationVenue()",pubblicazione.getPublicationVenue())
print("getAuthors()",pubblicazione.getAuthors())

print("\nThese are all the methods for the JournalArticle class")
print("getIds()",articolo.getIds())
print("getPublicationYear()",articolo.getPublicationYear())
print("getTitle()",articolo.getTitle())
print("getCitedPublications()",articolo.getCitedPublications())
print("getPublicationVenue()",articolo.getPublicationVenue())
print("getAuthors()",articolo.getAuthors())
print("getIssue()",articolo.getIssue())
print("getVolume()",articolo.getVolume())

print("\nThese are all the methods for the BookChapter class")
print("getIds()",capitolo.getIds())
print("getPublicationYear()",capitolo.getPublicationYear())
print("getTitle()",capitolo.getTitle())
print("getCitedPublications()",capitolo.getCitedPublications())
print("getPublicationVenue()",capitolo.getPublicationVenue())
print("getAuthors()",capitolo.getAuthors())
print("getChapterNumber()",capitolo.getChapterNumber())

print("\nThese are all the methods for the ProceedingsPaper class")
print("getIds()",procepapera.getIds())
print("getPublicationYear()",procepapera.getPublicationYear())
print("getTitle()",procepapera.getTitle())
print("getCitedPublications()",procepapera.getCitedPublications())
print("getPublicationVenue()",procepapera.getPublicationVenue())
print("getAuthors()",procepapera.getAuthors())

print("\nThese are all the methods for the Journal class")
print("getIds()",giornale.getIds())
print("getPublisher()",giornale.getPublisher())
print("getTitle()",giornale.getTitle())

print("\nThese are all the methods for the Book class")
print("getIds()",libro.getIds())
print("getPublisher()",libro.getPublisher())
print("getTitle()",libro.getTitle())

print("\nThese are all the methods for the Proceedings class")
print("getIds()",processione.getIds())
print("getPublisher()",processione.getPublisher())
print("getTitle()",processione.getTitle())
print("getEvent()",processione.getEvent())