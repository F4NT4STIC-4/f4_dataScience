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
