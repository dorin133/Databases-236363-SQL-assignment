class Match:
    def __init__(self, matchID=None, competition=None, homeTeamID=None, awayTeamID=None):
        self.__matchID = matchID
        self.__competition = competition
        self.__homeTeamID = homeTeamID
        self.__awayTeamID = awayTeamID

    def getMatchID(self):
        return self.__matchID

    def setMatchID(self, matchID):
        self.__matchID = matchID

    def getCompetition(self):
        return self.__competition

    def setCompetition(self, competition):
        self.__competition = competition

    def getHomeTeamID(self):
        return self.__homeTeamID

    def setHomeTeamID(self, homeTeamID):
        self.__homeTeamID = homeTeamID

    def getAwayTeamID(self):
        return self.__awayTeamID

    def setAwayTeamID(self, awayTeamID):
        self.__awayTeamID = awayTeamID

    @staticmethod
    def badMatch():
        return Match()

    def __str__(self):
        print("MatchID=" + str(self.__matchID) + ", competition=" + str(self.__competition) + ", home team=" + str(
            self.__homeTeamID) + ", away team=" + str(self.__awayTeamID))
