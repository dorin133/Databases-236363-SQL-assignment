class Player:
    def __init__(self, playerID=None, teamID=None, age=None, height=None, foot=None):
        self.__playerID = playerID
        self.__teamID = teamID
        self.__age = age
        self.__height = height
        self.__foot = foot

    def getPlayerID(self):
        return self.__playerID

    def setPlayerID(self, playerID):
        self.__playerID = playerID

    def getTeamID(self):
        return self.__teamID

    def setTeamID(self, teamID):
        self.__teamID = teamID

    def getAge(self):
        return self.__age

    def setAge(self, age):
        self.__age = age

    def getHeight(self):
        return self.__height

    def setHeight(self, height):
        self.__height = height

    def getFoot(self):
        return self.__foot

    def setFoot(self, foot):
        self.__foot = foot

    @staticmethod
    def badPlayer():
        return Player()

    def __str__(self):
        print("PlayerID=" + str(self.__playerID) + ", TeamID=" + str(self.__teamID) + ", age=" + str(self.__age)
              + ", height=" + str(self.__height) + ", foot=" + str(self.__foot))
