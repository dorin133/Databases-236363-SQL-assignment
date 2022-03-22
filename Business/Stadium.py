class Stadium:
    def __init__(self, stadiumID=None, capacity=None, belongsTo=None):
        self.__stadiumID = stadiumID
        self.__capacity = capacity
        self.__belongsTo = belongsTo

    def getStadiumID(self):
        return self.__stadiumID

    def setStadiumID(self, stadiumID):
        self.__stadiumID = stadiumID

    def getCapacity(self):
        return self.__capacity

    def setCapacity(self, capacity):
        self.__capacity = capacity

    def getBelongsTo(self):
        return self.__belongsTo

    def setBelongsTo(self, belongsTo):
        self.__belongsTo = belongsTo

    @staticmethod
    def badStadium():
        return Stadium()

    def __str__(self):
        print("stadiumID=" + str(self.__stadiumID) + ", capacity=" + str(self.__capacity) + ", belongs to=" + str(
            self.__belongsTo))
