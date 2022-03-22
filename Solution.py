from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Utility.DBConnector import ResultSet
from Business.Match import Match
from Business.Player import Player
from Business.Stadium import Stadium
from psycopg2 import sql

###
"""
ATTENTION ATTENTION!!
WE CHECK INPUT VALIDATION IN PYTHON WHICH IS FORBIDDEN. WE NEED TO MAKE CONSTRAINTS ON TABLES AND CHECK VIA THIS IN THE QUERY!!
DID WE CHECK FOR NULL IN INNER ATTRIBUTES IN ADD FUNCTIONS?! DO WE EVEN NEED TO??
CORRECT 'addStadium' ACCORDING TO PIAZZA ANSWERS!
SAME GOES FOR 'addMatch'!! (homeID==awayID case)

"""


def createTables():
    conn = None
    query_teams = sql.SQL("CREATE TABLE IF NOT EXISTS Teams(teamID INTEGER, PRIMARY KEY(teamID), CHECK(teamID > 0))")

    query_matches = sql.SQL("CREATE TABLE IF NOT EXISTS Matches (matchID INTEGER NOT NULL,competition VARCHAR(13) NOT NULL, homeID INTEGER NOT NULL,\
                            awayID INTEGER NOT NULL,\
                            FOREIGN KEY(homeID) REFERENCES Teams(teamID) ON DELETE CASCADE, \
                            FOREIGN KEY(awayID) REFERENCES Teams(teamID) ON DELETE CASCADE, PRIMARY KEY(matchID),\
                            CHECK(matchID > 0), CHECK(homeID <> awayID),CHECK(competition='International' or competition = 'Domestic'),\
                            CHECK(awayID>0),CHECK(homeID>0))")

    query_players = sql.SQL("CREATE TABLE IF NOT EXISTS Players(playerID INTEGER NOT NULL, teamID INTEGER NOT NULL, age INTEGER NOT NULL,\
                            height INTEGER NOT NULL, foot VARCHAR(5) NOT NULL, \
                            FOREIGN KEY (teamID) REFERENCES Teams(teamID) ON DELETE CASCADE, PRIMARY KEY(playerID), \
                            CHECK(playerID > 0),CHECK(age > 0), CHECK(height > 0), CHECK(foot='Right' or foot = 'Left'))")


    query_stadiums = sql.SQL("CREATE TABLE IF NOT EXISTS Stadiums(stadiumID INTEGER NOT NULL, capacity INTEGER NOT NULL, belongsTo INTEGER,\
                            UNIQUE(belongsTo), FOREIGN KEY (belongsTo) REFERENCES Teams(teamID) ON DELETE CASCADE, PRIMARY KEY(stadiumID),\
                            CHECK(stadiumID > 0), CHECK(capacity > 0), CHECK(belongsTo>0))")

    query_player_goals_in_match = sql.SQL("CREATE TABLE IF NOT EXISTS PlayerGoalsInMatch(playerID INTEGER NOT NULL, matchID INTEGER NOT NULL,\
                                    numberOfGoals INTEGER NOT NULL ,PRIMARY KEY(playerID, matchID),\
                                    FOREIGN KEY(playerID) REFERENCES Players(playerID) ON DELETE CASCADE,\
                                    FOREIGN KEY(matchID) REFERENCES Matches(matchID) ON DELETE CASCADE, \
                                    CHECK (numberOfGoals > 0))")

    query_attendance_in_match = sql.SQL("CREATE TABLE IF NOT EXISTS AttendanceInMatch(matchID INTEGER NOT NULL, stadiumID INTEGER NOT NULL,\
                                        attendance INTEGER NOT NULL,\
                                        PRIMARY KEY(matchID),\
                                        FOREIGN KEY(stadiumID) REFERENCES Stadiums(stadiumID) ON DELETE CASCADE,\
                                        FOREIGN KEY(matchID) REFERENCES Matches(matchID) ON DELETE CASCADE,\
                                        CHECK(attendance > -1))")

    query_goals_in_match = sql.SQL("CREATE VIEW GoalsInMatch AS\
                                    (SELECT PlayerGoalsInMatch.matchID as matchID, SUM(PlayerGoalsInMatch.numberOfGoals) as totalGoals\
                                    FROM PlayerGoalsInMatch GROUP BY PlayerGoalsInMatch.matchID)")

    query_total_goals_in_match = sql.SQL("CREATE VIEW TotalGoalsInMatch AS\
                                    (SELECT M.matchID,stadiumID,totalGoals FROM (AttendanceInMatch M INNER JOIN GoalsInMatch G ON G.matchID = M.matchID))")

    query_tall_teams = sql.SQL("CREATE VIEW TallTeams AS\
                               SELECT teamID \
                               FROM (SELECT teamID, height FROM Players) AS x \
                               GROUP BY x.teamID, x.height \
                               HAVING COUNT(x.height>190)>1")


    query_active_teams = sql.SQL("CREATE VIEW ActiveTeams AS\
                                 (SELECT teamID \
                                    FROM Teams \
                                    WHERE teamID IN (SELECT homeID FROM Matches) OR teamID IN (SELECT awayID FROM Matches))")

    query_active_tall_teams = sql.SQL("CREATE VIEW ActiveTallTeams AS\
                                      SELECT *\
                                      FROM ActiveTeams INTERSECT SELECT * FROM TallTeams")

    query_teams_with_less_40k_attendance = sql.SQL("CREATE VIEW TeamsLessAttendance AS \
                                                    SELECT M.homeID as teamID \
                                                    FROM Matches M, AttendanceInMatch AIM \
                                                    WHERE (M.matchID=AIM.matchID AND AIM.attendance <= 40000)")

    query_teams_with_less_40k_attendance_cuz_not_attendance_record = sql.SQL("CREATE VIEW TeamsLessAttendCuzNoAttendRec AS \
                                                                            SELECT M.homeID as teamID \
                                                                             FROM Matches M \
                                                                             WHERE M.matchID NOT IN (SELECT A.matchID FROM AttendanceInMatch A)")

    query_union_for_teams_with_less_40k_attend = sql.SQL("CREATE VIEW UnionTeamsLessAttend AS \
                                                          SELECT * FROM TeamsLessAttendance \
                                                          UNION \
                                                          SELECT * FROM TeamsLessAttendCuzNoAttendRec")

    query_popular_teams_non_empty_case = sql.SQL("CREATE VIEW PopularTeamsNonEmptyCase AS \
                                                SELECT M.homeID as teamID \
                                                FROM Matches M \
                                                WHERE M.homeID NOT IN \
                                                                    (SELECT teamID FROM UnionTeamsLessAttend UTLA)")

    query_popular_teams_empty_case = sql.SQL("CREATE VIEW PopularTeamsEmptyCase AS \
                                                SELECT T.teamID \
                                             FROM Teams T \
                                             WHERE T.teamID NOT IN \
                                                                (SELECT Matches.homeID FROM Matches)")

    query_attractive_stadiums_non_empty_case = sql.SQL("CREATE VIEW AttractiveStadiumsNonEmptyCase AS \
                                                            (SELECT TGIM.stadiumID as stadiumID, SUM(TGIM.totalGoals) as goalsSum \
                                                            FROM TotalGoalsInMatch TGIM \
                                                            GROUP BY TGIM.stadiumID)")

    query_attractive_stadiums_empty_case = sql.SQL("CREATE VIEW AttractiveStadiumsEmptyCase AS \
                                                        (SELECT S.stadiumID as stadiumID, COALESCE(TGIM.totalGoals, 0) as goalsSum \
                                                                FROM (Stadiums S LEFT OUTER JOIN TotalGoalsInMatch TGIM ON (S.stadiumID=TGIM.stadiumID)) \
                                                                WHERE (S.stadiumID NOT IN (SELECT T.stadiumID FROM TotalGoalsInMatch T)))")

    ## The fun part, excute the queries ##
    try:
        conn = Connector.DBConnector()
        _, _ = conn.execute(query_teams)
        _, _ = conn.execute(query_stadiums)
        _, _ = conn.execute(query_matches)
        _, _ = conn.execute(query_players)
        _, _ = conn.execute(query_player_goals_in_match)
        _, _ = conn.execute(query_goals_in_match)
        _, _ = conn.execute(query_attendance_in_match)
        _, _ = conn.execute(query_total_goals_in_match)
        _, _ = conn.execute(query_tall_teams)
        _, _ = conn.execute(query_active_teams)
        _, _ = conn.execute(query_active_tall_teams)
        _, _ = conn.execute(query_teams_with_less_40k_attendance)
        _, _ = conn.execute(query_teams_with_less_40k_attendance_cuz_not_attendance_record)
        _, _ = conn.execute(query_union_for_teams_with_less_40k_attend)
        _, _ = conn.execute(query_popular_teams_non_empty_case)
        _, _ = conn.execute(query_popular_teams_empty_case)
        _, _ = conn.execute(query_attractive_stadiums_non_empty_case)
        _, _ = conn.execute(query_attractive_stadiums_empty_case)



    finally:
        conn.close()

def dropTables():
    conn = None
    query_attractive_stadiums_non_empty_case = sql.SQL("DROP VIEW AttractiveStadiumsNonEmptyCase")
    query_attractive_stadiums_empty_case = sql.SQL("DROP VIEW AttractiveStadiumsEmptyCase")
    query_popular_teams_non_empty_case = sql.SQL("DROP VIEW PopularTeamsNonEmptyCase")
    query_popular_teams_empty_case = sql.SQL("DROP VIEW PopularTeamsEmptyCase")
    query_union_for_teams_with_less_40k_attend = sql.SQL("DROP VIEW UnionTeamsLessAttend")
    query_teams_with_less_40k_attendance_cuz_not_attendance_record = sql.SQL("DROP VIEW TeamsLessAttendCuzNoAttendRec")
    query_teams_with_less_40k_attendance = sql.SQL("DROP VIEW TeamsLessAttendance")
    query_goals_in_match = sql.SQL("DROP VIEW GoalsInMatch")
    query_total_goals_in_match = sql.SQL("DROP VIEW TotalGoalsInMatch")
    query_tall_teams = sql.SQL("DROP VIEW TallTeams")
    query_active_teams = sql.SQL("DROP VIEW ActiveTeams")
    query_active_tall_teams = sql.SQL("DROP VIEW ActiveTallTeams")
    query_attendance_in_match = sql.SQL("DROP TABLE AttendanceInMatch")
    query_player_goals_in_match = sql.SQL("DROP TABLE PlayerGoalsInMatch")
    query_matches = sql.SQL("DROP TABLE Matches")
    query_players = sql.SQL("DROP TABLE Players")
    query_stadiums = sql.SQL("DROP TABLE Stadiums")
    query_teams = sql.SQL("DROP TABLE Teams")
    try:
        conn = Connector.DBConnector()
        rows_effected, _ = conn.execute(query_attractive_stadiums_non_empty_case)
        rows_effected, _ = conn.execute(query_attractive_stadiums_empty_case)
        rows_effected, _ = conn.execute(query_popular_teams_non_empty_case)
        rows_effected, _ = conn.execute(query_popular_teams_empty_case)
        rows_effected, _ = conn.execute(query_union_for_teams_with_less_40k_attend)
        rows_effected, _ = conn.execute(query_teams_with_less_40k_attendance_cuz_not_attendance_record)
        rows_effected, _ = conn.execute(query_teams_with_less_40k_attendance)
        rows_effected, _ = conn.execute(query_total_goals_in_match)
        rows_effected, _ = conn.execute(query_goals_in_match)
        rows_effected, _ = conn.execute(query_active_tall_teams)
        rows_effected, _ = conn.execute(query_tall_teams)
        rows_effected, _ = conn.execute(query_active_teams)
        rows_effected, _ = conn.execute(query_attendance_in_match)
        rows_effected, _ = conn.execute(query_player_goals_in_match)
        rows_effected, _ = conn.execute(query_matches)
        rows_effected, _ = conn.execute(query_players)
        rows_effected, _ = conn.execute(query_stadiums)
        rows_effected, _ = conn.execute(query_teams)
    finally:
        conn.close()

def clearTables():
    conn = None
    query_teams = sql.SQL("DELETE FROM Teams")
    query_matched = sql.SQL("DELETE FROM Matches")
    query_players = sql.SQL("DELETE FROM Players")
    query_stadiums = sql.SQL("DELETE FROM Stadiums")
    query_player_goals_in_match = sql.SQL("DELETE FROM PlayerGoalsInMatch")
    query_attendance_in_match = sql.SQL("DELETE FROM AttendanceInMatch")
    try:
        conn = Connector.DBConnector()
        rows_effected, _ = conn.execute(query_teams)
        rows_effected, _ = conn.execute(query_matched)
        rows_effected, _ = conn.execute(query_players)
        rows_effected, _ = conn.execute(query_stadiums)
        rows_effected, _ = conn.execute(query_player_goals_in_match)
        rows_effected, _ = conn.execute(query_attendance_in_match)
    finally:
        conn.close()


# should we delete the prints?!?!
# we need to later on to create a Teams table, with TeamID being unique!
def addTeam(teamID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Teams(teamID) VALUES({id})").format(id =sql.Literal(teamID))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        # detected by the database
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK

# we need to later on to create a Teams table
def addMatch(match: Match) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        match_id = Match.getMatchID(match)
        competition = Match.getCompetition(match)
        home_team_id = Match.getHomeTeamID(match)
        away_team_id = Match.getAwayTeamID(match)

        query = sql.SQL("INSERT INTO Matches(matchID, competition, homeID, awayID) VALUES({id}, {comp}, {homeTeamID}, {awayTeamID})")\
            .format(id=sql.Literal(match_id), comp=sql.Literal(competition), homeTeamID=sql.Literal(home_team_id), awayTeamID=sql.Literal(away_team_id))
        rows_effected, _ = conn.execute(query)
        #if rows_effected == 0 :
        #    return ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def getMatchProfile(matchID: int) -> Match:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Matches M WHERE M.matchID = {match_ID}").format(match_ID=sql.Literal(matchID))
        rows_effected, result = conn.execute(query)
        if rows_effected!=1:
            return Match.badMatch()
    except Exception as e:
        return Match.badMatch()
    finally:
        conn.close()
    return Match(result[0]['matchID'], result[0]['competition'], result[0]['homeID'], result[0]['awayID'])

# remember to implement all tables with delete cascade whenever a matchID is a foreign key for them
def deleteMatch(match: Match) -> ReturnValue:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        matchID_to_delete = Match.getMatchID(match)
        # if doesn't work, add .format as before
        query = sql.SQL("DELETE FROM Matches M WHERE M.matchID={var}").format(var=sql.Literal(matchID_to_delete))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK

def addPlayer(player: Player) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        player_id = Player.getPlayerID(player)
        team_id = Player.getTeamID(player)
        age = Player.getAge(player)
        height = Player.getHeight(player)
        foot = Player.getFoot(player)
        query = sql.SQL(
            "INSERT INTO Players(playerID, teamID, age, height, foot) VALUES({playerID}, {teamID}, {age}, {height}, {foot})") \
            .format(playerID=sql.Literal(player_id), teamID=sql.Literal(team_id),
                    age=sql.Literal(age), height=sql.Literal(height), foot=sql.Literal(foot))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        # detected by the database
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    # if rows_effected == 0 :
    #     return ReturnValue.ALREADY_EXISTS
    return ReturnValue.OK


def getPlayerProfile(playerID: int) -> Player:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Players P WHERE P.playerID = {player_ID}").format(player_ID=sql.Literal(playerID))
        rows_effected, result = conn.execute(query)
        if rows_effected != 1:
            return Player.badPlayer()
    except Exception as e:
        return Player.badPlayer()
    finally:
        conn.close()
    return Player(result[0]['playerID'], result[0]['TeamID'], result[0]['age'], result[0]['height'], result[0]['foot'])


def deletePlayer(player: Player) -> ReturnValue:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        playerID_to_delete = Player.getPlayerID(player)
        # if doesn't work, add .format as before
        query = sql.SQL("DELETE FROM Players P WHERE P.playerID={var}").format(var=sql.Literal(playerID_to_delete))
        rows_effected, result = conn.execute(query)
        # check it's the right way to handle this exception...maybe the database catches it anyway in "Excpetion"?
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK

# Need to work according to answers in PIAZZA!!
def getStadiumProfile(stadiumID: int) -> Stadium:
    conn = None
    rows_effected, result = 0, ResultSet()
    # maybe no need here for try ?
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Stadiums S WHERE S.stadiumID = {stadium_ID}").format(stadium_ID=sql.Literal(stadiumID))
        rows_effected, result = conn.execute(query)
        if rows_effected != 1:
            return Stadium.badStadium()
    except Exception as e:
        return Stadium.badStadium()
    finally:
        conn.close()
    return Stadium(result[0]['stadiumID'], result[0]['capacity'], result[0]['belongsTo'])



def deleteStadium(stadium: Stadium) -> ReturnValue:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        stadiumID_to_delete = Stadium.getStadiumID(stadium)
        # if doesn't work, add .format as before
        query = sql.SQL("DELETE FROM Stadiums S WHERE S.stadiumID={var}").format(var=sql.Literal(stadiumID_to_delete))
        rows_effected, result = conn.execute(query)
        # check it's the right way to handle this exception...maybe the database catches it anyway in "Excpetion"?
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def addStadium(stadium: Stadium) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        stadiumID = Stadium.getStadiumID(stadium)
        capacity = Stadium.getCapacity(stadium)
        belong_to = Stadium.getBelongsTo(stadium)
        query = sql.SQL(
            "INSERT INTO Stadiums(stadiumID, capacity, belongsTo) VALUES({stadiumID}, {capacity}, {belong_to})") \
            .format(stadiumID=sql.Literal(stadiumID), capacity=sql.Literal(capacity),
                    belong_to=sql.Literal(belong_to))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # If reached here, Stadium.belong_to have a team ID which doesn't exist.
        return ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        # detected by the database.
        # is it issued for key and unique together? if so, this catch all 'already_exist' problems.
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
        # if rows_effected == 0:
        #     return ReturnValue.ERROR
    return ReturnValue.OK

### END OF CRUDE API   ###
### START OF BASIC API ###

def playerScoredInMatch(match: Match, player: Player, amount: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        player_id = Player.getPlayerID(player)
        match_id = Match.getMatchID(match)
        query = sql.SQL(
            "INSERT INTO PlayerGoalsInMatch(playerID, matchID, numberOfGoals) VALUES({player_ID}, {match_ID}, {num_goals})") \
            .format(player_ID=sql.Literal(player_id), match_ID=sql.Literal(match_id),
                    num_goals=sql.Literal(amount))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        # detected by the database
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def playerDidntScoreInMatch(match: Match, player: Player) -> ReturnValue:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        player_id = Player.getPlayerID(player)
        match_id = Match.getMatchID(match)
        query = sql.SQL("DELETE FROM PlayerGoalsInMatch PGIM WHERE PGIM.playerID={var_player} AND PGIM.matchID={var_match}")\
                .format(var_player=sql.Literal(player_id), var_match=sql.Literal(match_id))
        rows_effected, _ = conn.execute(query)
        # check it's the right way to handle this exception...maybe the database catches it anyway in "Excpetion"?
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK

def matchInStadium(match: Match, stadium: Stadium, attendance: int) -> ReturnValue:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        stadium_id = Stadium.getStadiumID(stadium)
        match_id = Match.getMatchID(match)
        query = sql.SQL("INSERT INTO AttendanceInMatch(matchID, stadiumID, attendance) VALUES({var_match}, {var_stadium}, {var_att})") \
            .format(var_match=sql.Literal(match_id), var_stadium=sql.Literal(stadium_id), var_att=sql.Literal(attendance))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        # detected by the database
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def matchNotInStadium(match: Match, stadium: Stadium) -> ReturnValue:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        match_id = Match.getMatchID(match)
        stadium_id = Stadium.getStadiumID(stadium)
        query = sql.SQL(
            "DELETE FROM AttendanceInMatch AIM WHERE AIM.matchID={var_match} AND AIM.stadiumID={var_stadium}") \
            .format(var_match=sql.Literal(match_id), var_stadium=sql.Literal(stadium_id))
        rows_effected, _ = conn.execute(query)
        # check it's the right way to handle this exception...maybe the database catches it anyway in "Excpetion"?
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK
#cant separate between division_by_zero exception to regular exception.
#In case of division by zero We return -1 and not 0.
def averageAttendanceInStadium(stadiumID: int) -> float:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT AVG(AttendanceInMatch.attendance) as avergae_attendance \
            FROM AttendanceInMatch WHERE AttendanceInMatch.stadiumID={var_stadium}").format(var_stadium=sql.Literal(stadiumID))
        rows_effected, result = conn.execute(query)
    except Exception as e:
        return -1
    finally:
        conn.close()
    if result[0]['avergae_attendance'] == None:
        return 0
    return result[0]['avergae_attendance']

def stadiumTotalGoals(stadiumID: int) -> int:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT SUM(TotalGoalsInMatch.totalGoals) as goalsInStadium \
               FROM TotalGoalsInMatch WHERE TotalGoalsInMatch.stadiumID={var_stadium}").format(var_stadium=sql.Literal(stadiumID))
        rows_effected, result = conn.execute(query)
        if result[0]['goalsInStadium'] == None:
            return 0
    except Exception as e:
        return -1
    finally:
        conn.close()
    return result[0]['goalsInStadium']

def playerIsWinner(playerID: int, matchID: int) -> bool:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT ROUND(COALESCE((1.0*(SELECT PGIM.numberOfGoals FROM PlayerGoalsInMatch PGIM \
                                           WHERE PGIM.matchID={var_PGIM_match} AND PGIM.playerID={var_player}) \
                                        /(SELECT TGIM.totalGoals FROM TotalGoalsInMatch TGIM WHERE TGIM.matchID={var_TGIM_match})), 0)) \
                       as WinnerBoolean FROM PlayerGoalsInMatch, TotalGoalsInMatch") \
                .format(var_PGIM_match=sql.Literal(matchID), var_player=sql.Literal(playerID), var_TGIM_match=sql.Literal(matchID))
        rows_effected, result = conn.execute(query)
        if rows_effected == 0:
            return 0
    except Exception as e:
        return 0
    finally:
        conn.close()
    return bool(result[0]['WinnerBoolean'])


def getActiveTallTeams() -> List[int]:
    conn = None
    rows_effected, result = 0, ResultSet()
    list = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM ActiveTallTeams ORDER BY teamID DESC LIMIT 5")
        rows_effected, result = conn.execute(query)
    finally:
        conn.close()
        for i in range(rows_effected):
            list.append(result[i]['teamID'])
        return list


def getActiveTallRichTeams() -> List[int]:
    conn = None
    rows_effected, result = 0, ResultSet()
    list = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT ATT.teamID FROM ActiveTallTeams ATT, Stadiums S \
                        WHERE S.capacity>55000 AND S.belongsTo=ATT.teamID \
                        ORDER BY ATT.teamID ASC LIMIT 5")
        rows_effected, result = conn.execute(query)
    finally:
        conn.close()
        for i in range(rows_effected):
            list.append(result[i]['teamID'])
        return list


def popularTeams() -> List[int]:
    conn = None
    rows_effected, result = 0, ResultSet()
    list = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM PopularTeamsNonEmptyCase \
                          UNION \
                          SELECT * FROM PopularTeamsEmptyCase \
                         ORDER BY teamID DESC LIMIT 10")
        rows_effected, result = conn.execute(query)
    finally:
        conn.close()
        for i in range(rows_effected):
            list.append(result[i]['teamID'])
        return list

def getMostAttractiveStadiums() -> List[int]:
    conn = None
    rows_effected, result = 0, ResultSet()
    list = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT stadiumID FROM \
                            (SELECT * \
                            FROM AttractiveStadiumsNonEmptyCase \
                            UNION \
                            SELECT * \
                            FROM AttractiveStadiumsEmptyCase \
                            ORDER BY goalsSum DESC, stadiumID ASC) AS Alias")
        rows_effected, result = conn.execute(query)
    finally:
        conn.close()
        for i in range(rows_effected):
            list.append(result[i]['stadiumID'])
        return list


def mostGoalsForTeam(teamID: int) -> List[int]:
    conn = None
    rows_effected, result = 0, ResultSet()
    list = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT P.playerID as playerID, SUM(G.numberOfGoals) as playerGoals FROM Players P, PlayerGoalsInMatch G\
                                                WHERE P.teamID = {var_team} AND P.playerID = G.playerID GROUP BY P.playerID \
                        UNION \
                        SELECT P2.playerID as playerID, COALESCE(SUM(G3.numberOfGoals),0) as playerGoals \
                            FROM Players P2 LEFT OUTER JOIN PlayerGoalsInMatch G3 ON (P2.playerID = G3.playerID) \
                        WHERE P2.playerID NOT IN (SELECT G2.playerID FROM PlayerGoalsInMatch G2) AND P2.teamID = {var_team} GROUP BY P2.playerID\
                        ORDER BY playerGoals DESC, playerID DESC LIMIT 5").format(var_team=sql.Literal(teamID))

        rows_effected, result = conn.execute(query)
    finally:
        conn.close()
        for i in range(rows_effected):
            list.append(result[i]['playerID'])
        return list

def getClosePlayers(playerID: int) -> List[int]:
    conn = None
    rows_effected, result = 0, ResultSet()
    list = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT DISTINCT M2.playerID as playerID \
                        FROM (SELECT DISTINCT PGIM2.matchID as p1 FROM playerGoalsInMatch PGIM2 WHERE PGIM2.playerID = {var_player}) M, \
                                (SELECT PGIM.playerID as playerID FROM playerGoalsInMatch PGIM, playerGoalsInMatch PGIM2 \
                                    WHERE PGIM.matchID = PGIM2.matchID AND PGIM2.playerID = {var_player} AND PGIM.playerID<>PGIM2.playerID) M2 \
                            GROUP BY M2.playerID HAVING 2*COUNT(M2.playerID)>=COUNT(M.p1) \
                        UNION \
                        (SELECT PL.playerID as playerID FROM Players PL WHERE PL.playerID <> {var_player} \
                                EXCEPT (SELECT P.playerID as empty \
                                            FROM Players P, (SELECT DISTINCT PlayerID as p FROM playerGoalsInMatch PGIM3 WHERE PGIM3.playerID = {var_player}) M3)) \
                        ORDER BY playerID ASC LIMIT 10").format(var_player=sql.Literal(playerID))
        rows_effected, result = conn.execute(query)
    finally:
        conn.close()
        for i in range(rows_effected):
            list.append(result[i]['playerID'])
        return list