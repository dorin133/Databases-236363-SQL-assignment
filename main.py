from Solution import *

if __name__ == '__main__':
    dropTables()
    print("--------- SET UP - START ---------")
    createTables()
    print("ADDING TEAMS")
    teams = [
        1, 2, 3, 4, 5, 6,
        # Illegal entries
        0, 6
    ]
    for team in teams:
        print(addTeam(team))

    print("ADDING MATCHES")
    matches = [
        Match(1, 'Domestic', 2, 3),
        Match(2, 'International', 3, 4),
        Match(3, 'International', 3, 2),
        Match(4, 'Domestic', 2, 4),
        Match(5, 'Domestic', 4, 5),
        Match(6, 'International', 5, 6),
        Match(7, 'International', 1, 6),
        # Illegal entries
        Match(100, 'Boop', 1, 2),
        Match(1, 'Domestic', 2, 3),
        Match(1, 'International', 4, 5),
        Match(101, 'Domestic', 2, 2),
        Match(None, 'International', 1, 2),
        Match(100, None, 1, 2),
        Match(100, 'International', None, 2),
        Match(100, 'International', 1, None),
    ]
    for match in matches:
        print(addMatch(match))

    print("ADDING PLAYERS")
    players = [
        Player(1, 1, 10, 150, "Left"),
        Player(2, 3, 10, 150, "Left"),
        Player(3, 2, 10, 150, "Right"),
        Player(4, 4, 10, 200, "Left"),
        Player(5, 4, 10, 200, "Left"),
        Player(6, 5, 10, 200, "Left"),
        Player(7, 5, 10, 200, "Left"),
        # Illegal entries
        Player(100, 1, 10, 150, "Both"),
        Player(101, 1, -1, 150, "Left"),
        Player(None, 1, 10, 150, "Left"),
        Player(101, None, 10, 150, "Left"),
        Player(101, 1, None, 150, "Left"),
        Player(101, 1, 10, None, "Left"),
        Player(101, 1, 10, 150, None),
    ]
    for player in players:
        print(addPlayer(player))

    print("ADDING STADIUMS")
    stadiums = [
        Stadium(1, 1, 1),
        Stadium(2, 2, 2),
        Stadium(5, 5, None),
        Stadium(6, 60000, 5),
        Stadium(7, 60000, 4),
        # Illegal entries
        Stadium(3, 3, 1),
        Stadium(4, 4, 2),
        Stadium(1, 5, 5),
    ]

    for stadium in stadiums:
        print(addStadium(stadium))

    print("GETTING MATCH PROFILES")
    for match in matches:
        getMatchProfile(match.getMatchID()).__str__()

    print("GETTING PLAYER PROFILES")
    for player in players:
        getPlayerProfile(player.getPlayerID()).__str__()

    print("GETTING STADIUM PROFILES")
    for stadium in stadiums:
        getStadiumProfile(stadium.getStadiumID()).__str__()

    print("--------- API TESTING - START ---------")

    print("--------- SET UP - END ---------")

    print("ADDING PLAYER SCORES")
    player_scores = [
        (matches[0], players[2], 5),
        (matches[0], players[1], 3),
        (matches[5], players[5], 20),
        # Illegal entries
        (matches[0], players[1], -3)
    ]
    for score in player_scores:
        print(playerScoredInMatch(*score))

    print("ADDING MATCH IN STADIUMS")
    match_in_stadiums = [
        (matches[0], stadiums[0], 1),
        (matches[1], stadiums[1], 2),
        (matches[2], stadiums[2], 3),
        (matches[5], stadiums[3], 50000),
        (matches[6], stadiums[4], 50000),
        # Illegal entries
        (matches[3], Stadium(3, 3, 3), -5),
        (matches[2], stadiums[2], 3),
    ]
    for match_in_stadium in match_in_stadiums:
        print(matchInStadium(*match_in_stadium))

    print("GETTING AVERAGE ATTENDANCE IN STADIUMS")
    for stadium in stadiums:
        stid = stadium.getStadiumID()
        print(f"AVERAGE ATTENDANCE IN STADIUM {stid}: {averageAttendanceInStadium(stid)}")

    # print("GETTING TOTAL GOALS IN STADIUMS")
    # for stadium in stadiums:
    #     stid = stadium.getStadiumID()
    #     print(f"TOTAL GOALS IN STADIUM {stid}: {stadiumTotalGoals(stid)}")
    #
    # print("CHECKING FOR WINNERS")
    # for player in players:
    #     for match in matches:
    #         pID = player.getPlayerID()
    #         mID = match.getMatchID()
    #         print(f"Player {pID} in Match {mID} is a winner: {playerIsWinner(pID, mID)}")
    #
    # print("GETTING TOP 5 TALL ACTIVE TEAMS")
    # print(getActiveTallTeams())
    #
    # print("GETTING TOP 5 RICHEST TALL ACTIVE TEAMS")
    # print(getActiveTallRichTeams())
    #
    # print("GETTING TOP 10 MOST POPULAR TEAMS")
    # print(popularTeams())
    #
    # print("GETTING STADIUMS ORDERED BY ATTRACTIVENESS")
    # print(getMostAttractiveStadiums())
    #
    print("GETTING MOST GOALS PLAYER LIST PER TEAM")
    for team in teams:
        print(f"Most goals list for team {team}: {mostGoalsForTeam(team)}")

    print("GETTING MOST CLOSE PLAYERS PER PLAYER")
    for player in players:
        pID = player.getPlayerID()
        # Ignore unhandled cases (by Piazza)
        if not pID or pID >= 100:
            continue
        print(f"Most close players for player {pID}: {getClosePlayers(pID)}")
    # print("REMOVING PLAYER SCORES")
    # for score in player_scores:
    #     print(playerDidntScoreInMatch(score[0], score[1]))
    #
    # print("REMOVING MATCHES IN STADIUMS")
    # for match_in_stadium in match_in_stadiums:
    #     print(matchNotInStadium(match_in_stadium[0], match_in_stadium[1]))

    print("--------- API TESTING - END ---------")

    print("--------- TEAR DOWN - START ---------")
    # print("DELETING MATCHES")
    # for match in matches:
    #     print(deleteMatch(match))
    #
    # print("DELETING PLAYERS")
    # for player in players:
    #     print(deletePlayer(player))
    #
    # print("DELETING STADIUM")
    # for stadium in stadiums:
    #     print(deleteStadium(stadium))
    # dropTables()
    print("--------- TEAR DOWN - END ---------")