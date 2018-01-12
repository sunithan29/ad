from variables import *
from databaseFile import *


def genUID4facultiesNstaff(record :TrimmedRecord, database: MySqlite()):

    if not record.category in ValidCategories.faculty:
        logger.error(__file__+": "+ErrWrongCat+ ":" + ','.join(record))
        raise ValueError(ErrWrongCat)

    secondarySeed = str(record.lastname).split(':')[0]
    primarySeed = str(record.firstname).split(':')[0]    
#primarySeed = str(record.lastname)
    #secondarySeed = str(record.firstname)
    # 1.1.1
    # Last name, if less than 12 characters long.
    # 12 character prefix of the last name, if last name is longer.
    uid = primarySeed[0:IDLength]
    '''if uid not in userIDs:
        userIDs.update({uid: [uid + domain, record]})
        continue'''
    if (len(uid) > 1) and database.uid_unique(uid):  # TRUE then update else continue
        logger.debug(__file__+": rule 1.1.1" + "\t" + uid + " : " + ','.join(record))
        return uid

    # 1.1.2
    # First name, if less than 12 characters long.
    # 12 character prefix of the first name, if first name is longer.
    #uid = secondarySeed[0:IDLength]
    '''if uid not in userIDs:
        userIDs.update({uid: [uid + domain, record]})'''
    #if (len(uid) > 1) and database.uid_unique(uid):  # TRUE then update else continue
    #    logger.debug(__file__+": rule 1.1.2" + "\t" + uid + " : " + ','.join(record))
    #    return uid

    # 1.1.3
    # Incrementally adding the prefix of the first name to the last name to get the smallest
    # username whose length is less or equal to than 12 characters.
    lenSseed = len(secondarySeed)
    for index in range(lenSseed):
        uid = primarySeed + secondarySeed[0:index + 1]
        uid = uid[0:IDLength]
        if database.uid_unique(uid):  # TRUE then update else continue
            # if uid not in userIDs:
            logger.debug(__file__+": rule 1.1.3" + "\t" + uid + " : " + ','.join(record))
            return uid



    # 1.1.4
    # Incrementally adding the first name to the prefix of the last name to get the smallest
    # username whose length is less or equal to than 12 characters.
    lenPseed = len(primarySeed)
    for index in range(lenPseed):
        uid = secondarySeed + primarySeed[0:index + 1]
        uid = uid[0:IDLength]
        # if uid not in userIDs:
        if database.uid_unique(uid):  # TRUE then update else continue
            logger.debug(__file__+": rule 1.1.4" + "\t" + uid + " : " + ','.join(record))
            return uid


    # 1.1.5
    # Incrementally adding the prefix of the first name to the prefix of the last name to get the
    # smallest username whose length is between 6 and 12 characters, inclusive.
    for totalLength in range(IDsmallest, IDLength + 1):
        if totalLength > (lenSseed + lenPseed):
            break
        for index in range(totalLength):
            if index > (lenSseed + lenPseed):
                break
            uid = primarySeed + secondarySeed[0:index + 1]
            uid = uid[0:totalLength]
            if database.uid_unique(uid):  # TRUE then update else continue
                logger.debug(__file__+": rule 1.1.5" + "\t" + uid + " : " + ','.join(record))
                return uid

    # 1.1.6
    # Repeat the above steps 1.1.1-5 with 11 characters by incrementally using a 1 digit
    # sequence number as suffix.
    digits = digits4students

    for digit in digits:
        for seqNo in range(0, 10 ** digit):
            # 1.1.6.1
            uid = primarySeed[0:IDLength - digit] + str(seqNo).zfill(digit)
            if database.uid_unique(uid):  # TRUE then update else continue
                # if uid not in userIDs:
                if digit == digits[0]:
                    logger.debug(__file__+": rule 1.1.6.1" + "\t" + uid + " : " + ','.join(record))
                return uid
            # 1.1.6.2
            uid = secondarySeed[0:IDLength - digit] + str(seqNo).zfill(digit)
            if database.uid_unique(uid):  # TRUE then update else continue
                # if uid not in userIDs:
                if digit == digits[0]:
                    logger.debug(__file__+": rule 1.1.6.2" + "\t" + uid + " : " + ','.join(record))
                return uid
            # 1.1.6.3
            for index in range(lenSseed):
                uid = secondarySeed[0:index + 1] + primarySeed
                uid = uid[0:IDLength - digit] + str(seqNo).zfill(digit)
                if database.uid_unique(uid):  # TRUE then update else continue
                    # if uid not in userIDs:
                    if digit == digits[0]:
                        logger.debug(__file__+": rule 1.1.6.3" + "\t" + uid + " : " + ','.join(record))
                    return uid

            # 1.1.6.4
            for index in range(lenPseed):
                uid = secondarySeed + primarySeed[0:index + 1]
                uid = uid[0:IDLength - digit] + str(seqNo).zfill(digit)
                if database.uid_unique(uid):  # TRUE then update else continue
                    # if uid not in userIDs:
                    if digit == digits[0]:
                        logger.debug(__file__+": rule 1.1.6.4" + "\t" + uid + " : " + ','.join(record))
                    return uid

            # 1.1.6.5
            for totalLength in range(IDsmallest, IDLength + 1):
                if totalLength > (lenSseed + lenPseed):
                    break
                for index in range(totalLength):
                    if index > (lenSseed + lenPseed):
                        break
                    uid = primarySeed + secondarySeed[0:index + 1]
                    uid = uid[0:totalLength - digit] + str(seqNo).zfill(digit)
                    if database.uid_unique(uid):  # TRUE then update else continue
                        # if uid not in userIDs:
                        if digit == digits[0]:
                            logger.debug(__file__+": rule 1.1.6.5" + "\t" + uid + " : " + ','.join(record))
                        return uid
    logger.error(__file__+": "+ErrUnable2gen + "\t" + ','.join(record))
    raise ValueError(ErrUnable2gen)

