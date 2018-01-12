import csv
import os
import re
from variables import *
from databaseFile import MySqlite

import faculties
import students
import contract_staff
import hashlib


def get_record(csv_fname):
    """
    A generator for the data in the csv. This is because the csv files can often contain millions of records and shouldn't be stored in memory all at once.

    :param csv_fname:
        filename/location of the csv.

    :return:
        yields each row as a namedtuple.
    """
    with open(csv_fname, "r") as file_records:
        for record in csv.reader(file_records):
            yield record


def get_hash(data):
    m = hashlib.md5()
    for each in data:
        m.update(each.encode())
    return m.hexdigest()


def processFile(csvFileName, database : MySqlite()):
    pathtofile, file = os.path.split(csvFileName)
    file, ext = os.path.splitext(file)

    successFilename = os.path.join(pathtofile, file+'_Success'+ext)
    failureFilename = os.path.join(pathtofile, file+'_Failure'+ext)
    # uidFilename = "./uidFile.csv"

    iter_record = iter(get_record(csvFileName))
    record = next(iter_record)
    recordColumns = [re.sub('[^a-z0-9]+', '', x.lower()) for x in record]
    inputFileColumns = ' '.join(recordColumns)
    print(inputFileColumns)
    InputRecord = namedtuple('InputRecord', inputFileColumns)

    if not trimmedFileColumns == recordColumns:
        logger.error(__file__ + ": Unable to Find all Required Columns in file " + csvFileName +
                     "\nColumns Required: " + trimmedColumns +
                     "\nColumns Found: " + inputFileColumns)
        print("Unable to Find all Required Columns in file " + csvFileName +
              "\nColumns Required: " + trimmedColumns +
              "\nColumns Found: " + inputFileColumns)
        exit(4)

    successWriter = csv.writer(open(successFilename, 'w', newline=''))
    successWriter.writerow(successFileColumns)

    failureWriter = csv.writer(open(failureFilename, 'w', newline=''))
    failureWriter.writerow(failureFileColumns)

    for row in iter_record:
        record = TrimmedRecord(*[x.lower().replace(' ', '') for x in row])
        # sanity check
        """
        if (len(record.firstname) < 1) and  (len(record.lastname) < 1):
            errorRecord = FailureRecord(*[record.category, record.firstname, record.lastname, record.department,
                                          "Last Name and First Name Too Small"])
            failureWriter.writerow(errorRecord)
            logger.warning(__file__ + ": Failed to process record :Last Name and First Name Too Small : " + ' '.join(record))
            continue
        """
        if ((not record.lastname) and len(record.firstname) <= 1) or ((not record.firstname) and len(record.lastname) <= 1):
            errorRecord = FailureRecord(*[record.category, record.firstname, record.lastname, record.department,
                                          "First Name or last name either one too small"])
            failureWriter.writerow(errorRecord)
            logger.warning(__file__ + ": Failed to process record :First Name or last name either one too small : " + ' '.join(record))
            continue
        
       


        uid = None
        hashCode = get_hash(record)

        resp = database.query_hash(hashCode)
        if resp:
            errorStr = ""
            for fields in resp:
                unique_id, hash_code = fields
                errorStr = errorStr + ' : %-20s %-20s' % (unique_id, hash_code)

            errorRecord = FailureRecord(*[record.category, record.firstname, record.lastname, record.department,
                                          "Duplicate Record hashcode match found"])
            failureWriter.writerow(errorRecord)
            logger.warning(__file__ + ": Hash Code match Found between records : " + ' '.join(record) + errorStr)
            print(__file__ + ": Hash Code match Found between records : " + ' '.join(record) + errorStr)
            continue

        if record.category in ValidCategories.faculty:
            # call faculty function
            try:
                uid = faculties.genUID4facultiesNstaff(record, database)
            except ValueError as e:
                if e == ErrWrongCat:
                    print(e)
                elif e == ErrUnable2gen:
                    print(e)
                else:
                    print("Error : Unknown Exception. Check log File")
                    logger.error(__file__ + ": Failed to process record : Unknown Exception: " + e + ": "+' '.join(record))
                errorRecord = FailureRecord(*[record.category, record.firstname, record.lastname, record.department,
                                              e])
                failureWriter.writerow(errorRecord)

        elif record.category in ValidCategories.student:
            # call faculty function
            try:
                uid = students.genUID4students(record, database)
            except ValueError as e:
                if e == ErrWrongCat:
                    print(e)
                elif e == ErrUnable2gen:
                    print(e)
                else:
                    print("Error : Unknown Exception. Check log File")
                    logger.error(__file__ + ": Failed to process record : Unknown Exception: " + e + ": "+' '.join(record))
                errorRecord = FailureRecord(*[record.category, record.firstname, record.lastname, record.department,
                                              e])
                failureWriter.writerow(errorRecord)

        elif record.category in ValidCategories.projectstaff:
            # call faculty function
            try:
                uid = contract_staff.genUID4projectstaff(record, database)
            except ValueError as e:
                if e == ErrWrongCat:
                    print(e)
                elif e == ErrUnable2gen:
                    print(e)
                else:
                    print("Error : Unknown Exception. Check log File")
                    logger.error(__file__ + ": Failed to process record : Unknown Exception: " + e + ": "+' '.join(record))
                errorRecord = FailureRecord(*[record.category, record.firstname, record.lastname, record.department,
                                              e])
                failureWriter.writerow(errorRecord)

        if uid:
            if database.update_table(DatabaseRecord(*[uid, hashCode])):
                database.commit_changes()
                successRecord = SuccessRecord(*[record.category, record.firstname, record.lastname,record.department,
                                                uid, hashCode])
                successWriter.writerow(successRecord)
            else:
                errorRecord = FailureRecord(*[record.category, record.firstname, record.lastname, record.department,
                                              "Error in Updating Database"])
                failureWriter.writerow(errorRecord)

    logger.debug(__file__ + ": Success List is generated in file: %s" % successFilename)
    logger.debug(__file__ + ": Failure List is generated in file: %s" % failureFilename)
