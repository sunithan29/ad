#! /usr/bin/python3
import csv
import re
import os
from variables import *


def get_student_record(csv_fname):
    """
    A generator for the data in the csv. This is because the csv files can often contain millions of records and shouldn't be stored in memory all at once.

    :param csv_fname:
        filename/location of the csv.

    :return:
        yields each row as a namedtuple.
    """
    with open(csv_fname, "r") as student_records:
        for student_record in csv.reader(student_records):
            converted = [re.sub('[^a-z0-9 ]+', '', x.lower().strip()) for x in student_record]
            yield converted


def trimmer(fileNames=[]):
    if not fileNames:
        return None
    pathtofile, file = os.path.split(fileNames[0])

    destDir = os.path.join(pathtofile, genFolder)
    if not os.path.exists(destDir):
        os.mkdir(destDir)

    facultyFilename = os.path.join(destDir, "Faculty_Staff.csv")
    studentFilename = os.path.join(destDir, "Students.csv")
    csFilename = os.path.join(destDir, "Project_Staff.csv")

    facultyWriter = csv.writer(open(facultyFilename, 'w', newline=''))
    facultyWriter.writerow(trimmedFileColumns)

    studentWriter = csv.writer(open(studentFilename, 'w', newline=''))
    studentWriter.writerow(trimmedFileColumns)

    csWriter = csv.writer(open(csFilename, 'w', newline=''))
    csWriter.writerow(trimmedFileColumns)

    for eachfile in fileNames:
        iter_record = iter(get_student_record(eachfile))
        record = next(iter_record)
        recordColumns = [re.sub('[^a-z0-9]+', '', x.lower()) for x in record]
        inputFileColumns = ' '.join(recordColumns)
        InputRecord = namedtuple('InputRecord', inputFileColumns)
        print(inputFileColumns)

        if not set(trimmedFileColumns).issubset(recordColumns):
            logger.warning(__file__ + ": Unable to Find all Required Columns in file " + eachfile +
                         "\nColumns Required: " + trimmedColumns +
                         "\nColumns Found: " + inputFileColumns)
            print("Unable to Find all Required Columns in file " + eachfile +
                  "\nColumns Required: " + trimmedFileColumns +
                  "\nColumns Found: " + inputFileColumns)
            continue

        for row in iter_record:
            record = InputRecord(*row)
            category = record.category.replace(' ', '')
            writeRowContent = [record.category.replace(' ', ''), record.firstname.replace(' ', ':'), record.lastname.replace(' ', ':'), record.department.replace(' ', '')]

            if category in ValidCategories.faculty:
                facultyWriter.writerow(writeRowContent)
            elif category in ValidCategories.student:
                studentWriter.writerow(writeRowContent)
            elif category in ValidCategories.projectstaff:
                csWriter.writerow(writeRowContent)
            else:
                logger.warning(__file__ + ": Unknown Category found in : " + ' '.join(record))
                print(__file__ + "Unknown Category found in : " + ' '.join(record))

    return [facultyFilename, studentFilename, csFilename]


def adTrimmer(fileName):
    pathtofile, file = os.path.split(fileName)
    file, ext = os.path.splitext(file)
    writeFilename = os.path.join(pathtofile, file+'_Trimmed'+ext)
    print(writeFilename)
    # uidFilename = "./uidFile.csv"

    iter_record = iter(get_student_record(fileName))
    record = next(iter_record)
    recordColumns = [re.sub('[^a-z]+', '', x.lower()) for x in record]
    inputFileColumns = ' '.join(recordColumns)
    InputRecord = namedtuple('InputRecord', inputFileColumns)

    if not set(adTrimmedFileColumns).issubset(recordColumns):
        logger.error(__file__ + ": Unable to Find all Required Columns in file " + fileName +
                       "\nColumns Required: " + adTrimmedElements +
                       "\nColumns Found: " + inputFileColumns)
        print("Unable to Find all Required Columns in file " + fileName +
              "\nColumns Required: " + adTrimmedElements +
              "\nColumns Found: " + inputFileColumns)
        return None

    csvWriter = csv.writer(open(writeFilename, 'w', newline=''))
    csvWriter.writerow(adTrimmedFileColumns)

    for row in iter_record:
        record = InputRecord(*row)
        writeRowContent = [record.uid, record.hashcode]
        csvWriter.writerow(writeRowContent)
    return writeFilename
"""
if __name__ == '__main__':
    readFilename = ["StudentData_new.csv"]
    trimmer(readFilename)
"""
