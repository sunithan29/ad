import csv
import hashlib
import re
import os
# from variables import *
from collections import namedtuple

def get_hash(data):
    m = hashlib.md5()
    for each in data:
        m.update(each.encode())
    return m.hexdigest()

genFolder = "genResults"

requiredHashColumns = 'category firstname lastname department uid'
requiredHashFileColumns = requiredHashColumns.split()
RequiredHashRecord = namedtuple('RequiredHashRecord', requiredHashColumns)

outHashColumns = 'category firstname lastname department uid hashcode'
outHashFileColumns = outHashColumns.split()
outHashRecord = namedtuple('outHashRecord', outHashColumns)



def get_filtered_record(csv_fname):
    """
    A generator for the data in the csv. This is because the csv files can often contain millions of records and 
    shouldn't be stored in memory all at once.

    :param csv_fname:
        filename/location of the csv.

    :return:
        yields each row converted to lower case and removes any characters other than alphanumerics.
    """
    with open(csv_fname, "r") as student_records:
        for student_record in csv.reader(student_records):
            converted = [re.sub('[^a-z0-9]+', '', x.lower()) for x in student_record]
            yield converted


def trimmer(fileNames=[]):
    """
    This function takes a list of filenames as argument and segregates the records in those files into Three files
    based on the category the record belongs to.
    :param fileNames: List of csv files
    :return: List of Three files containing the segregated records.
    """

    if not fileNames:
        return None
    pathtofile, file = os.path.split(fileNames[0])

    """
    The files will be created in the folder whose name is stored in the variable 'genFolder'
    """
    destDir = os.path.join(pathtofile, genFolder)
    if not os.path.exists(destDir):
        os.mkdir(destDir)

    """
    Create the three files and open them in write mode
    """
    facultyFilename = os.path.join(destDir, "Faculty_Hash_File.csv")

    facultyWriter = csv.writer(open(facultyFilename, 'w', newline=''))
    facultyWriter.writerow(outHashFileColumns)

    for eachfile in fileNames:
        iter_record = iter(get_filtered_record(eachfile))

        """
        Read the first row. It contains only the column names.
        Column names should not contain any special characters.
        """
        inputFileColumns = next(iter_record)
        str_inputFileColumns = ' '.join(inputFileColumns)

        if not set(requiredHashFileColumns).issubset(inputFileColumns):
            print("Unable to Find all Required Columns in file " + eachfile +
                  "\nColumns Required: " + requiredHashColumns +
                  "\nColumns Found: " + str_inputFileColumns)
            continue

        """
        1. Get the indexes of required data.
        2. Save them in a named tuple that can give the index based on column name.
        3. 'recordFileColumns' contains the list of strings that are the names of the required columns for processing.
        4. Get the Index of these columns in the current file and save them in a named tuple.
        """
        indexes = [inputFileColumns.index(i) for i in requiredHashFileColumns]
        recIndex = RequiredHashRecord(*indexes)

        for row in iter_record:

            hashlist = [row[recIndex.category], row[recIndex.firstname], row[recIndex.lastname], row[recIndex.department]]
            hashcode = get_hash(hashlist)
            writeRowContent = [row[recIndex.category], row[recIndex.firstname], row[recIndex.lastname], row[recIndex.department], row[recIndex.uid], hashcode]

            facultyWriter.writerow(writeRowContent)

    return [facultyFilename]


if __name__ == '__main__':
    trimmer(["hashcode_staff_mrc.csv"])
