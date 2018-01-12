#! /usr/bin/python3
import sys
import re
from databaseFile import MySqlite
from csvProcessing import *
from trimmer import trimmer, adTrimmer
import os


def get_filepaths(directory, filter=''):
    file_paths = []  # List which will store all of the full filepaths.
    # Walk the tree.
    for root, directories, files in os.walk(directory):
        if root == directory:
            for filename in files:
                if filename.endswith(filter+'.csv'):
                    # Join the two strings in order to form the full filepath.
                    # filepath = os.path.join(root, filename)
                    filepath = os.path.join(root, filename)
                    filepath = os.path.abspath(filepath)
                    file_paths.append(filepath)  # Add it to the list.
    return file_paths


def printUsage():
    line = "Usage:\n ./main.py [OPTIONS]... DIRECTORY/CSVFILE" \
           "\n\nProcess DIRECTORY/CSVFILE and generate unique user ids." \
           "\nMandatory Options:" \
           "\n\n--add <DIRECTORY/CSVFILE> " \
           "\t Adds the content of CSV files present in <DIRECTORY>" \
           "\n\t\t\t\t or specified by <CSVFILE> into MySQL Database."\
           "\n\n--del <DIRECTORY/CSVFILE> " \
           "\t Deletes the already created user ids present in <DIRECTORY>" \
           "\n\t\t\t\t or specified by <CSVFILE> from MySQL Database."
    print(line)

if __name__ == '__main__':
    numArguments = 1+2  # Number of command line arguments including Name of the Script.
    action = 1      # Command Line argumemt 1 (add/delete)
    filename = 2    # Command Line argumemt 2 (File Name)

    addArgName = "--add"
    help = "--help"
    actions = [addArgName, help]
    csvFiles = []

    try:
        if sys.argv[action] == help:
            printUsage()
            exit(0)
    except IndexError:
        print("Missing arguments...\nTry ./main.py --help for help menu.")
        exit(0)

    if len(sys.argv) < numArguments:
        print("Insufficient Arguments...\nTry ./main.py --help for help menu.")
        exit(1)

    if sys.argv[action] not in actions:
        print("./main.py: unrecognized option ", sys.argv[action], "\nTry ./main.py --help for help menu.")
        exit(1)

    if not os.path.exists(sys.argv[filename]):
        print("./main.py: `", sys.argv[filename], "`: No such file or directory",
              "\nTry ./main.py --help for help menu.")
    #    printUsage()
        exit(1)

    if os.path.isfile(sys.argv[filename]):
        for element in sys.argv:
            if element.endswith('.csv'):
                if element not in csvFiles:
                    csvFiles.append(os.path.realpath(element))

    if os.path.isdir(sys.argv[filename]):
        csvFiles = get_filepaths(sys.argv[filename])

    db_filename = 'uidDatabase.db'
    adFilename = "./adFile.csv"

    if not os.path.exists(adFilename):
        print("AD file:\'adFile.csv\' doesnot exist in current directory.")
        logger.error(__file__ + ": AD file:\'adFile.csv\' doesnot exist in current directory.")
        exit(1)

    uidFilename = adTrimmer(adFilename)

    if not uidFilename:
        logger.error(__file__ + ": AD file not compatible")
        exit(2)

    database = MySqlite()

    iter_record = iter(get_record(uidFilename))
    record = next(iter_record)
    recordColumns = [re.sub('[^a-z0-9]+', '', x.lower()) for x in record]
    inputFileColumns = ' '.join(recordColumns)
    InputRecord = namedtuple('InputRecord', inputFileColumns)
    db_is_new = not os.path.exists(db_filename)
    database.create_connection(db_filename)

    if not set(databaseTableColumns).issubset(recordColumns):
        logger.error(__file__ + ": Unable to Find all Required Columns in file " + uidFilename +
                     "\nColumns Required: " + databaseColumns +
                     "\nColumns Found: " + inputFileColumns)
        print("Unable to Find all Required Columns in file " + uidFilename +
              "\nColumns Required: " + databaseColumns +
              "\nColumns Found: " + inputFileColumns)
        exit(1)

    if db_is_new:
        logger.debug(__file__ + ": Creating New Database")
        database.create_table()
    else:
        logger.debug(__file__ + ": Clearing Existing Database")
        database.clear_table()
        # database.drop_table()
        # database.create_table()

    logger.debug(__file__ + ": Populating database")
    for row in iter_record:
        record = UidRecord(*row)
        if record.uid and record.hashcode:
            if database.uid_unique(record.uid):
                database.update_table(record)
            else:
                logger.error(__file__ + ": Duplicate uid Found while populating Database: " + record.uid)
                print("Error in %s. Duplicate uid Found while populating Database: " %(__file__ ) + record.uid)
                exit(3)

    database.commit_changes()
    logger.debug(__file__ + ": Database Populated")

    if sys.argv[action] == addArgName:
        logger.debug(__file__ + ": Trimming Files")
        csvFiles = trimmer(csvFiles)
        logger.debug(__file__ + ": Trimming Files Completed")
        for eachfile in csvFiles:
            logger.debug(__file__ + ": Processing File : %s" % eachfile)
            processFile(eachfile, database)
            logger.debug(__file__ + ": Processed File : %s" % eachfile)
        exit(0)
