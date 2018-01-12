import logging
import time
import datetime
logger = logging.getLogger('myapp')
hdlr = logging.FileHandler("uid_%s.log" % time.strftime("%Y%m%d_%H%M%S"))
formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)


domain = "@iisc.ac.in"
genFolder = "genResults"
delFolder = "delResults"
processedFolder = "processedFiles"

# Max ID length
IDLength = 12
IDLength_c = 10

# Min ID length when using a combination of First and Last names (excluding numbers)
IDsmallest = 6

# Max ID length for contract staff (excluding numbers)
csIDLength = 8

# Min ID length for contract staff
csIDsmallest = 4


# Number of digits that will be allowed in the userid
digits4faculties = [1]      # Rule 1.1.6
digits4students = [1,2]    # Rule 1.2.2 & # 1.2.3
digits4cs = [1, 2]             # Rule 1.3.1
digits = []


from collections import namedtuple

categoryColumn = 'faculty student projectstaff'
CategoryStrings = [['faculty', 'exfaculty', 'staff', 'adminofficers'], ['student'], ['projectstaff']]
Categories = namedtuple('Categories', categoryColumn)
ValidCategories = Categories(*CategoryStrings)

trimmedColumns = 'category firstname lastname department'
trimmedFileColumns = trimmedColumns.split()
TrimmedRecord = namedtuple('TrimmedRecord', trimmedColumns)

adTrimmedElements = 'uid hashcode'
adTrimmedFileColumns = adTrimmedElements.split()
AdTrimmedRecord = namedtuple('adTrimmedColumns', adTrimmedFileColumns)

uidColumns = 'uid hashcode'
UidRecord = namedtuple('UidRecord', uidColumns.split())

successColumns = trimmedColumns + " " + uidColumns
successFileColumns = successColumns.split()
SuccessRecord = namedtuple('SuccessRecord', successFileColumns)

failureColumns = trimmedColumns + ' reason'
failureFileColumns = failureColumns.split()
FailureRecord = namedtuple('FailureRecord', failureFileColumns)

databaseColumns = uidColumns
databaseTableColumns = databaseColumns.split()
DatabaseRecord = namedtuple('DatabaseRecord', databaseColumns)
DatabaseTableName = "uidTable"

## Error names to generate exceptions and log
ErrWrongCat = "Record Catogory miss-match"
ErrUnable2gen = "Unable to find username for record"
