# this script reads from stdin a sql file created by mysqldump, grabs the requested columns from
# the requested table from each tuple, and writes them out one tuple per line 
# with a comma between columns, keeping the original escaping of values as done by mysql.

import getopt
import os
import re
import sys

class ConverterError(Exception):
    pass

class MysqlFile:
    def __init__(self, f, tableRequested, columnsRequested, valuesRequestedCols, valuesRequestedVals, fieldSeparator):
        self.file = f
        self.tableRequested = tableRequested
        self.columnsRequested = columnsRequested
        self.valuesRequestedCols = valuesRequestedCols
        self.valuesRequestedVals = valuesRequestedVals
        self.fieldSeparator = fieldSeparator

        self.buffer = ""
        self.bufferInd = 0
        self.eof = False
        self.rowsDone = False
        self.GET = 1
        self.CHECK = 2
        self.SKIP = 0

    def findCreateStatement(self):
        tableFound = False
        toFind = "CREATE TABLE `%s` (\n" % self.tableRequested
        line = self.getLine(len(toFind))
        if (not line.endswith("\n")):
            self.skipLineRemainder()
        while line != "":
            if line == toFind:
                tableFound = True
                break
            line = self.getLine(len(toFind))
            if (not line.endswith("\n")):
                self.skipLineRemainder()
        if not tableFound:
            raise ConverterError("create statement for requested table not found in file")

    def getLine(self, maxbytes = 0):
        """returns line including the \n, up to maxbytes"""
        line = ""
        length = 0
        if self.eof:
            return False
        while self.buffer[self.bufferInd] != '\n':
                line = line + self.buffer[self.bufferInd]
                if not self.incrementBufferPtr():
                   return False
                length = length + 1
                if maxbytes and length == maxbytes:
                    return line

        if not self.skipChar('\n'):
            return False
        return line + "\n"

    def skipLineRemainder(self):
        # skip up to the newline...
        while self.buffer[self.bufferInd] != '\n':
                if not self.incrementBufferPtr():
                   return False
        # and now the newline.
        return self.incrementBufferPtr()
        
    def findInsertStatement(self):
        """leave the file contents at the line immediately following
        an INSERT statement"""
        if m.eof:
            return False
        insertFound = False
        toFind = "INSERT INTO `%s` VALUES " % self.tableRequested
        line = self.getLine(len(toFind))
        while line and not self.eof:
            if line.startswith(toFind):
                insertFound = True
                break
            if (not line.endswith("\n")):
                self.skipLineRemainder()
            line = self.getLine(len(toFind))
        return insertFound

    def setupColumnRetrieval(self):
        self.columnsInTable = []
        columnNameExpr = re.compile('\s+`([^`]+)`')
        line = self.getLine()
        while (line and not self.eof and line[0] != ')' ):
            columnNameMatch = columnNameExpr.match(line)
            if (columnNameMatch):
                self.columnsInTable.append(columnNameMatch.group(1))
            line = self.getLine()

        for c in self.columnsRequested:
            if not c in self.columnsInTable:
                raise ConverterError("requested column %s not found in table" % c)

#        print "columns in table: ", self.columnsInTable
#        print "columnsRequested: ", self.columnsRequested

        self.columnsToGet = []
        for c in self.columnsInTable:
            v = self.SKIP
            if c in self.columnsRequested:
                v = v | self.GET
            if c in self.valuesRequestedCols:
                v = v | self.CHECK
            self.columnsToGet.append( v )

#        print "columns to get: ", self.columnsToGet

        self.columnOrder = []
        # we want here a list which tells us to
        # write the ith column we read from tuple first,
        # the jth one second, the kth one third etc. 
        columnsToGetTrue = []
        for i in range(0,len(self.columnsToGet)):
            if self.columnsToGet[i] & self.GET:
                columnsToGetTrue.append(self.columnsInTable[i])
        for c in self.columnsRequested:
            self.columnOrder.append(columnsToGetTrue.index(c))

#        print "column order: ", self.columnOrder

    def whine(self, message = None):
        if (message):
            raise ConverterError("whine whine whine: " + message )
        else:
            raise ConverterError("whine whine whine. failed to parse a row.")

    def getColumnsFromRow(self):
        """returns a list of column values extracted from a row.
        f is an open input file positioned at the beginning of a 
        tuple representing a row in mysql output format,
        colsToGet is a list of True/False correspnding to which
        elements in the tuple we want to retrieve and return"""
    
#        print "buffer is ", self.buffer[self.bufferInd:self.bufferInd+80], "..."
        if not self.skipStartOfRow():
            self.whine("couldn't find start of row")
        cols = []
        ind = 0
        skip = False
        for c in self.columnsToGet:
            if skip:
                    self.skipColValue()
            elif c & self.GET:
                cols.append(self.getColValue())
                if c & self.CHECK:
                    colName = self.columnsInTable[ind]
                    j = self.valuesRequestedCols.index(colName)
                    if self.getColValue() != self.valuesRequestedVals[j]:
                        skip = True
                        cols = None
            elif c & self.CHECK:
                colName = self.columnsInTable[ind]
                j = self.valuesRequestedCols.index(colName)
                if self.getColValue() != self.valuesRequestedVals[j]:
                    skip = True
                    cols = None
            else:
                    self.skipColValue()
            ind = ind + 1

        self.skipEndOfRow()
        return(cols)

    def skipStartOfRow(self):
        # expect (
        if not self.skipChar('('):
            return False
        return True

    def skipEndOfRow(self):
        # expect... what do we expect? ); or ), 
        # the first means end of row with no more rows after, the second means end of
        # specific row only
        if not self.skipChar(')'):
            self.whine()
        if not self.skipChar(','):
            if self.skipChar(';'):
                self.rowsDone = True
            else:
                self.whine()
            self.skipChar('\n')

    def getColValue(self):
        #expect: a string of digits 
        # or: '  some stuff, ' 
        value=""
        if (self.buffer[self.bufferInd].isdigit()):
            while self.buffer[self.bufferInd].isdigit():
                value=value + self.buffer[self.bufferInd]
                if not self.incrementBufferPtr():
                    return False
            # there will be a comma before the next
            # column if we aren't at the end of the row.
            self.skipChar(',')
            return value
        elif (self.skipChar("'")):
            value = "'"
            done = False
            escaped = False
            while not done:
                if self.buffer[self.bufferInd] != "'" and self.buffer[self.bufferInd] != '\\':
                    value=value + self.buffer[self.bufferInd]
                    if not self.incrementBufferPtr():
                        return False
                    escaped = False
                elif self.buffer[self.bufferInd] == "'":
                    value=value + self.buffer[self.bufferInd]
                    if not self.incrementBufferPtr():
                        return False
                    if not escaped:
                        done = True
                    else:
                        escaped = False
                else: # escape char \ found
                    value=value + self.buffer[self.bufferInd]
                    if not self.incrementBufferPtr():
                        return False
                    if escaped:
                        escaped = False
                    else:
                        escaped = True
            if done:
                # there will be a comma before the next
                # column if we aren't at the end of the row.
                self.skipChar(',')
                return value
        else:
            self.whine()

    def skipColValue(self):
        #expect: a string of digits with possibly a . in there
        # or: '  some stuff, ' 
        if (self.buffer[self.bufferInd].isdigit()):
            # might have a float so... crudely...
            while self.buffer[self.bufferInd].isdigit() or self.buffer[self.bufferInd] == '.' or self.buffer[self.bufferInd] == 'e' or self.buffer[self.bufferInd] == '-':
                if not self.incrementBufferPtr():
                    return False
            # there will be a comma before the next
            # column if we aren't at the end of the row.
            self.skipChar(',')
        elif (self.skipChar("'")):
            done = False
            escaped = False
            while not done:
                if self.buffer[self.bufferInd] != "'" and self.buffer[self.bufferInd] != '\\':
                    if not self.incrementBufferPtr():
                        return False
                    escaped = False
                elif self.buffer[self.bufferInd] == "'":
                    if not self.incrementBufferPtr():
                        return False
                    if not escaped:
                        done = True
                    else:
                        escaped = False
                else: # escape char \ found
                    if not self.incrementBufferPtr():
                        return False
                    if escaped:
                        escaped = False
                    else:
                        escaped = True
            if done:
                # there will be a comma before the next
                # column if we aren't at the end of the row.
                self.skipChar(',')
        else:
#            print "buffer is ", self.buffer[self.bufferInd:self.bufferInd+80], "..."
            self.whine("failed to parse a value, found start character " + self.buffer[self.bufferInd])

    def skipChar(self, c):
        if self.buffer[self.bufferInd] == c:
            if not self.incrementBufferPtr():
                return False
            return True
        else:
            return False

    def incrementBufferPtr(self):
        self.bufferInd = self.bufferInd + 1
        if self.bufferInd == len(self.buffer):
            return self.fillBuffer() # this will move the index accordingly
        return True

    def fillBuffer(self):
        if self.bufferInd == len(self.buffer) and not self.rowsDone:
            # we are out of data in the buffer, and there's more 
            # rows to be gotten

            # fixme this should be a constant someplace configurable
            self.buffer = self.file.read(8192)
            if (self.buffer == ""):
                self.rowsDone = True
                self.eof = True
                return False
            else:
                self.bufferInd = 0
                return True

    def formatColumn(self, column):
        """for now we do nothing. maybe we want this in the future."""
        return column

    def writeColumns(self, columns, outFile):
        """takes a list of column values without names. 
        must find the names these correspond to, figure out the right 
        order (or alternatively maybe we have a map that tells us the order)
        and write the values out in the new order."""
        if columns:
            ind = 0
            for i in self.columnOrder:
                outFile.write(self.formatColumn(columns[i])) 
                if ind < len(self.columnOrder)-1:
                    outFile.write(self.fieldSeparator)
                ind = ind + 1
            outFile.write('\n')

def usage(message = None):
    if message:
        print message
        print "Usage: python mysql2txt.py --table=tablename --columns=col1,col2... "
        print "                     [--values=col1=val1,col2=val2...] [--separator=<string>]"
        print ""
        print "This script reads a table dump in mysql format from stdin and writes"
        print "specified columns from desired rows to stdout, one line per row."
        print ""
        print "--table:     the name of the table from which we want to extract values"
        print "--columns:   the names of the columns from the table, separated by commas,"
        print "             the values of which we want to retrieve, in the order we want"
        print "             them to be written on each line of the output"
        print "--values:    pairs of column names and values we want the column to have, for"
        print "             each row to be printed;  in each pair the column name and"
        print "             the value are separated by an equals sign, and these pairs are"
        print "             separated from each other by commas"
        print "--separator: the string which will be written after each value in a row"
        print "             to separate it from the next value, by default a space"
        print ""
        print "Example: zcat elwikidb-20111102-page.sql.gz | python mysql2txt.py --table=page \\"
        print "              --columns=page_title,page_id  --values=page_namespace=15 --separator=' | '"
        sys.exit(1)

if __name__ == "__main__":
    tableRequested = None
    columnsRequested = None
    valuesRequestedCols = []
    valuesRequestedVals = []
    fieldSeparator = ' '

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "", ['table=', 'columns=', 'values=', 'separator=' ])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--table":
            tableRequested = val
        elif opt == "--columns":
            if ',' in val:
                columnsRequested = val.split(',')
            else:
                columnsRequested = [ val ]
        elif opt == "--values":
            if ',' in val:
                vlist = val.split(',')
            else:
                vlist = [ val ]
            valuesRequestedCols = [ v.split('=')[0] for v in vlist ]
            valuesRequestedVals = [ v.split('=')[1] for v in vlist ]
        elif opt == "--separator":
            fieldSeparator = val

    if (len(remainder) > 0):
        usage("Unknown option specified")

    if (not tableRequested or not columnsRequested):
        usage("Missing required option")

    m = MysqlFile(sys.stdin, tableRequested, columnsRequested, valuesRequestedCols, valuesRequestedVals, fieldSeparator)
    m.fillBuffer()

    m.findCreateStatement()
    m.setupColumnRetrieval()

    if not m.findInsertStatement():
            raise ConverterError("insert statement for requested table not found in file")
    while (not m.eof):
        cols = m.getColumnsFromRow()
        # write them out in the correct order...
        m.writeColumns(cols, sys.stdout)
        if m.rowsDone and not m.eof:
            # could have multiple inserts for the same table
            m.rowsDone = False
            m.findInsertStatement()

    exit(0);

