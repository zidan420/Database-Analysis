import sqlite3
import os
import argparse
from hackslib.hackslib import banner
from colorama import Fore, Style, init

# c.execute(f"SELECT {rowid_string}{header} FROM {table} WHERE {where} ORDER BY {property_name} {asc_desc} LIMIT {limit}")

def rowid_str(db):
    if db.split('.')[-1] == 'sqlite':
        return ''
    else:
        return 'rowid, '

def header_str(header):
    if header == "":
        return '*'
    else:
        return header

def where_str(db, table, where):
    if where == "":
        return ''
    else:
        return where_handler(db, table, where)

def rowid_where_str(rowid, where):
    if rowid == "":
        if where =="":
            return ''
        else:
            return f' WHERE {where}'
    else:
        rowids = rowid.split(",")
        for row_id in rowids:
            # Rowid checker
            try:
                int(row_id)
            except:
                print(f"[-] \"{row_id}\" is NOT valid!")
                print("[-] Please provide a valid rowid")
                exit()
            if row_id == rowids[0]:
                r = f"rowid = {row_id}"
            else:
                r += f" OR rowid = {row_id}"

        if where == "":
            return f' WHERE {r}'
        else:
            return f' WHERE ({r}) AND ({where})'

def order_by_str(order_by):
    if order_by == "":
        return ''
    else:
        property_name = order_by[0]
        asc_desc = order_by[1]
        order_by = f' ORDER BY {property_name} {asc_desc}'
        return order_by

def limit_str(limit):
    if limit == "":
        return ''
    else:
        limit = f' LIMIT {limit}'
        return limit

def print_values(c):
    # print the headers
    headers = c.description
    header_format = ""
    for header in headers:
        if header[0] == "rowid":
            header_format += header[0].ljust(10)
        else:
            header_format += header[0].ljust(50)
    print(header_format)
    underscore = "-"
    print(underscore.ljust(10+50*(len(headers)-1), "-"))

    # print the description or values of headers
    all_table_info = c.fetchall()
    for column in all_table_info:
        i = 0
        column_format = ""
        while i < len(column):
            if len(str(column[i])) > 48:
                if i == len(column) - 1:
                    item = str(column[i])
                else:
                    item = str(column[i])[:44] + " [..] "
            else:
                item = str(column[i])
            if i == 0:
                column_format = column_format + item.ljust(10)
            else:
                column_format = column_format + item.ljust(50)
            i += 1
        print(column_format)

def struc_an(database):
    conn = sqlite3.connect(database)
    c = conn.cursor()

    # Enables coloring in windows
    init(convert = True)

    # list all the tables
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")

    table_names = c.fetchall()
    print("\nTables\n---------------")
    for table_name in table_names:
        # table_name is a tuple
        print(Fore.GREEN + table_name[0] + Style.RESET_ALL)

        if table_name[0]:
            # selects all the data from table
            c.execute(f"SELECT * FROM {table_name[0]}")

            # print each row in the table
            rows = c.description
            space = " "
            for row in rows:
                print(Fore.CYAN + space * 20 + row[0] + Style.RESET_ALL)                  # prints the tuple
    c.close()

def db_fetch_handler(db, table, header, rowid, where, order_by, limit):
    rowid_string = rowid_str(db)
    header = header_str(header)
    where = where_str(db, table, where)
    rowid_where = rowid_where_str(rowid, where)
    order_by = order_by_str(order_by)
    limit = limit_str(limit)

    conn = sqlite3.connect(db)
    c = conn.cursor()

    c.execute(f"SELECT {rowid_string}{header} FROM {table} {rowid_where} {order_by} {limit}")
    print_values(c)
    c.close()

def arg_validator(db, table, hdr, order_by, limit):
    rowid_string = rowid_str(db)

    conn = sqlite3.connect(db)
    c = conn.cursor()

    table_matched = False
    order_by_0 = False
    hdr_matched = 0

    # Table Name checker
    c.execute("SELECT name FROM sqlite_master WHERE type='table';") # list all the tables

    table_names = c.fetchall()
    for table_name in table_names:
        if table == table_name[0]:
            table_matched = True
    if table_matched == False:
        print("[-] No such table Found!")
        exit()

    # Header checker
    if hdr != "":
        hdr = hdr.split(",")
        hdr_len = len(hdr)
        c.execute(f"SELECT {rowid_string}* FROM {table}")
        headers = c.description
        for header in headers:
            for i in hdr:
                if i == header[0]:
                    hdr_matched += 1
        if hdr_matched != hdr_len:
            print("[-] No such column exists!")
            exit()
    
    # Order Checker
    if order_by != "":
        c.execute(f"SELECT {rowid_string}* FROM {table}")
        headers = c.description
        for header in headers:
            if order_by[0] == header[0]:
                order_by_0 = True
        if order_by_0 == False:
            print("[-] No such column exists!")
            exit()

        if (order_by[1].lower() == "asc") or (order_by[1].lower() == "desc"):
            pass
        else:
            print("[-] Please type \"asc\" or \"desc\" as argument of order (regardless of small letter or captial letter)")
            exit()

    # Limit Checker
    if limit != "":
        try:
            int(limit)
        except:
            print(f"[-] \"{limit}\" is NOT an integer!")
            print("[-] Please provide an integer value for limit.")
            exit()
    c.close()

def where_handler(db, table, where):
    rowid_string = rowid_str(db)

    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute(f"SELECT {rowid_string}* FROM {table}")
    headers = c.description
    
    where_math = False
    header_match = 0
    condi_splitter = ""
    splitter = ""
    where_clause = ""
    
    if ",AND" in where:
        condi_splitter = ",AND"
    elif ",OR" in where:
        condi_splitter = ",OR"
    else:
        if ">=" in where:
            splitter = ">="
            where_math = True
        elif "<=" in where:
            splitter = "<="
            where_math = True
        elif ">" in where:
            splitter = ">"
            where_math = True
        elif "<" in where:
            splitter = "<"
            where_math = True
        elif "=" in where:
            splitter = "="
        elif "LIKE" in where:
            splitter = "LIKE"
        else:
            print("[-] Invalid WHERE Clause")
            c.close()
            exit()
        condi_array = where.split(f"{splitter}")

        # Header Checker
        for header in headers:
            if condi_array[0] == header[0]:
                header_match = 1
        if header_match != 1:   # Only 1 condition/tuple
            print("[-] No such column exists!")
            exit()

        if where_math == False:
            condition_formatted = f"{condi_array[0]} {splitter} '{condi_array[1]}'"
        else:
            condition_formatted = f"{condi_array[0]} {splitter} {condi_array[1]}"
        c.close()
        return condition_formatted

    conditions = where.split(f"{condi_splitter}")
    for condition in conditions:
        if ">=" in condition:
            splitter = ">="
            where_math = True
        elif "<=" in condition:
            splitter = "<="
            where_math = True
        elif ">" in condition:
            splitter = ">"
            where_math = True
        elif "<" in condition:
            splitter = "<"
            where_math = True
        elif "=" in condition:
            splitter = "="
        elif "LIKE" in condition:
            splitter = "LIKE"
        else:
            print("[-] Invalid WHERE Clause")
            c.close()
            exit()
        
        condi_array = condition.split(f"{splitter}")

        # Header Checker
        for header in headers:
            if condi_array[0] == header[0]:
                header_match += 1
        
        if where_math == False:
            condition_formatted = f"{condi_array[0]} {splitter} '{condi_array[1]}'"
        else:
            condition_formatted = f"{condi_array[0]} {splitter} {condi_array[1]}"

        if condition == conditions[0]:
            where_clause = condition_formatted
        else:
            where_clause += f" {condi_splitter[1:]} {condition_formatted}"

    if header_match != len(conditions):
            print("[-] No such column exists!")
            exit()
    c.close()
    return where_clause

def main():
    header = ""
    rowid = ""
    where = ""
    order_by = ""
    limit = ""

    parser = argparse.ArgumentParser(description=f"Welcome To Database Analysis! (Version: 2.0)")
    requiredArgs = parser.add_argument_group("required arguments:")
    requiredArgs.add_argument("-d", "--database", required=True, help="Path to the Database")
    parser.add_argument("-s", "--structure", action="store_true", help="Analyse the structure of a database")
    parser.add_argument("-t", "--table", help="Table to dump")
    parser.add_argument("-hd", "--header", help="Table Header to dump")
    parser.add_argument("-l", "--limit", help="Limit no. of rows")
    parser.add_argument("-id", "--rowid", help="RowID of a table")
    parser.add_argument("-w", "--where", help="Search using WHERE clause of sqlite3")
    parser.add_argument("-ord", "--orderby", nargs="+", help="Arrange a column in ascending or descending order")
    args = parser.parse_args()
    database = args.database
    structure = args.structure
    if args.header != None:
        header = args.header
    if args.limit != None:
        limit = args.limit
    if args.rowid != None:
        rowid = args.rowid
    if args.where != None:
        where = args.where
    if args.orderby != None:
        order_by = args.orderby

    if os.path.isfile(database) == False:
        print("[-] database NOT Found!")
        exit()

    print(banner.banner('dban'))
    print(f"\nversion: 2.0\t\t ~ Made by Md Zidan Khan\n{'-'.ljust(58, '-')}")

    if structure:
        struc_an(database)
        exit()

    if args.table == None:
        print("[-] Are you missing any argument?")
        exit()
    else:
        table = args.table

    arg_validator(database, table, header, order_by, limit)
    db_fetch_handler(database, table, header, rowid, where, order_by, limit)

if __name__ == '__main__':
    main()