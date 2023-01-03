import json
from json.decoder import JSONDecodeError
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
import sys
import threading

locking = threading.Lock()
f = open("database/mapper_database.json",'w')
f.close()
f = open("database/reducer_database.json",'w')
f.close()
f = open("database/solution_database.json",'w')
f.close()
class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass
def solution_put(key,val):
    locking.acquire()
    with open('database/solution_database.json','r') as j_file:
        try:
            database = json.load(j_file)
            j_file.close()
        except JSONDecodeError:
            database = {}
    database[key] = val
    with open("database/solution_database.json", "w") as outfile:
        json.dump(database, outfile)
        outfile.close()
    locking.release()
    return True
def solution_find(key):
    locking.acquire()
    with open('database/solution_database.json','r') as j_file:
        try:
            database = json.load(j_file)
            j_file.close()
        except JSONDecodeError:
            database = {}
    locking.release()
    if key in database.keys():
        return database[key]
    else:
        return False
def reducer_put(key,val):
    locking.acquire()
    with open('database/reducer_database.json','r') as j_file:
        try:
            database = json.load(j_file)
            j_file.close()
        except JSONDecodeError:
            database = {}
    if key in database.keys():
        temp_arr = database[key]
        temp_arr.extend(val)
        database[key] = temp_arr
    else:
        database[key] = val
    with open("database/reducer_database.json", "w") as outfile:
        json.dump(database, outfile)
        outfile.close()
    locking.release()
    return True
def reducer_find(key):
    locking.acquire()
    with open('database/reducer_database.json','r') as j_file:
        try:
            database = json.load(j_file)
            j_file.close()
        except JSONDecodeError:
            database = {}
    locking.release()
    if key in database.keys():
        return database[key]
    else:
        return False

def mapper_put(key,val):
    locking.acquire()
    with open('database/mapper_database.json','r') as j_file:
        try:
            database = json.load(j_file)
            j_file.close()
        except JSONDecodeError:
            database = {}
    database[key] = val
    with open("database/mapper_database.json", "w") as outfile:
        json.dump(database, outfile)
        outfile.close()
    locking.release()
    return True
def mapper_find(key):
    locking.acquire()
    with open('database/mapper_database.json','r') as j_file:
        try:
            database = json.load(j_file)
            j_file.close()
        except JSONDecodeError:
            database = {}
    locking.release()
    if key in database.keys():
        return database[key]
    else:
        return False
        
def main():
    con_name = sys.argv[1]
    td = open(con_name,'r')
    a_j = json.load(td)
    td.close()
    db_ser_ip = a_j["database"].split(" ")[0]
    db_port = a_j["database"].split(" ")[1]
    with SimpleThreadedXMLRPCServer((str(db_ser_ip), int(db_port))) as ser:
        ser.register_introspection_functions()
        print("DB server created")
        def solution_put_func(key,val):
            comp = solution_put(key,val)
            return comp
        ser.register_function(solution_put_func,'solution_put_func')
        def solution_find_func(key):
            comp = solution_find(key)
            return comp
        ser.register_function(solution_find_func,'solution_find_func')

        def reducer_put_func(key,val):
            comp = reducer_put(key,val)
            return comp
        ser.register_function(reducer_put_func,'reducer_put_func')
        def reducer_find_func(key):
            comp = reducer_find(key)
            return comp
        ser.register_function(reducer_find_func,'reducer_find_func')
        def mapper_put_func(key,val):
            comp = mapper_put(key,val)
            return comp
        ser.register_function(mapper_put_func,'mapper_put_func')
        def mapper_find_func(key):
            val = mapper_find(key)
            return val
        ser.register_function(mapper_find_func,'mapper_find_func')
        def come_out():
            ser.shutdown()
            return("DB shut down")
        ser.register_function(come_out,'come_out')
        ser.serve_forever()
if __name__ == "__main__":
    main()