import sys
import json
import xmlrpc.client

def num_calculate(load_red):
    hashmap_calc = {}
    for w in load_red:
        if w[0] not in hashmap_calc:
            hashmap_calc[w[0]] = [w[1]]
        else:
            hashmap_calc[w[0]].append(w[1])
    return hashmap_calc

def db_addition(s,hashmap_calc):
    for key,value in hashmap_calc.items():
        s.solution_put_func(key,value)

def input_find(s,id_red):
    ans = "reducer_"+str(id_red)
    load_red = s.reducer_find_func(ans)
    return load_red

def main():
    add_config = str(sys.argv[1])
    id_red = sys.argv[2]
    id_red = int(id_red)
    filed = open(add_config,'r')
    j_add = json.load(filed)
    filed.close() 
    ip_db = j_add["database"].split(" ")[0]
    port_db = j_add["database"].split(" ")[1]
    add_serv = "http://"+ip_db+":"+port_db
    s = xmlrpc.client.ServerProxy(add_serv)
    load_red = input_find(s,id_red)
    word_count = num_calculate(load_red)
    db_addition(s,word_count)

if __name__ == "__main__":
    main()