import sys
import json
import xmlrpc.client

def words_find(load_mapper):
    w = []
    for l in load_mapper:
        random = l[1]
        temp1 = l[0].split(" ")
        temp = [] 
        for j in temp1:
            if j != "":
                temp.append((j,random))
                random+=len(j)+1
        w.extend(temp)
    return w

def reducer_space(s,w,red_count):
    res_map = [[] for t in range(red_count)]
    for word in w:
        if word[0] != "":
            dict_h = (len(word[0]))%red_count
            res_map[dict_h].append(word)
    k = 0        
    for k in range(red_count):
        ans = "reducer_"+str(k)
        value = res_map[k]
        s.reducer_put_func(ans,value)

def input_find(s,id_map):
    key = "mapper_"+str(id_map)
    load_mapper = s.mapper_find_func(key)
    return load_mapper

def main():
    red_count = int(sys.argv[3])
    map_num = sys.argv[2]
    layout_addr = str(sys.argv[1])
    map_num = int(map_num)
    filed = open(layout_addr,'r')
    json_patta = json.load(filed)
    filed.close() 
    port_db = json_patta["database"].split(" ")[1]
    ip_db = json_patta["database"].split(" ")[0]
    add_ser = "http://"+ip_db+":"+port_db
    s = xmlrpc.client.ServerProxy(add_ser)
    load_mapper = input_find(s,map_num)
    w = words_find(load_mapper)
    reducer_space(s,w,red_count)
if __name__ == "__main__":
    main()