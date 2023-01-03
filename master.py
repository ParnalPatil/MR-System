from typing import List
import xmlrpc.client
import multiprocessing
import json
import os 
import re


def red_hold(file_content,addr_content,code_red,num_red):
    loc_red = addr_content["red_"+str(num_red)]
    make_dir = "ssh "+str(loc_red)+" 'mkdir -p reducer_"+str(num_red)+"'"
    var_arg = ["rsync", "./"+code_red, str(loc_red) + ":" + "reducer_"+str(num_red)+"/"+code_red]
    var_arg1 = ["rsync", "./"+file_content, str(loc_red) + ":" + "reducer_"+str(num_red)+"/"+file_content]
    com_red_transfer = " ".join(var_arg)
    com_con_transfer = " ".join(var_arg1)
    os.system(make_dir)
    os.system(com_red_transfer)
    os.system(com_con_transfer)
    os.system(f"rsync start_map_red.sh {str(loc_red)}:start_map_red.sh")
    run_reducer_cmd = "ssh "+str(loc_red)+" 'bash start_map_red.sh reducer_"+str(num_red)+"/"+code_red+" reducer_"+str(num_red)+"/"+file_content+" "+str(num_red)+"'"
    os.system(run_reducer_cmd)


def map_hold(file_content,addr_content,code_map,inp_map,num_map,num_red):
    add_map = addr_content["map_"+str(num_map)]
    make_dir = "ssh "+str(add_map)+" 'mkdir -p mapper_"+str(num_map)+"'"
    var_arg1 = ["rsync", "./"+file_content, str(add_map) + ":" + "mapper_"+str(num_map)+"/"+file_content]
    var_arg = ["rsync", "./"+code_map, str(add_map) + ":" + "mapper_"+str(num_map)+"/"+code_map]
    command = " ".join(var_arg)
    command1 = " ".join(var_arg1)
    os.system(make_dir)
    os.system(command)
    os.system(command1)
    os.system(f"rsync start_map_red.sh {str(add_map)}:start_map_red.sh")
    port_db = addr_content["database"].split(" ")[1]
    ip_db = addr_content["database"].split(" ")[0]
    add_db = "http://"+ip_db+":"+port_db
    s = xmlrpc.client.ServerProxy(add_db)
    current = s.mapper_put_func("mapper_"+str(num_map),inp_map)
    map_speed = "ssh "+str(add_map)+" 'bash start_map_red.sh mapper_"+str(num_map)+"/"+code_map+" mapper_"+str(num_map)+"/"+file_content+" "+str(num_map)+" "+num_red+"'"
    os.system(map_speed)  


def slice_c(line_in,map_num):
    slice_begin = 0
    slice_temp = 0
    line_count = len(line_in)
    avg_line_map = line_count // map_num
    slice_finish = avg_line_map + (line_count % map_num)
    slices = []
    while slice_temp < map_num:
        slices.append((slice_begin,slice_finish))
        slice_begin = slice_finish
        slice_temp+=1
        slice_finish = slice_finish + avg_line_map
    return slices

def l_pre_get(l_in_file):
    line_in =[]
    l_len_all = 0
    with open(l_in_file,"r") as md:
        lines = md.readlines()
        l_len_all = len(lines)
    with open(l_in_file,"r") as kd:
        random = 0
        count = 0
        while count < l_len_all:
            l = kd.readline()
            if l != "\n":
                l = re.sub(r"[^a-zA-Z0-9 ]","",l)
                line_in.append(tuple((l,random)))
            random += len(l)
            count+=1
    return line_in

def master_function (l_in_file,map_num,num_red,code_map,code_red,addr_content):
    map_num = int(map_num)
    num_red = int(num_red)
    md = open(addr_content,'r')
    j_address = json.load(md)
    md.close()
    
    
    line_in = l_pre_get(l_in_file)
    slices = slice_c(line_in,map_num)
    procs_map: List[multiprocessing.Process] =[]
    procs_reds = []
    print('mapper starting')
    for i in range(map_num):
        func_map = multiprocessing.Process(target=map_hold,args=(addr_content,j_address,code_map,line_in[slices[i][0]:slices[i][1]],i,str(num_red),))
        func_map.start()
        procs_map.append(func_map)

    for p in procs_map:
        p.join()
        if p.exitcode !=0:
            p.start()
            p.join()

    print('mapper end')
    for j in range(num_red):
        func_red = multiprocessing.Process(target = red_hold,args =(addr_content,j_address,code_red,j,))
        func_red.start()
        procs_reds.append(func_red)

    print('reducer starting')
    for p in procs_reds:
        p.join()
        if p.exitcode !=0:
            p.start()
            p.join()

    print('reducer end')

    ip_db = j_address["database"].split(" ")[0]
    port_db = j_address["database"].split(" ")[1]
    add_db = "http://"+ip_db+":"+port_db
    s = xmlrpc.client.ServerProxy(add_db)
    s.come_out()