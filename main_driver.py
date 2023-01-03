import sys
import multiprocessing
import threading
from master import *
import time

locking = threading.Lock()

def ser_db_handle(address_config):
    filed = open(address_config,'r')
    js_add = json.load(filed)
    filed.close() 
    port_ip = js_add["database"].split(" ")[1]
    ip_brief = js_add["database"].split(" ")[0]
    directory_command = "ssh "+str(ip_brief)+" 'mkdir -p database'"
    arguments = ["scp", "./ser_db.py", str(ip_brief) + ":" + "database/ser_db.py"]
    arguments2 = ["scp", "./"+address_config, str(ip_brief) + ":" + "database/"+address_config]
    os.system(f"scp start_db.sh {ip_brief}:start_db.sh")
    command2 = " ".join(arguments2)
    command = " ".join(arguments)
    database_run_cmd = "ssh "+ip_brief+" 'bash start_db.sh "+"database/"+address_config+" 2>&1 >>database_log'"
    os.system(directory_command)
    os.system(command)
    os.system(command2)
    os.system(database_run_cmd)
    time.sleep(5)  
def main():
    user_input = sys.argv[1]
    user_open = open(user_input,'r')
    user_json = json.load(user_open)
    user_open.close() 
    function_mapper = user_json['mapper_func']
    function_reducer = user_json['reducer_func']
    ip_file_location = user_json['input_file']
    config_add = user_json['ipconfig']  
    map_count = user_json['mapper_count']
    reduc_count = user_json['reducer_count']
    arg_len = len(sys.argv)
    proc = multiprocessing.Process(target=ser_db_handle, args=(config_add,))
    proc.start()
    proc.join()
    proc2 = multiprocessing.Process(target=master_function, args=(ip_file_location,map_count,reduc_count,function_mapper,function_reducer,config_add,))
    proc2.start()
    proc2.join()
if __name__ == "__main__":
    main()