from xmlrpc.client import ServerProxy
import os
import threading
def get_server_ips(sfile):
    server_ips=[]
    with open(sfile,'r',encoding='utf-8') as f:
        for ind,line in enumerate(f.readline()):
            ip=line.strip()
            server_ips.append([ind,ip])
    return server_ips


def do_work_client(server_ip):
    server=ServerProxy(server_ip[1])
    server._start()

def multiple_threads_test():
    sfile_name=os.path.abspath('.')+'/serverIps'+'/server_ips.txt'
    server_ips=get_server_ips(sfile_name)
    server_cnt=len(server_ips)
    th_lst=[]
    for i in range(server_cnt):
        t=threading.Thread(target=do_work_client,args=(server_ips[i%server_cnt]))
        th_lst.append(t)
    for t in th_lst:
        t.start()
    for t in th_lst:
        t.join()

if __name__=='__main__':
    multiple_threads_test()