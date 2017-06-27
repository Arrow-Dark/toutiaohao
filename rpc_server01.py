from xmlrpc.server import SimpleXMLRPCServer
import socketserver
import sys
import headlineFetcher_domain

class Do_Work():
    def _start(self):
        try:
            headlineFetcher_domain.pro_pool_()
        except Exception as e:
            return e

if __name__=='__main__':
    ip=sys.argv[1]
    port=sys.argv[2]
    class RPCThreading(socketserver.ThreadingMixIn,SimpleXMLRPCServer):
        pass
    do_work_obj=Do_Work()
    server=RPCThreading((ip,int(port)))
    server.register_instance(do_work_obj)
    server.serve_forever()
