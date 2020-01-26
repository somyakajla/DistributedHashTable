import hashlib
import pickle
import sys
import socket
import threading
from queue import Queue

import rpyc
from rpyc.utils.server import ThreadedServer
from threading import Thread


M = 3 # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
NODES = 2** M
BUF_SZ = 4096  # socket recv arg
BACKLOG = 100  # socket listen arg
TEST_BASE = 43544  # for testing use port numbers on localhost at TEST_BASE+n
NUMBER_THREADS = 2
THREAD_NUMBER = [1, 2]
QUEUE_OBJ =Queue()

class ModRange(object):
    """
    Range-like object that wraps around 0 at some divisor using modulo arithmetic.

    >>> mr = ModRange(1, 4, 100)
    >>> mr

    >>> 1 in mr and 2 in mr and 4 not in mr
    True
    >>> [i for i in mr]
    [1, 2, 3]
    >>> mr = ModRange(97, 2, 100)
    >>> 0 in mr and 99 in mr and 2 not in mr and 97 in mr
    True
    >>> [i for i in mr]
    [97, 98, 99, 0, 1]
    """

    def __init__(self, start, stop, divisor):
        self.divisor = divisor
        self.start = start % self.divisor
        self.stop = stop % self.divisor
        # we want to use ranges to make things speedy, but if it wraps around the 0 node, we have to use two
        if self.start < self.stop:
            self.intervals = (range(self.start, self.stop),)
        else:
            self.intervals = (range(self.start, self.divisor), range(0, self.stop))

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ''.format(self.start, self.stop, self.divisor)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        for interval in self.intervals:
            if id in interval:
                return True
        return False

    def __len__(self):
        total = 0
        for interval in self.intervals:
            total += len(interval)
        return total

    def __iter__(self):
        return ModRangeIter(self, 0, -1)


class ModRangeIter(object):
    """ Iterator class for ModRange """

    def __init__(self, mr, i, j):
        self.mr, self.i, self.j = mr, i, j

    def __iter__(self):
        return ModRangeIter(self.mr, self.i, self.j)

    def __next__(self):
        if self.j == len(self.mr.intervals[self.i]) - 1:
            if self.i == len(self.mr.intervals) - 1:
                raise StopIteration()
            else:
                self.i += 1
                self.j = 0
        else:
            self.j += 1
        return self.mr.intervals[self.i][self.j]


class FingerEntry(object):
    """
    Row in a finger table.

    >>> fe = FingerEntry(0, 1)
    >>> fe

    >>> fe.node = 1
    >>> fe

    >>> 1 in fe, 2 in fe
    (True, False)
    >>> FingerEntry(0, 2, 3), FingerEntry(0, 3, 0)
    (, )
    >>> FingerEntry(3, 1, 0), FingerEntry(3, 2, 0), FingerEntry(3, 3, 0)
    (, , )
    >>> fe = FingerEntry(3, 3, 0)
    >>> 7 in fe and 0 in fe and 2 in fe and 3 not in fe
    True
    """

    def __init__(self, n, k, node=None):
        if not (0 <= n < NODES and 0 < k <= M):
            raise ValueError('invalid finger entry values')
        self.start = (n + 2 ** (k - 1)) % NODES
        self.next_start = (n + 2 ** k) % NODES if k < M else n
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.node = node

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ''.format(self.start, self.next_start, self.node)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        return id in self.interval

    '''
    Chord class which implements chord paper
    '''


class ChordNode:
    def __init__(self, n):
        self.node = n
        self.finger = [None] + [FingerEntry(n, k) for k in range(1, M + 1)]  # indexing starts at 1
        self.predecessor = None
        self.keys = {}
        self.hash = {}

    ''' 
        This method, Joins a network if  existing_node_address != 0 and initialise its finger table
        Update others
        if this is the very first node in the chord it put itself in the finger table and predecessor as well '''
    def join_network(self, existing_node_address, n):
        if not existing_node_address == 0:
            existing_node = existing_node_address - TEST_BASE
            self.init_finger_table(existing_node)
            self.update_others()
            for i in range(1, M + 1):
                self.update_finger_table(self.node, i)
        else:
            for i in range(1, M + 1):
                self.finger[i].node = n
            self.predecessor = n
           #self.print_finger_table()

    '''
        This method initialize the finger table
        Param 1: node number of existing node in the chord network
        it calls rpc to find successor for self.node
        update its predecessor, and call rpc to its successor to update self.node as its predecessor
        Updates keys whenever there is new node in the chord network'''
    def init_finger_table(self, existing_node):
        self.finger[1].node = self.call_rpc(existing_node, 'find_successor', self.finger[1].start)
        self.predecessor = self.call_rpc(self.successor, 'predecessor')
        self.call_rpc(self.successor, 'update_predecessor', self.node)
        keys_to_save = self.call_rpc(self.successor, 'generate_keys', self.predecessor+1, self.node+1)
        self.keys.update(keys_to_save)
        for i in range(1, M):
            if self.finger[i + 1].start in ModRange(self.node, self.finger[i].node, NODES):
                self.finger[i + 1].node = self.finger[i].node
            else:
                self.finger[i + 1].node = self.call_rpc(existing_node, 'find_successor', self.finger[i + 1].start)
        #self.print_finger_table()

    '''
        This method updates the keys whenever new node is being added to the chord network
        Param 1 and Param 2: (predecessor+1, self+node)
        This method asks its successor to provide the keys for which, it is responsible for
        keys between range (predecessor+1, self+node) should be return as a response
        @:return: {key: value} pairs'''
    def generate_keys(self, start, end):
        response = {}
        for key in range(start, end):
            if key in self.keys:
                response.update({key:self.keys[key]})
        for key_ in response:
            if key_ in self.keys: del self.keys[key_]
        return response
    '''
        This method iterates all the nodes for which finger table should be updated
        It find predecessor from 1 to M (entries in the finger table)
        it calls rpc to update the finger table of each predecessor node for finger table entry'''
    def update_others(self):
        for i in range(1, M + 1):
            np = self.find_predecessor((1 + self.node - 2 ** (i - 1) + NODES) % NODES)
            self.call_rpc(np, 'update_finger_table', self.node, i)
        #self.print_finger_table()

    '''
        This method updates finger table
        Param 1: Node number
        Param 2: finger table entry number
        @:return: string(self)
        '''
    def update_finger_table(self, s, i):
        if self.finger[i].start != self.finger[i].node and s in ModRange(self.finger[i].start, self.finger[i].node, NODES):
            self.finger[i].node = s
            #self.print_finger_table()
            np = self.predecessor
            if np != s:
                self.call_rpc(np, 'update_finger_table', s, i)
            return str(self)
        return 'Do nothing : {}'.format(self)

    '''
        To print finger table'''
    def print_finger_table(self):
        print('##############################################################################')
        for i in range(1, M + 1):
            print("finger table entry :", self.finger[i].start, self.finger[i].next_start, self.finger[i].node)
        print('The keys are :', self.keys)
        print('##############################################################################')

    '''
        This method gets value of successor node 
        Each node has 1 successor 
        @:return: successor node
        '''
    @property
    def successor(self):
        #print('successor is :', self.finger[1].node)
        return self.finger[1].node

    '''
        This method updates the successor 
        Param 1: node number which has to update as successor 
        '''
    @successor.setter
    def successor(self, id):
        self.finger[1].node = id

    '''
        This method updates the predecessor 
        Param 1: node number which has to update as predecessor 
        '''
    def update_predecessor(self, id):
        self.predecessor = id

    '''
        This method is used to find the successor of node, Id passed in the parameter
        Param 1: node number for which we want to get the successor
        @:return: node number'''
    def find_successor(self, id):
        np = self.find_predecessor(id)
        return self.call_rpc(np, 'successor')
    '''
        This method is used to find the predecessor of node Id passed in the parameter
        Param 1: node number for which we want to get the predecessor
        @:return: node number'''
    def find_predecessor(self, id):
        np = self.node
        while id not in ModRange(np + 1, self.call_rpc(np, 'successor') + 1, NODES): #TODO
            np = self.call_rpc(np, 'closest_preceding_finger', id)
        return np

    '''
        This method is used to the closest finger table entry for id
         Param 1: node number for which we want the closest node in finger table
         @:return: node number'''
    def closest_preceding_finger(self, id):
        n = self.node
        for i in range(M, 0, -1):
            if self.finger[i].node in ModRange(n+1, id, NODES):
                return self.finger[i].node
        return n
    '''
        This method is used is update the self.keys dictionary object
        which populate class request to save on the node
        Param 1: {key : value } pair which populate has send to save
        @:return: None'''
    def save_key_value(self, key_val):
        dic = pickle.loads(key_val)
        self.keys.update(dic)
        #return None this is the change
    '''
        This method is used to retrieve values from self.keys dictionary
        Param 1: key for which client has request to get its value
        @:return it return value'''
    def retrieve_value(self, key):
        if key in self.keys:
            value = self.keys[key]
        return value

    '''
        This method is used to execute 2 main tasks start_server and join_network
        from two threads
        Param 1: port number of existing node in the chord
        Param 2: node number which we want add into the chord network'''
    def create_threads(self, existing_node_address, n_hash_fun):
        t1 = threading.Thread(target = self.start_server, args=[n_hash_fun])
        t2 = threading.Thread(target = self.join_network, args=[existing_node_address, n_hash_fun])
        t1.start()
        t2.start()
        t1.join()
        t2.join()
    '''
        Starts up as a server.
        Allocates a new port in the localhost and starts listening
        It listens upto 1000 servers
        And diverts object to handle_rpc method'''
    def start_server( self, n_hash_fun):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('localhost', TEST_BASE + n_hash_fun))
        sock.listen(1000)
        while True:
            client, client_addr = sock.accept()
            self.handle_rpc(client)
    '''
        This method receives data from client object
        unpickle the dumps received from client object(or node in the chord)
        it dispatches the unpickled data to dispatch_rpc method  
        '''
    def handle_rpc(self, client):
        rpc = client.recv(BUF_SZ)
        method, arg1, arg2 = pickle.loads(rpc)
        result = self.dispatch_rpc(method, arg1, arg2)
        client.sendall(pickle.dumps(result))

    '''
        This method is used to send request to another node in the chord
        It call rpc if n != self.node else redirects request to dispatch_rpc
        param 1: node identifier 
        param 2: fun_to_invoked
        param 3 and param 4: parameters which is required for method we want execute'''
    def call_rpc(self, n, fun_to_invoked, request_param1=None, request_param2=None):
        if n == self.node:
            result = self.dispatch_rpc(fun_to_invoked, request_param1, request_param2)
        else:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(('localhost', TEST_BASE + n))
                s.sendall(pickle.dumps((fun_to_invoked, request_param1, request_param2)))
                response = s.recv(BUF_SZ)
                result = pickle.loads(response)
        return result

    ''' This method passes the request to specific function
    param 1: fun_to_invoked
    param 2 and param 3: parameters which is required for method we want execute'''
    def dispatch_rpc(self, fun_to_invoked, request_param1, request_param2):
        re = None
        if fun_to_invoked == 'find_successor':
            re = self.find_successor(request_param1)
        elif fun_to_invoked == 'predecessor':
            re = self.predecessor
        elif fun_to_invoked == 'update_predecessor':
            re = self.update_predecessor(request_param1)
        elif fun_to_invoked == 'successor':
            re = self.successor
        elif fun_to_invoked == 'update_finger_table':
            re = self.update_finger_table(request_param1, request_param2)
        elif fun_to_invoked == 'closest_preceding_finger':
            re = self.closest_preceding_finger(request_param1)
        elif fun_to_invoked == 'predecessor':
            re = self.predecessor
        elif fun_to_invoked == 'save_key_value':
            re = self.save_key_value(request_param1)
        elif fun_to_invoked == 'retrieve_value':
            re = self.retrieve_value(request_param1)
        elif fun_to_invoked == 'generate_keys':
            re = self.generate_keys(request_param1, request_param2)
        return re


'''Main method: requires two arguments
   One : port number of existing node in the chordNode
   Second : node number which we want to add in the chord
   0, 0 : to add the very first node in the chord'''
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python chord_node.py EXISTINGNODE NODENUMBER")
        exit(1)
    existing_node_address = int(sys.argv[1])
    node_number = int(sys.argv[2])
    chordNode = ChordNode(node_number)
    chordNode.create_threads(existing_node_address, node_number)







