import hashlib
import pickle
import socket
import sys
import rpyc

TEST_BASE = 43544
BUF_SZ = 4096  # socket recv arg
M = 3  # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
''' 
    Chord query class which is used to 
    retrieve the key : value pair from the nodes present 
    in the chord network'''
class chord_query():
    '''
        This method: finds the node which holds the value for key 
        Param 1, Param 2: port of existing node, key for which we have retrieve the value
        @:return: node number from where we have to get the value of requested key'''
    def find_key(self, existing_port, key):
        hash_key = self.convert_hash(key)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', existing_port))
            s.sendall(pickle.dumps(('find_successor', hash_key, None)))
            response = s.recv(BUF_SZ)
            node_add = pickle.loads(response)
            self.retrieve_val(node_add, hash_key)
            return node_add

    '''
        This method is used get the value from node
        Param 1: number number where key is stored
        Param 2: key 
        @:return: value from node'''
    def retrieve_val(self, node_add, key):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', node_add+TEST_BASE))
            s.sendall(pickle.dumps(('retrieve_value', key, None)))
            response = s.recv(BUF_SZ)
            value = pickle.loads(response)
            print('##################################################################')
            print('{key: value}', '{', key, ':', value,'}')
            print('##################################################################')
            return value
    '''
        This methos is used to convert the hash value 
        Param 1: value which has to be hashed
        @:return: int value of key'''
    def convert_hash(self, key_to_value):
        result = hashlib.sha1(key_to_value.encode())
        n = result.hexdigest()
        i = int(n, 16)
        print(i)
        val = divmod(i, 2**M)
        key = val[1]
        return key
'''
    Main method : requires two arguments
    Argument 1: existing port number
    Argument 2: key for which we want to get value from chord network'''
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python chord_query.py EXISTINGNODE_PORT and KEY")
        exit(1)
    existing_node_port = int(sys.argv[1])
    key = sys.argv[2]
    chordquery = chord_query()
    chordquery.find_key(existing_node_port, key)

