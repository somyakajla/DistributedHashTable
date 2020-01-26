import csv
import hashlib
import pickle
import socket
import sys


TEST_BASE = 43544
BUF_SZ = 4096  # socket recv arg
M = 3 # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8

''' 
    Chord populate class which is used to 
    save the key : value to the node present 
    in the chord network'''
class chord_populate():
    '''
        This method reads the data from CSV file 
        It extract key and value from each row in the csv file
        It hash the key through SHA1
        It calls look_up method where calculated key should be saved
        And then save key: value to that node'''
    def open_file(self, existing_port, file_name):
        with open(file_name) as csvfile:
            readCSV = csv.reader(csvfile, delimiter=',')
            for row in readCSV:
                row_key = row[0] + row[3]
                row_value =''
                for i in [x for x in range(1, len(row)) if x != 3]:
                    row_value += row[i]
                result = hashlib.sha1(row_key.encode())
                n = result.hexdigest()
                i = int(n, 16)
                val = divmod(i, 2**M)
                key = val[1]
                node_add = self.look_up(existing_port, key)
                self.save_key(node_add, key, row_value)
    '''
        This method is used to lookup for the node where respected key should be saved
        calls the find successor method to get the node number
        Param 1 and Param 2: port number of existing node, key for which we want to save the key: value pair
        find_successor : will be the node where keys has to be saved
        @:return: node number'''
    def look_up(self, existing_port, key):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', existing_port))
            s.sendall(pickle.dumps(('find_successor', key, None)))
            response = s.recv(BUF_SZ)
            result = pickle.loads(response)
        return result

    '''
        This method save key: value pair to the nodes
        Param 1, Param 2  and Param 3: node number where key value pair has to saved'''
    def save_key(self, node_add, key, row_value):
        #print(node_add, key, row_value)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', node_add+TEST_BASE))
            dic = {key: row_value}
            re = pickle.dumps(dic)
            s.sendall(pickle.dumps(('save_key_value', re, None)))
        #     response = s.recv(BUF_SZ)
        #     result = pickle.loads(response)
        # return result  ' this is the another change'

'''
    Main method: requires 2 arguments
    Argument 1: existing port number 
    Argument 2: file name which has to hashed and send to the chord nodes'''
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python chord_populate.py EXISTINGNODE_PORT and FILE_NAME")
        exit(1)
    existing_node_port = int(sys.argv[1])
    file_name = sys.argv[2]
    chordpopulate = chord_populate()
    chordpopulate.open_file(existing_node_port, file_name)

    # '/Users/somyakajla/Documents/distributed/lab4/hello.csv'