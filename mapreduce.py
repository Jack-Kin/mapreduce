import collections
import multiprocessing
import pickle
import random
from enum import Enum
import threading
import lzma
import socket
import time

class Master:
    def __init__(self, file_list, words_dict, slave_id_list) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(("127.0.0.1", 60000))
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.listen(0)
        self.status = 0 # 1 map done 2 all done
        self.lock = threading.Lock()
        self.words_dict = words_dict
        self.dict_list = []
        self.file_status_dict = {}
        self.words_status_dict = {}
        for file in file_list:
            self.file_status_dict[file] = 'no_worker'
        for slave in words_dict.keys():
            self.words_status_dict[slave] = 'no_worker'
    def make_data(self, data):
        return lzma.compress(pickle.dumps(data))
    def checker(self, type, key):
        with self.lock:
            if type == 'map' and self.file_status_dict[key] == 'working':
                self.file_status_dict[key] = 'no_worker'
            elif type == 'reduce' and self.words_status_dict[key] == 'working':
                self.words_status_dict[key] = 'no_worker'

    def process_idle(self, data, client):
        with self.lock:
            if self.status == 0:
                for file, status in self.file_status_dict.items():
                    if status == 'no_worker':
                        self.file_status_dict[file] = 'working'
                        threading.Timer(15, self.checker, ['map', file]).start()
                        client.send(self.make_data({'type': 'map', 'file_name': file}))
                        break
                else:
                    client.send(self.make_data({'type': 'idle'}))
            elif self.status == 1:
                for slave, status in self.words_status_dict.items():
                    if status == 'no_worker':
                        self.words_status_dict[slave] = 'working'
                        threading.Timer(15, self.checker, ['reduce', slave]).start()
                        client.send(self.make_data({'type': 'reduce', 'dict_list': self.dict_list, 'word_list': self.words_dict[slave], 'slave_id': slave}))
                        break
                else:
                    client.send(self.make_data({'type': 'idle'}))
            elif self.status == 2:
                client.send(self.make_data({'type': 'done'}))
                return True
            return False
    def process_map_done(self, data, client):
        with self.lock:
            if self.status != 0:
                print('something wrong, map already finished')
            else:
                self.file_status_dict[data['file_name']] = 'done'
                if data['dict_name'] not in self.dict_list:
                    self.dict_list.append(data['dict_name'])
            if all(map(lambda x: x == 'done', self.file_status_dict.values())):
                self.status = 1
            client.send(self.make_data({'type': 'idle'}))
            return False
    def process_reduce_done(self, data, client):
        with self.lock:
            if self.status != 1:
                print('something wrong, not doing reduce')
            else:
                self.words_status_dict[data['slave_id']] = 'done'
            if all(map(lambda x: x == 'done', self.words_status_dict.values())):
                self.status = 2
                client.send(self.make_data({'type': 'done'}))
                print("Done!")
                return True
            client.send(self.make_data({'type': 'idle'}))
            return False

    def server(self, client):
        while True:
            data = pickle.loads(lzma.decompress(client.recv(4096)))
            t = data['type']
            done_send = eval(f'self.process_{t}')(data, client)
            if done_send:
                return
    def run(self):
        while True:
            client, _ = self.sock.accept()
            threading.Thread(target=self.server, args=(client,)).start() # when accept, start a server for it

class ErrorType(Enum):
    Normal = -1
    LateStart = 0
    DontStart = 1
    StartbutCrash = 2

class Slave:
    def __init__(self, my_id, control_arg: ErrorType) -> None:
        self.my_id = my_id
        self.ctrl = control_arg
        print(f"{self.my_id} run in {self.ctrl} mode")
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def send_recv(self, data):
        self.sock.send(lzma.compress(data))
        response = self.sock.recv(4096)
        return pickle.loads(lzma.decompress(response))

    def make_idle_data(self):
        return pickle.dumps({'type': 'idle', 'id': self.my_id})
    def make_map_data(self, process_file, save_dict):
        return pickle.dumps({'type': 'map_done', 'id': self.my_id, 'file_name': process_file, 'dict_name': save_dict})
    def make_reduce_data(self, process_slave):
        return pickle.dumps({'type': 'reduce_done', 'id': self.my_id, 'slave_id': process_slave})

    def do_map(self, file_name):
        print(f"{self.my_id} do map on file {file_name}")
        c = collections.Counter()
        with open(file_name) as f:
            c.update(f.read().split())
        with open(file_name+'.pkl', 'wb') as f:
            pickle.dump(dict(c), f)
    def do_reduce(self, dict_list, word_list, slave_id):
        print(f'{self.my_id} do reduce for {slave_id}')
        c = collections.Counter()            
        for file in dict_list:
            with open(file, 'rb') as f:
                c.update(pickle.load(f))
        with open(f'Ans-{slave_id}.txt', 'w') as f:
            for word in word_list:
                f.write(f'{word} {c[word]}\n')
    def do_crash(self):
        if self.ctrl == ErrorType.StartbutCrash and random.random() < 0.5:
            print(f'{self.my_id} crash')
            return True
        return False

    def run(self):
        if self.ctrl == ErrorType.DontStart:
            return
        if self.ctrl == ErrorType.LateStart:
            time.sleep(5)
        time.sleep(1)
        self.sock.connect(('127.0.0.1', 60000))
        try:
            while True:
                time.sleep(1)
                if self.do_crash():
                    print('when do idle')
                    break
                ret = self.send_recv(self.make_idle_data())
                if ret['type'] == 'done':
                    break
                if ret['type'] == 'map':
                    if self.do_crash():
                        print('when do map')
                        break
                    self.do_map(ret['file_name'])
                    ret = self.send_recv(self.make_map_data(ret['file_name'], ret['file_name']+'.pkl'))
                elif ret['type'] == 'reduce':
                    if self.do_crash():
                        print('when do reduce')
                        break
                    self.do_reduce(ret['dict_list'], ret['word_list'], ret['slave_id'])
                    ret = self.send_recv(self.make_reduce_data(ret['slave_id']))
                if ret['type'] == 'done':
                    break
        finally:
            print(f"{self.my_id} close")
            self.sock.close()

def generate_word_list(count, count1):
    base = ['a', 'b', 'c', 'd', 'e', 'f', 'z']
    word_list = []
    ans = []
    for x in base:
        for i in range(1, 5):
            word_list.append(x*i)
    for i in range(count):
        with open(f'file_{i}.txt', 'w') as f:
            for i in range(random.randint(100, 1000)):
                word = random.choice(word_list)
                ans.append(word)
                f.write(word + ' ')
    c = collections.Counter(ans)
    words_dict = {}
    for i in range(count1):
        words_dict[f'Slave{i}'] = random.sample(word_list, random.randint(1, len(word_list)))
        print(f'##########   Slave{i} ##############')
        for word in words_dict[f'Slave{i}']:
            print(f'{word} {c[word]}')
    with open('req_words.pkl', 'wb') as f:
        pickle.dump(words_dict, f)

if __name__ == '__main__':
    file_count = 10
    generate_word_list(file_count, 4)
    pending_file_list = [f'file_{i}.txt' for i in range(file_count)]
    slave_id_list = [f'Slave{i}' for i in range(3)]
    with open('req_words.pkl', 'rb') as f:
        words_dict = pickle.load(f)  # {'Slave1': ['a', 'and'], 'Slave2': ['Alice', 'Bob']}

    # Create a Master
    m = Master(pending_file_list, words_dict, slave_id_list)
    # Create some slaves
    s_list = [Slave(slave_id, ErrorType(random.randint(0, 2))) for slave_id in slave_id_list[:-1]]
    s_list.append(Slave(slave_id_list[-1], ErrorType(-1))) # assert one slave run in normal

    # run in multiprocess
    m_t = threading.Thread(target=m.run)
    m_t.daemon = True
    s_p_list = [multiprocessing.Process(target=s.run) for s in s_list]
    m_t.start()
    for p in s_p_list:
        p.start()
    for p in s_p_list:
        p.join()
