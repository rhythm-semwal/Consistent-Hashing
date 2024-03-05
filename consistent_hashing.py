import hashlib
from bisect import bisect, bisect_left, bisect_right

import requests


class StorageNode:
    def __init__(self, name=None, host=None):
        self.name = name
        self.host = host

    def fetch_file(self, path):
        return requests.get(f'https://{self.host}:1231/{path}').text

    def put_file(self, path):
        with open(path, 'r') as fp:
            content = fp.read()
            return requests.post(f'https://{self.host}:1231/{path}', body=content).text


storage_nodes = [
    StorageNode(name='A', host='239.67.52.72'),
    StorageNode(name='B', host='137.70.131.229'),
    StorageNode(name='C', host='98.5.87.182'),
    StorageNode(name='D', host='11.225.158.95'),
    StorageNode(name='E', host='203.187.116.210'),
]


class ConsistentHashing:
    def __init__(self):
        self._keys = []  # represents the hash keys for each node. keys[i] = nodes[i]
        self.nodes = []  # represents the nodes present in the ring
        self.total_slots = 25  # total slots in the ring

    def hash_fn(self, key):
        """
        hash_fn creates an integer equivalent of a SHA256 hash and
        takes a modulo with the total number of slots in hash space.
        """
        hsh = hashlib.sha256()

        # converting data into bytes and passing it to hash function
        hsh.update(bytes(key.encode('utf-8')))

        # converting the HEX digest into equivalent integer value
        return int(hsh.hexdigest(), 16) % self.total_slots

    def add_node(self, node: StorageNode):
        if len(self._keys) == self.total_slots:
            raise "Hash Ring is Full"

        key = self.hash_fn(node.host)

        index = bisect(self._keys, key)

        self.nodes.insert(index, node)
        self._keys.insert(index, key)

        return key

    def remove_node(self, node: StorageNode):
        if len(self._keys) == 0:
            raise "Hash Space is Empty"

        key = self.hash_fn(node.host)

        index = bisect_left(self._keys, key)

        if index >= len(self._keys) or self._keys[index] != key:
            raise Exception("node does not exist")

        self.nodes.pop(index)
        self._keys.pop(index)

        return key

    def assign(self, item):
        """Given an item, the function returns the node it is associated with.
        """
        key = self.hash_fn(item)

        index = bisect_right(self._keys, key) % len(self._keys)

        return self.nodes[index]

    @property
    def keys(self):
        return self._keys


ch = ConsistentHashing()
for node in storage_nodes:
    ch.add_node(node)

print(ch.keys)
print(ch.nodes)

for file in ['f1.txt', 'f2.txt', 'f3.txt', 'f4.txt', 'f5.txt']:
    print(f"file {file} resides on node {ch.assign(file).name}")
