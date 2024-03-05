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


class TraditionalHashing:
    def hash_fn(self, key):
        """
        The function sums the bytes present in the `key` and then
        take a mod with 5. This hash function thus generates output
        in the range [0, 4].
        """
        return sum(bytearray(key.encode('utf-8'))) % len(storage_nodes)

    def upload(self, path):
        index = self.hash_fn(path)
        node = storage_nodes[index]
        return node.put_file(path)

    def fetch(self, path):
        index = self.hash_fn(path)
        node = storage_nodes[index]
        return node.fetch_file(path)


for file in ['f1.txt', 'f2.txt', 'f3.txt', 'f4.txt', 'f5.txt']:
    print(f"file {file} resides on node {storage_nodes[TraditionalHashing().hash_fn(file)].name}")
