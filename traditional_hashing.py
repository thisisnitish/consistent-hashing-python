from storagenode import StorageNode

class TraditionalHashing:
    
    storage_nodes: StorageNode = None

    def __init__(self, storage_nodes):
        self.storage_nodes = storage_nodes

    def hash_fn(self, key):
        """
        The function sums the bytes present in the `key` and then
        take a mod with n (number of storage node). This hash function 
        thus generates output in the range [0, n-1].
        """
        return sum(bytearray(key.encode('utf-8'))) % len(self.storage_nodes)
    
    def upload_file(self, path):
        index = self.hash_fn(path)  # get the index of the storage node where the file will be stored using hash function
        storage_node = self.storage_nodes[index] # get the storage node
        return storage_node.put_file(path) # upload the file to the storage node
    
    def fetch_file(self, path):
        index = self.hash_fn(path)  # get the index of the storage node where the file will be stored using hash function
        storage_node = self.storage_nodes[index] # get the storage node
        return storage_node.fetch_file(path) # fetch the file from the storage node


if __name__ == '__main__':

    print("Traditional Hashing with 5 Storage Nodes\n")
    storage_nodes = [
        StorageNode(name='A', host='239.67.52.72'),
        StorageNode(name='B', host='137.70.131.229'),
        StorageNode(name='C', host='98.5.87.182'),
        StorageNode(name='D', host='11.225.158.95'),
        StorageNode(name='E', host='203.187.116.210'),
    ]
    traditional_hashing_5 = TraditionalHashing(storage_nodes=storage_nodes)
    for file in ['f1.txt', 'f2.txt', 'f3.txt', 'f4.txt', 'f5.txt']:
        print(f"file {file} resides on node {traditional_hashing_5.storage_nodes[traditional_hashing_5.hash_fn(file)].name}")

    print("\nAfter Scalling the Storage Nodes\n")
    print("Traditional Hashing with 7 Storage Nodes\n")

    # SCALLING THE STORAGE NODES
    storage_nodes.append(StorageNode(name='F', host='107.117.238.203'))
    storage_nodes.append(StorageNode(name='G', host='27.161.219.131'))

    traditional_hashing_7 = TraditionalHashing(storage_nodes=storage_nodes)
    for file in ['f1.txt', 'f2.txt', 'f3.txt', 'f4.txt', 'f5.txt']:
        print(f"file {file} resides on node {traditional_hashing_7.storage_nodes[traditional_hashing_7.hash_fn(file)].name}")


