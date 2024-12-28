import hashlib
from bisect import bisect, bisect_left, bisect_right
from storagenode import StorageNode

class ConsistentHashing:
    """
    This represents the array based implementation of Consistent Hashing.
    """

    def __init__(self):
        self.keys = []              # indices taken up in the ring
        self.nodes = []             # nodes present in the ring. nodes[i] is present at index keys[i]
        self.total_slots = 50       # total slots in the ring

    def hash_fn(self, key, total_slots):
        """
        Creates an integer equivalent to SHA256 hash and takes a modulo
        with the total number of slots.
        """
        hash = hashlib.sha256()

        # converting data into bytes and passing it to hash function
        hash.update(key.encode('utf-8'))

        # converting the HEX digest into equivalent integer value
        return int(hash.hexdigest(), 16) % total_slots
        

    def add_node(self, node: StorageNode):
        """
        Add a new node to the ring and returns the key
        from the hash space where it was placed.
        """

        if len(self.keys) == self.total_slots:
            raise Exception("Hash space is full. Cannot add more nodes.")
        
        key  = self.hash_fn(node.host, self.total_slots)

        # find the index where the key should be inserted in the keys array
        # this will be the index where the node should be inserted in the nodes array
        index = bisect(self.keys, key)

        # if we have already seen the key i.e node is already present in the ring
        # for the same key. We raise the collision exception
        if index > 0 and self.keys[index-1] == key:
            raise Exception("Collision detected. Node already present in the hash space.")

        # insert the node and the key at the same `index` location
        # this insertion will maintain the sorted order of keys
        self.keys.insert(index, key)
        self.nodes.insert(index, node)

        return key

    def remove_node(self, node: StorageNode):
        """
        Remove a node from the ring and returns the key from the hash space
        where it was placed.
        """

        # handling error when space is empty
        if len(self.keys) == 0:
            raise Exception("Hash space is empty. Cannot remove any node.")

        key = self.hash_fn(node.host, self.total_slots)

        # we find the index where the key would reside in the keys
        index = bisect_left(self.keys, key)

        # if key does not exist in the keys array, we raise an exception
        if index >= len(self.keys) or self.keys[index] != key:
            raise Exception("Node not present in the hash space.")

        # now that all sanity checks are done we popping the
        # keys and nodes at the index and thus removing presence of the node.
        self.keys.pop(index)
        self.nodes.pop(index)

        return key

    def assign(self, item):
        """
        Given the item, this function returns the node it is associated with
        """

        key = self.hash_fn(item, self.total_slots)

        # we find the first node to the right of this key
        # if bisect_right returns index which is out of bounds then
        # we circle back to the first in the array in a circular fashion.
        index = bisect_right(self.keys, key) % len(self.keys)

        # return the node present at that index
        return self.nodes[index]


if __name__ == '__main__':
    storage_nodes = [
        StorageNode(name='A', host='239.67.52.72'),
        StorageNode(name='B', host='137.70.131.229'),
        StorageNode(name='C', host='98.5.87.182'),
        StorageNode(name='D', host='11.225.158.95'),
        StorageNode(name='E', host='203.187.116.210'),
        StorageNode(name='F', host='107.117.238.203'),
        StorageNode(name='G', host='27.161.219.131')
    ]

    consistent_hashing = ConsistentHashing()

    for node in storage_nodes:
        consistent_hashing.add_node(node)

    for file in ['f1.txt', 'f2.txt', 'f3.txt', 'f4.txt', 'f5.txt']:
        print(f"file {file} resides on node {consistent_hashing.assign(file).name}")