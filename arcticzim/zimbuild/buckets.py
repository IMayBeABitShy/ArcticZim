"""
This module contains functionality to create batches from a stream of data.
"""


class BucketMaker(object):
    """
    The BucketMaker takes single elements and creates new buckets from them.

    @ivar maxsize: max size of a bucket
    @type maxsize: L{int}
    """
    def __init__(self, maxsize):
        """
        The default constructor.

        @param maxsize: max size of a bucket
        @type maxsize: L{int}
        """
        assert isinstance(maxsize, int) and maxsize > 0
        self.maxsize = maxsize
        self._cur_bucket = []

    def feed(self, element):
        """
        Add a new element to the current bucket.

        If the current bucket will be full as part of the task, return
        the bucket and start a new one. Otherwise, return None.


        @param element: element to put into a bucket
        @type element: any
        @return: the bucket if the bucket is now full else L{None}
        @rtype: L{list} if bucket is full else L{None}
        """
        self._cur_bucket.append(element)
        if len(self._cur_bucket) >= self.maxsize:
            # new bucket
            bucket = self._cur_bucket
            self._cur_bucket = []
            return bucket
        return None

    def finish(self):
        """
        If the current bucket is not empty, return it.

        @return: the current bucket if it is not empty else L{None}
        @rtype: L{list} or L{None}
        """
        if self._cur_bucket:
            bucket = self._cur_bucket
            self._cur_bucket = []
            return bucket
        return None
