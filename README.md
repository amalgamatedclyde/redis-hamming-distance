# redis-hamming-distance

Computes the hamming distance of two redis bitarrys with option masking even if these bitarrays are spread accross multiple hosts. 

# Install

	pip install redishammingdist

# Usage

	>>from redishammingdist import Rhd
	>>import redis
	>>conn1 = redis.StrictRedis(
	    host='localhost', port=6379, db=0)
	>>conn2 = redis.StrictRedis(
	    host='localhost', port=6379, db=1)
	>>connections = [conn1, conn2]	
	>>rc=Rhd(connections)
	>>rc.setbit(['k1']*5,[0,1,2,3,4],[1,1,1,0,0])
	>>rc.setbit(['k2']*5,[0,1,2,3,4],[0,1,0,1,0])
	>>rc.hamming_dist('k1','k2')  
	3

You can also use masks to ignore positions. set mask bits to 0 to skip positions. e.g.

	>> rc.setbit_mask('k1',0,0)
	>> rc.hamming_dist('k1','k2')
	2
