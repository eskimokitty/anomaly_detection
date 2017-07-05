from collections import defaultdict, deque
from os.path import abspath, exists
import numpy
import json
import sys
import heapq

def load_file(file_name):
    file_path = abspath(file_name)
    data = []
    with open(file_path) as f:
        for line in f:
            try:
                current_line = json.loads(line)
                data.append(json.loads(line))
            except ValueError:
                continue
    return data


def get_friend_purchases(friends, purchases, d, t, id):
    # Get D level all friends in all_friends set
    all_friends = set()
    q = deque()
    for fr in friends[id]:
        q.append(fr)
    level = 0
    while level < d:
        s = len(q)
        for _ in range(s):
            u = q.popleft()
            if u in all_friends or u == id:
                continue
            all_friends.add(u)
            for nb in friends[u]:
                q.append(nb)
        level+=1
    # Merge all friends purchase history
    # Time complexity is O(n), all purchases history are already sorted
    # Merge them while keep only the latest T transactions
    r = []
    for f in all_friends:
        r = heapq.merge(r,purchases[f])
        r = heapq.nlargest(t, r)
    return [i[1] for i in r]


def process_line(friends, purchases, d, t, line, output=False, flagged_purchases=None):
    # Process purchase log
    if line['event_type'] == 'purchase':
        id, amount = line['id'], float(line['amount'])
        # When new purchase log comming in, add to purchase history heapq
        # Time complexity: O(log(T)) which is a constant number
        heapq.heappush(purchases[id], (line['timestamp'],amount))
        # Keep the most recent T purchase transactions
        # If size is > T, pop the one with smallest timestamp
        if len(purchases[id]) > t:
            heapq.heappop(purchases[id])
        if output:
            # Get most recent T transaction history in D level of friend network
            recent_purchases = get_friend_purchases(friends, purchases, d, t, id)
            # Flag big purchases and add them in output list
            std = numpy.std(recent_purchases)
            mean = numpy.mean(recent_purchases)
            if amount > mean + 3*std:
                line['mean'] = "{:0.2f}".format(mean)
                line['sd'] = "{:0.2f}".format(std)
                flagged_purchases.append(line)
                print(flagged_purchases)
    # Process friend network update log
    elif line['event_type'] == 'befriend':
        friends[line['id1']].add(line['id2'])
        friends[line['id2']].add(line['id1'])
    elif line['event_type'] == 'unfriend':
        friends[line['id1']].remove(line['id2'])
        friends[line['id2']].remove(line['id1'])
    

def process():
    # Get input variables and data
    init_data = load_file(sys.argv[1])
    stream_data = load_file(sys.argv[2])
    output_file = sys.argv[3]
    d, t = int(init_data[0]['D']), int(init_data[0]['T'])
    init_data = init_data[1:]

    # Initiate data
    # Friend Map to track friending network key: user_id, value: set of friend ids
    friends = defaultdict(set) 
    # Purchases_history Map to track the most recent T purchase transactions for each user_id
    # Key: user_id, Value: Heapq of most recent T trasactions for this user
    # Choosing to use heapq over deque because log data can come in out of order
    #     Eg: 14:05:25 purchase log can arrive ealier than 14:05:20 purchase log for a users
    purchases = defaultdict(list)
    # Flagged big purchases list
    flagged_purchases = []

    # Process batch data
    for line in init_data:
        process_line(friends, purchases, d, t,line, output=False)
    
    # Process streaming data
    for line in stream_data:
        process_line(friends, purchases, d, t,line, output=True, flagged_purchases=flagged_purchases)
    print(flagged_purchases)
    
    # Generate ouptut
    with open(output_file, 'w') as outfile:
        for i in flagged_purchases:
            json.dump(i, outfile)
print(process())