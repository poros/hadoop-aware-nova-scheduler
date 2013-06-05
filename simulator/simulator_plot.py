import sys
import re
import matplotlib.pyplot as plt

if (len(sys.argv) != 2):
    print 'usage ' + sys.argv[0] + '<input_file>'
    exit()

try:
    fin = open(sys.argv[1], 'r')
except IOError:
    print 'Error: unable to open ' + sys.argv[1]
    exit()

cnt = 0
xdata = []
random = []
greedy = []
for line in fin:
    if (cnt % 3) == 0:
        xdata.append(int(line))
    elif (cnt % 3) == 1:
        random.append(float(re.search('RANDOM ([0-9.]+)', line).group(1)))
        # print re.search('RANDOM ([0-9.]+)', line).group(1)
    elif (cnt % 3) == 2:
        greedy.append(float(re.search('GREEDY ([0-9.]+)', line).group(1)))
        # print re.search('GREEDY ([0-9.]+)', line).group(1)
    cnt = cnt + 1

plt.figure(1)
plt.plot(xdata, random, 'ro', xdata, greedy, 'bo')
plt.xlim(10, 1000)
plt.xlabel('hosts')
plt.ylabel('cost')
plt.legend(['Random', 'Greedy'], loc='center right')

plt.figure(2)
plt.plot(xdata, [x - y for x, y in zip(random, greedy)], 'g.')
plt.xlim(10, 1000)
plt.xlabel('hosts')
plt.ylabel('cost difference')
plt.legend(['Random - Greedy'], loc='center right')
plt.show()

fin.close()
