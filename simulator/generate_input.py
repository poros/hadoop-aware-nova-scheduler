import sys

if (len(sys.argv) < 6):
    print 'usage: ' + sys.argv[0] + ' <output_file> <hosts_number> <hosts_ram> <deployments_number> [ <deployment_tasktrackers>:<tasktrackers_ram>...]'
    exit()

hosts_number = int(sys.argv[2])
hosts_ram = int(sys.argv[3])
deployments_number = int(sys.argv[4])
if ((len(sys.argv) - 5) != deployments_number):
    print 'usage: ' + sys.argv[0] + ' <output_file> <hosts_number> <hosts_ram> <deployments_number> [ <deployment_tasktrackers>:<tasktrackers_ram>...]'
    exit()

try:
    fout = open(sys.argv[1], 'w')
except IOError:
    print 'Error: unable to open ' + sys.argv[1]
    exit()

deployments = []
total_tt = 0
for i in range(5, len(sys.argv)):
    dep = [int(x) for x in sys.argv[i].split(':', 1)]
    deployments.append(dep)
    total_tt += dep[0]

fout.write("%d %d %d\n" % (hosts_number, deployments_number, total_tt))
for i in range(0, hosts_number):
    fout.write('host%s %d\n' % (i, hosts_ram))
    for j in range(0, deployments_number):
        fout.write('deploy%s 1 0\n' % (j))

fout.write("\n")

cnt = 0
for i in range(0, deployments_number):
    for j in range(0, deployments[i][0]):
        fout.write('inst%s deploy%s %d\n' % (cnt, i, deployments[i][1]))
        cnt += 1

fout.close()
