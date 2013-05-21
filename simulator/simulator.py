import sys
import argparse


def read_params(fd, param):
    cnt = 0
    num = len(param)
    while (cnt < num):
        line = fd.readline()
        if line and line != '':
            if (line.split(0) in param):
                param[line.split(0)] = line.split[1]
                cnt += 1
            else:
                print "Error: unknown configuration %s" % (line.split(0))
                exit()
        else:
            break
    if (cnt != num):
        print "Error: configuration input error"
        exit()


def read_status(fd):
    hosts = {}
    for i in range(param['hosts_number']):
        line = fd.readline()
        if line and line != "":
            host_name, ram = line.split(' ', 1)
            hosts[host_name] = {}
            hosts[host_name]['ram'] = float(ram)
            hosts[host_name]['deployment'] = {}
        else:
            print "Error: input format error"
            exit()
        for j in range(param['deployments_number']):
            line = fd.readline()
            if line and line != "":
                dep_name, datanodes, tasktrackers = line.split(' ', 2)
                hosts[host_name]['deployment'][dep_name] = {}
                hosts[host_name]['deployment'][dep_name]['datanodes'] = float(datanodes)
                hosts[host_name]['deployment'][dep_name]['tasktrackers'] = float(tasktrackers)
                hosts[host_name]['deployment'][dep_name]['scheduled'] = 0
            else:
                print "Error: input format error"
                exit()
    return hosts


param = {
    "ram_weight": 1.0,
    "datanodes_weight": 2.0,
    "tasktrackers_weight": 1.0,
    "corrective_weight": 1024.0,
    "hosts_number": 4,
    "deployments_number": 2
}

parser = argparse.ArgumentParser(description="Nova Application-Aware Scheduler Simulator")
parser.add_argument("input_fd", metavar='<input_file>', type=argparse.FileType('r'), default=sys.stdin, help='input file')
parser.add_argument("--out", dest='output_fd', metavar='<output_file>', type=argparse.FileType('w'), default=sys.stdout, help='output file')
parser.add_argument("--conf", dest='conf_fd', metavar='<conf_file>', type=argparse.FileType('r'), help='configuration file')
args = parser.parse_args()

# if (args.input != sys.stdin):
#     try:
#         in_fd = open(args.input, "r")
#     except IOError:
#         print "Error: unable to open %s" % (args.input)

# if (args.output != sys.stdout):
#     try:
#         out_fd = open(args.output, "w")
#     except IOError:
#         print "Error: unable to open %s" % (args.output)

# if args.conf:
#     try:
#         conf_fd = open(args.conf, "r")
#         read_params(conf_fd, param)
#     except IOError:
#         print "Error: unable to open %s" % (args.conf)

if args.conf_fd:
    read_params(args.conf_fd, param)

hosts = read_status(args.input_fd)
