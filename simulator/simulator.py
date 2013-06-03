import sys
import argparse
import copy
import unittest


class Instance:
    '''
    A tasktracker to be scheduled
    '''
    def __init__(self, name, deploy_name, ram):
        self.name = name
        self.deploy_name = deploy_name
        self.ram = ram

    def to_string(self):
        return "instance %s RAM %d\n" % (self.name, self.ram)


class Hadoop_Host_Deployment:
    '''
    An Hadoop deployment on a certain host
    '''
    def __init__(self, name, datanodes=0, tasktrackers=0):
        self.name = name
        self._datanodes = datanodes
        self._tasktrackers = tasktrackers
        self._instances = []

    @property
    def datanodes(self):
        return self._datanodes

    @property
    def tasktrackers(self):
        return self._tasktrackers + len(self._instances)

    def get_instances(self):
        return self._instances

    def instances_num(self):
        return len(self._instances)

    def schedule_instance(self, instance):
        if (instance):
            self._instances.append(instance)

    def undo_schedule_instance(self, instance):
        if (instance):
            self._instances.pop()

    #NOTE: the term 'tasktrackers' refers to actual ones, not originals
    def to_string(self):
        res = "deployment %s datanodes %d tasktrackers %d\n" % (self.name, self.datanodes, self.tasktrackers)
        for inst in self._instances:
            res += "%s" % (inst.to_string())
        return res

    #NOTE: the term 'tasktrackers' refers to actual ones, not originals
    def __repr__(self):
        res = "datanodes " + str(self._datanodes) + " tasktrackers " + str(self.tasktrackers) + "\n"
        for inst in self._instances:
            res += "\t|-" + inst.name + "\n"
        return res


class Host:
    '''
    An host in the cluster
    '''
    def __init__(self, name, free_ram=0):
        self.name = name
        self.free_ram = free_ram
        self._deployments = {}
        self.cost = 0

    def add_deployment(self, deploy_name, datanodes=0, tasktrackers=0):
        if (deploy_name in self._deployments):
            print "Error: deployment already present on the host"
            exit()
        self._deployments[deploy_name] = Hadoop_Host_Deployment(deploy_name, datanodes, tasktrackers)

    def get_deployment(self, deploy_name):
        return self._deployments.get(deploy_name)

    def compute_instance_cost(self, instance, param):
        cost = 0
        cost += param['ram_weight'] * self.free_ram
        deploy = self.get_deployment(instance.deploy_name)
        if (deploy):
            cost += param['corrective_weight'] * param['datanodes_weight'] * float(deploy.datanodes)
            cost += param['corrective_weight'] * param['tasktrackers_weight'] * float(deploy.tasktrackers)
        return cost

    def compute_host_cost(self, param):
        cost = 0
        cost += param['ram_weight'] * self.free_ram
        for deploy in self._deployments.values():
            cost += param['corrective_weight'] * param['datanodes_weight'] * float(deploy.datanodes) * float(deploy.instances_num())
            cost += param['corrective_weight'] * param['tasktrackers_weight'] * (float(deploy.tasktrackers) - 1.0) * float(deploy.instances_num())
        return cost

    def schedule_instance(self, instance):
        deploy = self.get_deployment(instance.deploy_name)
        deploy.schedule_instance(instance)
        self.free_ram -= instance.ram

    def undo_schedule_instance(self, instance):
        deploy = self.get_deployment(instance.deploy_name)
        deploy.undo_schedule_instance(instance)
        self.free_ram += instance.ram

    def to_string(self):
        res = 'host %s free_RAM %d cost %f\n' % (self.name, self.free_ram, self.cost)
        for deploy in self._deployments.values():
            res += deploy.to_string()
        return res

    def __repr__(self):
        cost = self.compute_host_cost(param)
        res = self.name + "\tFree RAM " + str(self.free_ram) + '\tHost Cost ' + str(cost) + "\n"
        for deploy_name in self._deployments.keys():
            res += "|-" + deploy_name + " " + self._deployments[deploy_name].__repr__()
        return res


def read_params(fd, param):
    cnt = 0
    num = len(param)
    while (cnt < num):
        line = fd.readline()
        if line:
            param_name, param_value = line.split(' ', 1)
            if (param_name in param):
                param[param_name] = float(param_value) if '.' in param_value else int(param_value)
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
    hosts = []
    line = fd.readline()
    if line:
        param['hosts_number'], param['deployments_number'], param['instances_number'] = [int(x) for x in line.split(' ', 2)]
    else:
        print "Error: input format error"
        exit()
    for i in range(param['hosts_number']):
        line = fd.readline()
        if line:
            host_name, ram = line.split(' ', 1)
            host = Host(host_name, int(ram) * 1024)
        else:
            print "Error: input format error"
            exit()
        for j in range(param['deployments_number']):
            line = fd.readline()
            if line and line != "":
                deploy_name, datanodes, tasktrackers = line.split(' ', 2)
                host.add_deployment(deploy_name, int(datanodes), int(tasktrackers))
            else:
                print "Error: input format error"
                exit()
        hosts.append(host)

    instances = []
    for line in fd:
        if line and line != '\n':
            inst_name, deploy_name, ram = line.split(' ', 2)
            instances.append(Instance(inst_name, deploy_name, int(ram) * 1024))
    if (len(instances) != param['instances_number']):
        print "Error: input format error"
        exit()

    return hosts, instances


def greedy_scheduler(instances, status, param):
    hosts = copy.deepcopy(status)
    for inst in instances:
        # print inst.name
        max_cost, max_host = None, None
        for host in hosts:
            cost = host.compute_instance_cost(inst, param)
            if (not max_cost or max_cost < cost):
                max_cost = cost
                max_host = host
        # print max_cost/1024, max_host.name
        max_host.schedule_instance(inst)
    return hosts


def recursive_schedule(instances, hosts, param, max_cost, max_hosts, level):
        if (level == len(instances)):
            cost = 0
            for host in hosts:
                cost += host.compute_host_cost(param)
            if (not max_cost or max_cost < cost):
                max_cost = cost
                max_hosts = copy.deepcopy(hosts)
            return max_cost, max_hosts

        for host in hosts:
            host.schedule_instance(instances[level])
            max_cost, max_hosts = recursive_schedule(instances, hosts, param, max_cost, max_hosts, level+1)
            host.undo_schedule_instance(instances[level])
        return max_cost, max_hosts


def optimal_scheduler(instances, status, param):
    hosts = copy.deepcopy(status)
    max_cost, max_hosts = None, None
    max_cost, max_hosts = recursive_schedule(instances, hosts, param, max_cost, max_hosts, 0)
    return max_cost, max_hosts


def system_unbalance_index(hosts):
    ram = 0
    for h in hosts:
        ram += h.free_ram
    avg_ram = float(ram) / float(len(hosts))
    unbalance = 0
    for h in hosts:
        unbalance += abs(avg_ram - float(h.free_ram))
    unbalance_index = unbalance / float(ram)
    return unbalance_index

# MAIN PROGRAM

param = {
    "ram_weight": 1.0,
    "datanodes_weight": 2.0,
    "tasktrackers_weight": 1.0,
    "corrective_weight": 1024.0
}

parser = argparse.ArgumentParser(description="Nova Application-Aware Scheduler Simulator")
parser.add_argument("input_fd", metavar='<input_file>', type=argparse.FileType('r'), default=sys.stdin, help='input file')
parser.add_argument("--out", dest='output_fd', metavar='<output_file>', type=argparse.FileType('w'), default=sys.stdout, help='output file')
parser.add_argument("--conf", dest='conf_fd', metavar='<conf_file>', type=argparse.FileType('r'), help='configuration file')
parser.add_argument("--test", dest='test_no', metavar='<unit_test#>', type=int, help='unit test #')
parser.add_argument("--verbose", "-v", dest='verbose', action='store_true', help='verbose (and more human readable) solution visualization')

args = parser.parse_args()

if args.conf_fd:
    read_params(args.conf_fd, param)

hosts, instances = read_status(args.input_fd)

greedy_solution = greedy_scheduler(instances, hosts, param)

optimal_cost, optimal_solution = optimal_scheduler(instances, hosts, param)

original_unbalance_index = system_unbalance_index(hosts)
# print original_unbalance_index
greedy_unbalance_index = system_unbalance_index(greedy_solution)
# print greedy_unbalance_index
greedy_unbalance_diff = (greedy_unbalance_index - original_unbalance_index) / original_unbalance_index
optimal_unbalance_index = system_unbalance_index(optimal_solution)
# print optimal_unbalance_index
optimal_unbalance_diff = (optimal_unbalance_index - original_unbalance_index) / original_unbalance_index

args.output_fd.write('GREEDY\n')
greedy_cost = 0
for i in greedy_solution:
    i.cost = i.compute_host_cost(param)
    greedy_cost += i.cost
    args.output_fd.write(i.to_string())
args.output_fd.write('total_cost %f\n' % greedy_cost)
args.output_fd.write('relative_unbalance_variation %f\n' % greedy_unbalance_diff)

args.output_fd.write('OPTIMAL\n')
for i in optimal_solution:
    i.cost = i.compute_host_cost(param)
    args.output_fd.write(i.to_string())
args.output_fd.write('total_cost %f\n' % optimal_cost)
args.output_fd.write('relative_unbalance_variation %f\n' % optimal_unbalance_diff)
if args.verbose:
    print '################'
    print 'GREEDY SCHEDULER\n'
    for i in greedy_solution:
        print i
    print 'TOTAL COST ', greedy_cost
    print 'RELATIVE UNBALANCE VARIATION', greedy_unbalance_diff
    print '################'
    print 'OPTIMAL SCHEDULER\n'
    for i in optimal_solution:
        print i
    print 'TOTAL COST ', optimal_cost
    print 'RELATIVE UNBALANCE VARIATION', optimal_unbalance_diff
    print '################'


#############################################################
#                        UNIT TESTS                         #
#############################################################

# UNIT TEST 1 - GREEDY

if args.test_no and args.test_no == 1:

    # HOST A
    assert greedy_solution[0].name == 'hostA'
    assert greedy_solution[0].free_ram == 11.0 * 1024.0
    assert greedy_solution[0].get_deployment('hadoop1').datanodes == 0
    assert greedy_solution[0].get_deployment('hadoop1').tasktrackers == 2
    assert len(greedy_solution[0].get_deployment('hadoop1').get_instances()) == 2
    assert greedy_solution[0].get_deployment('hadoop1').get_instances()[0].name == 'instA'
    assert greedy_solution[0].get_deployment('hadoop1').get_instances()[1].name == 'instB'
    assert greedy_solution[0].get_deployment('hadoop2').datanodes == 0
    assert greedy_solution[0].get_deployment('hadoop2').tasktrackers == 0
    assert len(greedy_solution[0].get_deployment('hadoop2').get_instances()) == 0

    # HOST B
    assert greedy_solution[1].name == 'hostB'
    assert greedy_solution[1].free_ram == 8.0 * 1024.0
    assert greedy_solution[1].get_deployment('hadoop1').datanodes == 2
    assert greedy_solution[1].get_deployment('hadoop1').tasktrackers == 1
    assert len(greedy_solution[1].get_deployment('hadoop1').get_instances()) == 0
    assert greedy_solution[1].get_deployment('hadoop2').datanodes == 1
    assert greedy_solution[1].get_deployment('hadoop2').tasktrackers == 0
    assert len(greedy_solution[1].get_deployment('hadoop2').get_instances()) == 0

    # HOST C
    assert greedy_solution[2].name == 'hostC'
    assert greedy_solution[2].free_ram == 8.0 * 1024.0
    assert greedy_solution[2].get_deployment('hadoop1').datanodes == 1
    assert greedy_solution[2].get_deployment('hadoop1').tasktrackers == 1
    assert len(greedy_solution[2].get_deployment('hadoop1').get_instances()) == 0
    assert greedy_solution[2].get_deployment('hadoop2').datanodes == 3
    assert greedy_solution[2].get_deployment('hadoop2').tasktrackers == 3
    assert len(greedy_solution[2].get_deployment('hadoop2').get_instances()) == 1
    assert greedy_solution[2].get_deployment('hadoop2').get_instances()[0].name == 'instC'

    # HOST D
    assert greedy_solution[3].name == 'hostD'
    assert greedy_solution[3].free_ram == -10.0 * 1024.0
    assert greedy_solution[3].get_deployment('hadoop1').datanodes == 0
    assert greedy_solution[3].get_deployment('hadoop1').tasktrackers == 0
    assert len(greedy_solution[3].get_deployment('hadoop1').get_instances()) == 0
    assert greedy_solution[3].get_deployment('hadoop2').datanodes == 0
    assert greedy_solution[3].get_deployment('hadoop2').tasktrackers == 0
    assert len(greedy_solution[3].get_deployment('hadoop2').get_instances()) == 0


# UNIT TEST INPUT2 -GREEDY

if args.test_no and args.test_no == 2:

    # HOST A
    assert greedy_solution[0].name == 'hostA'
    assert greedy_solution[0].free_ram == 13.0 * 1024.0
    assert greedy_solution[0].get_deployment('hadoop1').datanodes == 0
    assert greedy_solution[0].get_deployment('hadoop1').tasktrackers == 1
    assert len(greedy_solution[0].get_deployment('hadoop1').get_instances()) == 1
    assert greedy_solution[0].get_deployment('hadoop1').get_instances()[0].name == 'instA'
    assert greedy_solution[0].get_deployment('hadoop2').datanodes == 0
    assert greedy_solution[0].get_deployment('hadoop2').tasktrackers == 1
    assert len(greedy_solution[0].get_deployment('hadoop2').get_instances()) == 1
    assert greedy_solution[0].get_deployment('hadoop2').get_instances()[0].name == 'instC'

    # HOST B
    assert greedy_solution[1].name == 'hostB'
    assert greedy_solution[1].free_ram == 8.0 * 1024.0
    assert greedy_solution[1].get_deployment('hadoop1').datanodes == 2
    assert greedy_solution[1].get_deployment('hadoop1').tasktrackers == 1
    assert len(greedy_solution[1].get_deployment('hadoop1').get_instances()) == 0
    assert greedy_solution[1].get_deployment('hadoop2').datanodes == 1
    assert greedy_solution[1].get_deployment('hadoop2').tasktrackers == 0
    assert len(greedy_solution[1].get_deployment('hadoop2').get_instances()) == 0

    # HOST C
    assert greedy_solution[2].name == 'hostC'
    assert greedy_solution[2].free_ram == 6.0 * 1024.0
    assert greedy_solution[2].get_deployment('hadoop1').datanodes == 1
    assert greedy_solution[2].get_deployment('hadoop1').tasktrackers == 1
    assert len(greedy_solution[2].get_deployment('hadoop1').get_instances()) == 0
    assert greedy_solution[2].get_deployment('hadoop2').datanodes == 3
    assert greedy_solution[2].get_deployment('hadoop2').tasktrackers == 3
    assert len(greedy_solution[2].get_deployment('hadoop2').get_instances()) == 1
    assert greedy_solution[2].get_deployment('hadoop2').get_instances()[0].name == 'instB'

    # HOST D
    assert greedy_solution[3].name == 'hostD'
    assert greedy_solution[3].free_ram == -10.0 * 1024.0
    assert greedy_solution[3].get_deployment('hadoop1').datanodes == 0
    assert greedy_solution[3].get_deployment('hadoop1').tasktrackers == 0
    assert len(greedy_solution[3].get_deployment('hadoop1').get_instances()) == 0
    assert greedy_solution[3].get_deployment('hadoop2').datanodes == 0
    assert greedy_solution[3].get_deployment('hadoop2').tasktrackers == 0
    assert len(greedy_solution[3].get_deployment('hadoop2').get_instances()) == 0


# UNIT TEST INPUT3 - GREEDY AND OPTIMAL

if args.test_no and args.test_no == 3:

    # HOST A
    assert greedy_solution[0].name == 'hostA'
    assert greedy_solution[0].free_ram == 0.0 * 1024.0
    assert greedy_solution[0].cost == 4.0 * 1024.0
    assert greedy_solution[0].get_deployment('hadoop1').datanodes == 0
    assert greedy_solution[0].get_deployment('hadoop1').tasktrackers == 3
    assert len(greedy_solution[0].get_deployment('hadoop1').get_instances()) == 2
    assert greedy_solution[0].get_deployment('hadoop1').get_instances()[0].name == 'instA'
    assert greedy_solution[0].get_deployment('hadoop1').get_instances()[1].name == 'instB'

    # HOST B
    assert greedy_solution[1].name == 'hostB'
    assert greedy_solution[1].free_ram == 0.0 * 1024.0
    assert greedy_solution[1].cost == 2.0 * 1024.0
    assert greedy_solution[1].get_deployment('hadoop1').datanodes == 0
    assert greedy_solution[1].get_deployment('hadoop1').tasktrackers == 3
    assert len(greedy_solution[1].get_deployment('hadoop1').get_instances()) == 1
    assert greedy_solution[1].get_deployment('hadoop1').get_instances()[0].name == 'instC'

    assert greedy_cost == 6.0 * 1024.0

    # HOST A
    assert optimal_solution[0].name == 'hostA'
    assert optimal_solution[0].free_ram == 4.0 * 1024.0
    assert optimal_solution[0].cost == 4.0 * 1024.0
    assert optimal_solution[0].get_deployment('hadoop1').datanodes == 0
    assert optimal_solution[0].get_deployment('hadoop1').datanodes == 0
    assert optimal_solution[0].get_deployment('hadoop1').tasktrackers == 1
    assert len(optimal_solution[0].get_deployment('hadoop1').get_instances()) == 0

    # HOST B
    assert optimal_solution[1].name == 'hostB'
    assert optimal_solution[1].free_ram == -4.0 * 1024.0
    assert optimal_solution[1].cost == 8.0 * 1024.0
    assert optimal_solution[1].get_deployment('hadoop1').datanodes == 0
    assert optimal_solution[1].get_deployment('hadoop1').tasktrackers == 5
    assert len(optimal_solution[1].get_deployment('hadoop1').get_instances()) == 3
    assert optimal_solution[1].get_deployment('hadoop1').get_instances()[0].name == 'instA'
    assert optimal_solution[1].get_deployment('hadoop1').get_instances()[1].name == 'instB'
    assert optimal_solution[1].get_deployment('hadoop1').get_instances()[2].name == 'instC'

    assert optimal_cost == 12.0 * 1024.0

# UNIT TEST INPUT4 - GREEDY AND OPTIMAL

if args.test_no and args.test_no == 4:

    # HOST A
    assert greedy_solution[0].name == 'hostA'
    assert greedy_solution[0].free_ram == 1.0 * 1024.0
    assert greedy_solution[0].cost == 1.0 * 1024.0
    assert greedy_solution[0].get_deployment('hadoop1').datanodes == 1
    assert greedy_solution[0].get_deployment('hadoop1').tasktrackers == 0
    assert len(greedy_solution[0].get_deployment('hadoop1').get_instances()) == 0
    assert greedy_solution[0].get_deployment('hadoop2').datanodes == 0
    assert greedy_solution[0].get_deployment('hadoop2').tasktrackers == 1
    assert len(greedy_solution[0].get_deployment('hadoop2').get_instances()) == 1
    assert greedy_solution[0].get_deployment('hadoop2').get_instances()[0].name == 'instB'

    # HOST B
    assert greedy_solution[1].name == 'hostB'
    assert greedy_solution[1].free_ram == 0.0 * 1024.0
    assert greedy_solution[1].cost == 0.0 * 1024.0
    assert greedy_solution[1].get_deployment('hadoop1').datanodes == 0
    assert greedy_solution[1].get_deployment('hadoop1').tasktrackers == 1
    assert len(greedy_solution[1].get_deployment('hadoop1').get_instances()) == 1
    assert greedy_solution[1].get_deployment('hadoop1').get_instances()[0].name == 'instA'
    assert greedy_solution[1].get_deployment('hadoop2').datanodes == 0
    assert greedy_solution[1].get_deployment('hadoop2').tasktrackers == 2
    assert len(greedy_solution[1].get_deployment('hadoop2').get_instances()) == 0

    assert greedy_cost == 1.0 * 1024.0

    # HOST A
    assert optimal_solution[0].name == 'hostA'
    assert optimal_solution[0].free_ram == -3.0 * 1024.0
    assert optimal_solution[0].cost == -1.0 * 1024.0
    assert optimal_solution[0].get_deployment('hadoop1').datanodes == 1
    assert optimal_solution[0].get_deployment('hadoop1').tasktrackers == 1
    assert len(optimal_solution[0].get_deployment('hadoop1').get_instances()) == 1
    assert optimal_solution[0].get_deployment('hadoop1').get_instances()[0].name == 'instA'
    assert optimal_solution[0].get_deployment('hadoop2').datanodes == 0
    assert optimal_solution[0].get_deployment('hadoop2').tasktrackers == 0
    assert len(optimal_solution[0].get_deployment('hadoop2').get_instances()) == 0

    # HOST B
    assert optimal_solution[1].name == 'hostB'
    assert optimal_solution[1].free_ram == 4.0 * 1024.0
    assert optimal_solution[1].cost == 6.0 * 1024.0
    assert optimal_solution[1].get_deployment('hadoop1').datanodes == 0
    assert optimal_solution[1].get_deployment('hadoop1').tasktrackers == 0
    assert len(optimal_solution[1].get_deployment('hadoop1').get_instances()) == 0
    assert optimal_solution[1].get_deployment('hadoop2').datanodes == 0
    assert optimal_solution[1].get_deployment('hadoop2').tasktrackers == 3
    assert len(optimal_solution[1].get_deployment('hadoop2').get_instances()) == 1
    assert optimal_solution[1].get_deployment('hadoop2').get_instances()[0].name == 'instB'

    assert optimal_cost == 5.0 * 1024.0

    assert abs(original_unbalance_index - 0.428571428571) < 0.000000000001
    assert abs(greedy_unbalance_index - 1.0) < 0.000000000001
    assert abs(greedy_unbalance_diff - 1.333333333333) < 0.000000000001
    assert abs(optimal_unbalance_index - 7.0) < 0.000000000001
    assert abs(optimal_unbalance_diff - 15.333333333333) < 0.000000000001

if args.test_no:
    print 'Test #' + str(args.test_no) + ' passed'
