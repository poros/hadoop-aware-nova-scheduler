import sys
import argparse
import copy
import random
from simulator_test import simulator_unit_test_cases


#############################################################
#                          CLASSES                          #
#############################################################


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

    def get_deployment_num_instances(self, deploy_name):
        return len(self.get_deployment(deploy_name).get_instances())

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


#############################################################
#                     INPUT PARSING                         #
#############################################################


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


#############################################################
#                      COST FUNCTIONS                       #
#############################################################


def system_unbalance_index(hosts):
    free_rams = [h.free_ram for h in hosts]
    max_ram = max(free_rams)
    min_ram = min(free_rams)
    unbalance_index = abs(float(max_ram - min_ram))
    return unbalance_index


# def system_unbalance_index(hosts):
#     ram = 0
#     for h in hosts:
#         ram += h.free_ram
#     avg_ram = float(ram) / float(len(hosts))
#     unbalance = 0
#     for h in hosts:
#         unbalance += abs(avg_ram - float(h.free_ram))
#     unbalance_index = unbalance / float(ram)
#     return unbalance_index


def random_scheduler(instances, status):
    hosts = copy.deepcopy(status)
    for inst in instances:
        x = random.randint(0, len(hosts) - 1)
        # if hosts[x].free_ram >= inst.ram:
        hosts[x].schedule_instance(inst)

    cost = 0
    for h in hosts:
        h.cost = h.compute_host_cost(param)
        cost += h.cost

    return cost, hosts


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

    cost = 0
    for h in hosts:
        h.cost = h.compute_host_cost(param)
        cost += h.cost
    return cost, hosts


counter = 0


def recursive_schedule(instances, hosts, param, max_cost, max_hosts, level, last_host):
        global counter
        if (level == len(instances)):
            # counter += 1
            # print '#######################################'
            # print counter
            # for i in hosts:
            #     print i
            # print '#######################################'
            cost = 0
            for host in hosts:
                cost += host.compute_host_cost(param)
            #if (not max_cost or max_cost < cost or (max_cost == cost and system_unbalance_index(hosts) < system_unbalance_index(max_hosts))):
            if (not max_cost or max_cost < cost):
                max_cost = cost
                max_hosts = copy.deepcopy(hosts)
            return max_cost, max_hosts

        if (level == 40):
            counter += 1
            print counter

        start = max(last_host, 0)
        end = min(len(hosts), last_host + 2)
        deploy = instances[level].deploy_name
        for h in range(start, end):
            # if (h > 0):
                # x = [hosts[t].get_deployment_num_instances(deploy) >= (hosts[h].get_deployment_num_instances(deploy) + 1)
                # for t in range(0, h-1)]
            if (h == 0
                or hosts[h-1].get_deployment_num_instances(deploy)
                >= (hosts[h].get_deployment_num_instances(deploy) + 1)
                ):
                host = hosts[h]
                host.schedule_instance(instances[level])
                max_cost, max_hosts = recursive_schedule(instances, hosts, param, max_cost, max_hosts, level+1, h)
                host.undo_schedule_instance(instances[level])
        return max_cost, max_hosts


def optimal_scheduler(instances, status, param):
    hosts = copy.deepcopy(status)
    max_cost, max_hosts = None, None
    max_cost, max_hosts = recursive_schedule(instances, hosts, param, max_cost, max_hosts, 0, -1)
    # print 'GLOBAL COUNTER!!!!!!!!!!!!!!!!!!!!! %d' % (counter)
    return max_cost, max_hosts


#############################################################
#                        PARAMETERS                         #
#############################################################


param = {
    "ram_weight": 3.0,
    "datanodes_weight": 2.0,
    "tasktrackers_weight": 1.0,
    "corrective_weight": 1024.0
}


#############################################################
#                       MAIN PROGRAM                        #
#############################################################


parser = argparse.ArgumentParser(description="Nova Application-Aware Scheduler Simulator")
parser.add_argument("input_fd", metavar='<input_file>', type=argparse.FileType('r'), default=sys.stdin, help='input file')
parser.add_argument("--out", dest='output_fd', metavar='<output_file>', type=argparse.FileType('w'), default=sys.stdout, help='output file')
parser.add_argument("--conf", dest='conf_fd', metavar='<conf_file>', type=argparse.FileType('r'), help='configuration file')
parser.add_argument("--test", dest='test_no', metavar='<unit_test#>', type=int, help='unit test #')
parser.add_argument("--verbose", "-v", dest='verbose', action='store_true', help='verbose (and more human readable) solution visualization will be printed on screen')
parser.add_argument("--short", "-s", dest='short', action='store_true', help='a shorter version of the output with only the costs will be used instead of the normal one')

args = parser.parse_args()

if args.conf_fd:
    read_params(args.conf_fd, param)

hosts, instances = read_status(args.input_fd)

random_cost, random_solution = random_scheduler(instances, hosts)

greedy_cost, greedy_solution = greedy_scheduler(instances, hosts, param)

optimal_cost, optimal_solution = optimal_scheduler(instances, hosts, param)

# original_unbalance_index = system_unbalance_index(hosts)
# random_unbalance_index = system_unbalance_index(random_solution)
# greedy_unbalance_index = system_unbalance_index(greedy_solution)
# optimal_unbalance_index = system_unbalance_index(optimal_solution)
# if original_unbalance_index != 0:
#     random_unbalance_diff = (random_unbalance_index - original_unbalance_index) / original_unbalance_index
#     greedy_unbalance_diff = (greedy_unbalance_index - original_unbalance_index) / original_unbalance_index
#     optimal_unbalance_diff = (optimal_unbalance_index - original_unbalance_index) / original_unbalance_index
# else:
#     random_unbalance_diff = random_unbalance_index - original_unbalance_index
#     greedy_unbalance_diff = greedy_unbalance_index - original_unbalance_index
#     optimal_unbalance_diff = optimal_unbalance_index - original_unbalance_index

if not args.short:
    args.output_fd.write('RANDOM\n')
    for i in random_solution:
        i.cost = i.compute_host_cost(param)
        args.output_fd.write(i.to_string())
    args.output_fd.write('total_cost %f\n' % random_cost)
    # args.output_fd.write('relative_unbalance_variation %f\n' % random_unbalance_diff)

    args.output_fd.write('GREEDY\n')
    for i in random_solution:
        i.cost = i.compute_host_cost(param)
        args.output_fd.write(i.to_string())
    args.output_fd.write('total_cost %f\n' % greedy_cost)
    # args.output_fd.write('relative_unbalance_variation %f\n' % greedy_unbalance_diff)

    args.output_fd.write('OPTIMAL\n')
    for i in optimal_solution:
        i.cost = i.compute_host_cost(param)
        args.output_fd.write(i.to_string())
    args.output_fd.write('total_cost %f\n' % optimal_cost)
    # args.output_fd.write('relative_unbalance_variation %f\n' % optimal_unbalance_diff)
else:
    args.output_fd.write('RANDOM %f\n' % random_cost)
    args.output_fd.write('GREEDY %f\n' % greedy_cost)
    args.output_fd.write('OPTIMAL %f\n' % optimal_cost)

if args.verbose:
    print '################'
    print 'RANDOM SCHEDULER\n'
    for i in random_solution:
        print i
    print 'TOTAL COST ', random_cost
    # print 'RELATIVE UNBALANCE VARIATION', random_unbalance_diff
    print '################'
    print 'GREEDY SCHEDULER\n'
    for i in greedy_solution:
        print i
    print 'TOTAL COST ', greedy_cost
    # print 'RELATIVE UNBALANCE VARIATION', greedy_unbalance_diff
    print '################'
    print 'OPTIMAL SCHEDULER\n'
    for i in optimal_solution:
        print i
    print 'TOTAL COST ', optimal_cost
    # print 'RELATIVE UNBALANCE VARIATION', optimal_unbalance_diff
    print '################'

if args.test_no:
    simulator_unit_test_cases(args.test_no, greedy_solution, greedy_cost, optimal_solution, optimal_cost)
