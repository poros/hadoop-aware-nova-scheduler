#############################################################
#                        UNIT TESTS                         #
#############################################################


def simulator_unit_test_cases(test_no, greedy_solution, greedy_cost, optimal_solution, optimal_cost):

    # UNIT TEST 1 - GREEDY

    if test_no == 1:

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

    if test_no == 2:

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

    if test_no == 3:

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

    if test_no == 4:

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

        # assert abs(original_unbalance_index - 0.428571428571) < 0.000000000001
        # assert abs(greedy_unbalance_index - 1.0) < 0.000000000001
        # assert abs(greedy_unbalance_diff - 1.333333333333) < 0.000000000001
        # assert abs(optimal_unbalance_index - 7.0) < 0.000000000001
        # assert abs(optimal_unbalance_diff - 15.333333333333) < 0.000000000001

    print 'Test #' + str(test_no) + ' passed'
